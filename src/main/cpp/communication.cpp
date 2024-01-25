/*
 * Copyright © 2023-2024 University of Texas at Arlington
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define GC_THREADS
#include <gc.h>
#include <sstream>
#include <cstring>
#include <vector>
#include <tuple>
#include <thread>
#include <mutex>
#include <chrono>
#include <unistd.h>
#include "mpi.h"
#include "omp.h"
#include "tensor.h"
#include "opr.h"

int num_of_executors = 1;
int executor_rank = 0;
int coordinator = 0;
const int max_buffer_size = 500000000;
auto comm = MPI_COMM_WORLD;
MPI_Request receive_request;
extern vector<Opr*> operations;
extern vector<void*(*)(void*)> functions;
bool stop_receiver = false;

void info ( const char *fmt, ... );

int serialize ( void* data, char* buffer, vector<int>* encoded_type );

void deserialize ( void* &data, const char* buffer, size_t len, vector<int>* encoded_type );


void handle_received_message ( int tag, int opr_id, void* data, int len ) {
  Opr* opr = operations[opr_id];
  if (tag == 0) {
    info("    received %d bytes from %d (opr %d)",len,opr->node,opr_id);
    cache_data(opr,data);
    opr->status = completed;
    enqueue_ready_operations(opr_id);
  } else if (tag == 2) {   // cache data
    info("    received %d bytes from %d (opr %d)",len,opr->node,opr_id);
    opr->cached = data;
    opr->status = completed;
  } else if (tag == 1 && opr->type == reduceOPR) {
    info("    received partial reduce result of %d bytes (opr %d)",len,opr_id);
    void*(*op)(tuple<void*,void*>*) = functions[opr->opr.reduce_opr->op];
    if (opr->cached == nullptr) {
      // first reduction input
      info("    set reduce opr %d to the first incoming input",opr_id);
      opr->cached = data;
    } else if (opr->opr.reduce_opr->valuep)
      // total aggregation
      opr->cached = op(new tuple<void*,void*>(opr->cached,data));
    else {
      tuple<void*,void*>* x = opr->cached;
      tuple<void*,void*>* y = data;
      // merge the current state with the incoming partially reduced data
      info("    merge reduce opr %d with incoming input",opr_id);
      opr->cached = new tuple<void*,void*>(get<0>(*x),op(new tuple<void*,void*>(get<1>(*x),get<1>(*y))));
    }
    opr->reduced_count--;
    if (opr->reduced_count <= 0) {
      // completed final reduce
      info("    completed reduce opr %d",opr_id);
      opr->status = computed;
      enqueue_ready_operations(opr_id);
    }
  }
}

// check if there is a request to send data (array blocks); if there is, get the data and cache it
int check_communication () {
  // needs to be static
  static const char* buffer = (char*)GC_MALLOC_ATOMIC_IGNORE_OFF_PAGE(max_buffer_size);
  MPI_Status status;
  if (receive_request == nullptr) {
    // prepare for the first receive (non-blocking)
    MPI_Irecv(buffer,max_buffer_size,MPI_BYTE,MPI_ANY_SOURCE,MPI_ANY_TAG,
              comm,&receive_request);
  }
  int mtag;
  MPI_Test(&receive_request,&mtag,&status);
  if (mtag != 0) {
    // deserialize and cache the incoming data
    int opr_id = *(const int*)buffer;
    try {
      int tag = *(const int*)(buffer+sizeof(int));
      int len = *(const int*)(buffer+2*sizeof(int));
      Opr* opr = operations[opr_id];
      void* data;
      char* b = (char*)GC_MALLOC_ATOMIC_IGNORE_OFF_PAGE(len);
      memcpy(b,buffer+3*sizeof(int),len);
      deserialize(data,b,len,opr->encoded_type);
      GC_FREE(b);
      MPI_Request_free(&receive_request);
      // prepare for the next receive (non-blocking)
      MPI_Irecv(buffer,max_buffer_size,MPI_BYTE,MPI_ANY_SOURCE,
                MPI_ANY_TAG,comm,&receive_request);
      handle_received_message(tag,opr_id,data,len+3*sizeof(int));
    } catch ( const exception &ex ) {
      info("*** fatal error: while receiving data for %d",opr_id);
      abort();
    }
    return 1;
  }
  return 0;
}

void kill_receiver () {
  stop_receiver = true;
  MPI_Cancel(&receive_request);
  MPI_Request_free(&receive_request);
  receive_request = nullptr;
}

int run_receiver () {
  if (receive_request != nullptr)
    kill_receiver();
  struct GC_stack_base sb;
  GC_get_stack_base(&sb);
  GC_register_my_thread(&sb);
  while (!stop_receiver) {
    check_communication();
    this_thread::sleep_for(chrono::milliseconds(1));
  }
  GC_unregister_my_thread();
  return 0;
}

void abort () {
  MPI_Abort(comm,0);
}

mutex send_data_mutex;

void send_data ( int rank, void* data, int opr_id, int tag ) {
  // needs to be static
  static char* buffer = (char*)GC_MALLOC_IGNORE_OFF_PAGE(max_buffer_size);
  if (data == nullptr) {
    info("null data sent for %d to %d",opr_id,rank);
    abort();
  }
  lock_guard<mutex> lock(send_data_mutex);
  // serialize data into a byte array
  Opr* opr = operations[opr_id];
  *(int*)buffer = opr_id;
  *(int*)(buffer+sizeof(int)) = tag;
  int len = serialize(data,buffer+3*sizeof(int),opr->encoded_type);
  *(int*)(buffer+2*sizeof(int)) = len;
  info("    sending %d bytes to %d (opr %d)",
       len+3*sizeof(int),rank,opr_id);
  MPI_Send(buffer,len+3*sizeof(int),MPI_BYTE,rank,tag,comm);
}

bool wait_all ( bool b ) {
  unsigned char in[1] = { b ? 0 : 1 };
  unsigned char ret[1];
  MPI_Allreduce(in,ret,1,MPI_BYTE,MPI_LOR,comm);
  return ret[0] == 0;
}

void barrier () {
  MPI_Barrier(comm);
}

int getCoordinator () { return coordinator; }

bool isCoordinator () {
  return executor_rank == coordinator;
}

void mpi_startup ( int argc, char* argv[] ) {
  int ignore;
  GC_INIT();
  GC_allow_register_threads();
  MPI_Init_thread(&argc,&argv,MPI_THREAD_FUNNELED,&ignore);
  MPI_Comm_set_errhandler(comm,MPI_ERRORS_RETURN);
  MPI_Comm_rank(comm,&executor_rank);
  MPI_Comm_size(comm,&num_of_executors);
  coordinator = 0;
  char machine_name[256];
  gethostname(machine_name,255);
  int local_rank = atoi(getenv("OMPI_COMM_WORLD_LOCAL_RANK"));
  int ts;
  #pragma omp parallel
  { struct GC_stack_base sb;
    GC_get_stack_base(&sb);
    GC_register_my_thread(&sb);
    ts = omp_get_num_threads();
  }
  printf("Using executor %d: %s/%d (threads: %d)\n",
         executor_rank,machine_name,local_rank,ts);
}

void mpi_finalize () {
  MPI_Finalize();
}
