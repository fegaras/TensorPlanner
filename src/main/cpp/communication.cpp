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

#include "tensor.h"
#include <sstream>
#include <cstring>
#include <chrono>
#include <unistd.h>
#include "opr.h"

int num_of_executors = 1;
int executor_rank = 0;
int coordinator = 0;
int max_buffer_size = 50000000;
const auto comm = MPI_COMM_WORLD;
MPI_Request receive_request = MPI_REQUEST_NULL;
extern bool delete_arrays;
extern vector<Opr*> operations;
extern vector<void*(*)(void*)> functions;
bool stop_receiver = false;
extern bool inMemory;

void info ( const char *fmt, ... );

int serialize ( void* data, char* buffer, vector<int>* encoded_type );
void deserialize ( void* &data, const char* buffer, size_t len, vector<int>* encoded_type );

void delete_block ( void* &data, vector<int>* encoded_type );
void delete_first_reduce_input ( int rid );


void handle_received_message ( int tag, int opr_id, void* data, int len ) {
  Opr* opr = operations[opr_id];
  static tuple<void*,void*>* op_arg = new tuple<void*,void*>(nullptr,nullptr);
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
    info("    received partial reduce result of %d bytes (opr %d)",
         len,opr_id);
    auto op = (void*(*)(tuple<void*,void*>*))functions[opr->opr.reduce_opr->op];
    if (opr->cached == nullptr) {
      // first reduction input
      info("    set reduce opr %d to the first incoming input",opr_id);
      opr->cached = data;
    } else if (opr->opr.reduce_opr->valuep) {
      // total aggregation
      get<0>(*op_arg) = opr->cached;
      get<1>(*op_arg) = data;
      opr->cached = op(op_arg);
    } else {
      auto x = (tuple<void*,void*>*)opr->cached;
      auto y = (tuple<void*,void*>*)data;
      // merge the current state with the incoming partially reduced data
      info("    merge reduce opr %d with incoming input",opr_id);
      auto old = (void*)x;
      get<0>(*op_arg) = get<1>(*x);
      get<1>(*op_arg) = get<1>(*y);
      opr->cached = new tuple<void*,void*>(get<0>(*x),op(op_arg));
      if (delete_arrays && opr->first_reduced_input < 0) {
        info("    delete current reduce result in opr %d",opr_id);
        delete_block(old,opr->encoded_type);
      }
      if (delete_arrays) {
        info("    delete incoming partial reduce block for opr %d",opr_id);
        delete_block(data,opr->encoded_type);
      }
    }
    delete_first_reduce_input(opr_id);
    opr->reduced_count--;
    if (opr->reduced_count <= 0) {
      // completed final reduce
      info("    completed reduce opr %d",opr_id);
      opr->status = completed;
      enqueue_ready_operations(opr_id);
    }
  }
}

// check if there is a request to send data (array blocks); if there is, get the data and cache it
int check_communication () {
  static char* buffer = new char[max_buffer_size];
  if (receive_request == MPI_REQUEST_NULL) {
    // prepare for the first receive (non-blocking)
    MPI_Irecv(buffer,max_buffer_size,MPI_BYTE,MPI_ANY_SOURCE,MPI_ANY_TAG,
              comm,&receive_request);
  }
  int mtag;
  MPI_Test(&receive_request,&mtag,MPI_STATUS_IGNORE);
  if (mtag == 1) {
    // deserialize and process the incoming data
    int opr_id = *(const int*)buffer;
    int tag = *(const int*)(buffer+sizeof(int));
    int len = *(const int*)(buffer+2*sizeof(int));
    Opr* opr = operations[opr_id];
    void* data;
    deserialize(data,buffer+3*sizeof(int),len,opr->encoded_type);
    // prepare for the next receive (non-blocking)
    MPI_Irecv(buffer,max_buffer_size,MPI_BYTE,MPI_ANY_SOURCE,
              MPI_ANY_TAG,comm,&receive_request);
    handle_received_message(tag,opr_id,data,len+3*sizeof(int));
    return 1;
  }
  return 0;
}

void kill_receiver () {
  if (receive_request != MPI_REQUEST_NULL) {
    MPI_Cancel(&receive_request);
    MPI_Request_free(&receive_request);
    receive_request = MPI_REQUEST_NULL;
  }
}

void run_receiver () {
  if (receive_request != MPI_REQUEST_NULL)
    kill_receiver();
  while (!stop_receiver) {
    if (check_communication() == 0)
      this_thread::sleep_for(chrono::milliseconds(1));
  }
}

int mpi_abort () {
  return MPI_Abort(comm,0);
}

mutex send_data_mutex;

void send_data ( int rank, void* data, int opr_id, int tag ) {
  // needs to be static and locked
  static char* buffer = new char[max_buffer_size];
  if (data == nullptr) {
    info("null data sent for opr %d to %d",opr_id,rank);
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
  if (inMemory)
    return b;
  int in[1] = { ( b ? 0 : 1 ) };
  int ret[1];
  MPI_Allreduce(in,ret,1,MPI_INT,MPI_LOR,comm);
  return ret[0] == 0;
}

void mpi_barrier () {
  MPI_Barrier(comm);
}

int getCoordinator () { return coordinator; }

bool isCoordinator () {
  return executor_rank == coordinator;
}

void mpi_startup ( int argc, char* argv[], int block_dim_size ) {
  // can fit 5 blocks of double
  max_buffer_size = max(100000LU,5*sizeof(double)*block_dim_size*block_dim_size);
  if (inMemory)
    return;
  // Multiple threads may call MPI, with no restrictions
  int provided;
  MPI_Init_thread(&argc,&argv,MPI_THREAD_MULTIPLE,&provided);
  if (provided != MPI_THREAD_MULTIPLE) {
    printf("Required MPI_THREAD_MULTIPLE to run MPI+OpenMP");
    MPI_Finalize();
    exit(-1);
  }
  MPI_Comm_set_errhandler(comm,MPI_ERRORS_RETURN);
  MPI_Comm_rank(comm,&executor_rank);
  MPI_Comm_size(comm,&num_of_executors);
  coordinator = 0;
  char machine_name[256];
  gethostname(machine_name,255);
  int local_rank = 0;
  char* lc = getenv("OMPI_COMM_WORLD_LOCAL_RANK");
  if (lc == nullptr)
    lc = getenv("MV2_COMM_WORLD_LOCAL_RANK");
  if (lc != nullptr)
    local_rank = atoi(lc);
  int ts;
  #pragma omp parallel
  { ts = omp_get_num_threads(); }
  printf("Using executor %d: %s/%d (threads: %d)\n",
         executor_rank,machine_name,local_rank,ts);
}

void mpi_finalize () {
  if (!inMemory)
    MPI_Finalize();
}
