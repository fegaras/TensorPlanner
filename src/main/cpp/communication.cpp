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

#include <sstream>
#include <cstring>
#include <vector>
#include <tuple>
#include <thread>
#include <chrono>
#include <unistd.h>
#include "mpi.h"
#include "omp.h"
#include "tensor.h"
#include "opr.h"

void info ( const char *fmt, ... );

void put_int ( ostringstream &out, const int i ) {
  out.write((const char*)&i,sizeof(int));
}

int serialize ( ostringstream &out, const void* data, vector<int>* encoded_type, int loc ) {
  switch ((*encoded_type)[loc]) {
  case 10: { // tuple
    if ((*encoded_type)[loc+1] == 2
        && (*encoded_type)[loc+2] == 0
        && (*encoded_type)[loc+3] == 10) {
      tuple<int,void*>* x = data;
      put_int(out,get<0>(*x));
      return serialize(out,get<1>(*x),encoded_type,loc+3);
    }
    if ((*encoded_type)[loc+1] > 0 && (*encoded_type)[loc+2] == 0) {
      switch ((*encoded_type)[loc+1]) {
      case 1: {
        tuple<int>* x = data;
        put_int(out,get<0>(*x));
        break;
      }
      case 2: {
        tuple<int,int>* x = data;
        put_int(out,get<0>(*x));
        put_int(out,get<1>(*x));
        break;
      }
      case 3: {
        tuple<int,int,int>* x = data;
        put_int(out,get<0>(*x));
        put_int(out,get<1>(*x));
        put_int(out,get<2>(*x));
        break;
      }
      case 4: {
        tuple<int,int,int,int>* x = data;
        put_int(out,get<0>(*x));
        put_int(out,get<1>(*x));
        put_int(out,get<2>(*x));
        put_int(out,get<3>(*x));
        break;
      }
      }
      return loc+(*encoded_type)[loc+1]+2;
    } else {
      switch ((*encoded_type)[loc+1]) {
      case 0:
        return loc+2;
      case 2: {
        tuple<void*,void*>* x = data;
        int l2 = serialize(out,get<0>(*x),encoded_type,loc+2);
        return serialize(out,get<1>(*x),encoded_type,l2);
      }
      case 3: {
        tuple<void*,void*,void*>* x = data;
        int l2 = serialize(out,get<0>(*x),encoded_type,loc+2);
        int l3 = serialize(out,get<1>(*x),encoded_type,l2);
        return serialize(out,get<2>(*x),encoded_type,l3);
      }
      }
    }
  }
  case 11:  // Vec
    switch ((*encoded_type)[loc+1]) {
    case 0: {
      Vec<int>* x = data;
      put_int(out,x->size());
      out.write((const char*)x->buffer(),sizeof(int)*x->size());
      return loc+2;
    }
    case 1: {
      Vec<long>* x = data;
      put_int(out,x->size());
      out.write((const char*)x->buffer(),sizeof(long)*x->size());
      return loc+2;
    }
    case 3: {
      Vec<double>* x = data;
      put_int(out,x->size());
      out.write((const char*)x->buffer(),sizeof(double)*x->size());
      return loc+2;
    }
    }
  default:
    return loc+1;
  }
}

int serialize ( void* data, char* buffer, vector<int>* encoded_type ) {
  ostringstream out;
  serialize(out,data,encoded_type,0);
  memcpy(buffer,out.str().c_str(),out.tellp());
  return out.tellp();
}

int get_int ( istringstream &in ) {
  int n;
  in.read((const char*)&n,sizeof(int));
  return n;
}

int deserialize ( istringstream &in, void* &data, vector<int>* encoded_type, int loc ) {
  switch ((*encoded_type)[loc]) {
  case 10: { // tuple
    if ((*encoded_type)[loc+1] == 2
        && (*encoded_type)[loc+2] == 0
        && (*encoded_type)[loc+3] == 10) {
      int n = get_int(in);
      void* x;
      int l2 = deserialize(in,x,encoded_type,loc+3);
      data = new tuple<int,void*>(n,x);
      return l2;
    }
    if ((*encoded_type)[loc+1] > 0 && (*encoded_type)[loc+2] == 0) {
      switch ((*encoded_type)[loc+1]) {
      case 1: {
        data = new tuple<int>(get_int(in));
        break;
      }
      case 2: {
        data = new tuple<int,int>(get_int(in),get_int(in));
        break;
      }
      case 3: {
        data = new tuple<int,int,int>(get_int(in),get_int(in),get_int(in));
        break;
      }
      case 4: {
        data = new tuple<int,int,int,int>(get_int(in),get_int(in),get_int(in),get_int(in));
        break;
      }
      }
      return loc+(*encoded_type)[loc+1]+2;
    } else {
      switch ((*encoded_type)[loc+1]) {
      case 0:
        return loc+2;
      case 2: {
        void *x1, *x2;
        int l2 = deserialize(in,x1,encoded_type,loc+2);
        int l3 = deserialize(in,x2,encoded_type,l2);
        data = new tuple<void*,void*>(x1,x2);
        return l3;
      }
      case 3: {
        void *x1, *x2, *x3;
        int l2 = deserialize(in,x1,encoded_type,loc+2);
        int l3 = deserialize(in,x2,encoded_type,l2);
        int l4 = deserialize(in,x3,encoded_type,l3);
        data = new tuple<void*,void*,void*>(x1,x2,x3);
        return l4;
      }
      }
    }
  }
  case 11:  // Vec
    switch ((*encoded_type)[loc+1]) {
    case 0: {
      int len = get_int(in);
      Vec<int>* x = new Vec<int>(len);
      in.read((const char*)x->buffer(),sizeof(int)*len);
      data = x;
      return loc+2;
    }
    case 1: {
      int len = get_int(in);
      Vec<long>* x = new Vec<long>(len);
      in.read((const char*)x->buffer(),sizeof(long)*len);
      data = x;
      return loc+2;
    }
    case 3: {
      int len = get_int(in);
      Vec<double>* x = new Vec<double>(len);
      in.read((const char*)x->buffer(),sizeof(double)*len);
      data = x;
      return loc+2;
    }
    }
  default:
    return loc+1;
  }
}

void deserialize ( void* &data, const char* buffer, size_t len, vector<int>* encoded_type ) {
  string s(buffer,len);
  istringstream in(s);
  in.seekg(0,ios::beg);
  deserialize(in,data,encoded_type,0);
}

int num_of_executors = 1;
int executor_rank = 0;
int coordinator = 0;
const int max_buffer_size = 500000000;
auto comm = MPI_COMM_WORLD;
MPI_Request receive_request;
vector<int> active_executors;
extern bool delete_arrays;
extern vector<Opr*> operations;
extern vector<void*(*)(void*)> functions;
bool stop_receiver = false;

//void delete_reduce_input ( int opr_id );
//void delete_array ( int opr_id );
int delete_array ( void* &data, vector<int>* encoded_type, int loc );

void handle_received_message ( int tag, int opr_id, void* data, int len ) {
  Opr* opr = operations[opr_id];
  if (tag == 0) {
    info("    received %d bytes from %d (opr %d)",len+4,opr->node,opr_id);
    cache_data(opr,data);
    opr->status = completed;
    enqueue_ready_operations(opr_id);
  } else if (tag == 2) {   // cache data
    info("    received %d bytes from %d (opr %d)",len+4,opr->node,opr_id);
    opr->cached = data;
    opr->status = completed;
  } else if (tag == 1 && opr->type == reduceOPR) {
    info("    received partial reduce result of %d bytes (opr %d)",
         len+4,opr_id);
    vector<int>* rs = opr->opr.reduce_opr->s;
    void*(*op)(tuple<void*,void*>*) = functions[opr->opr.reduce_opr->op];
    if (opr->cached == nullptr) {
      // first reduction input
      info("    set reduce opr %d to the first incoming input",opr_id);
      opr->cached = data;
    } else if (opr->opr.reduce_opr->valuep)
      // total aggregation
      opr->cached = op(new tuple<void*,void*>(opr->cached,data));
    else if (operations[(*rs)[0]]->int_index) {
      tuple<int,void*>* x = opr->cached;
      tuple<int,void*>* y = data;
      // merge the current state with the incoming partially reduced data
      info("    merge reduce opr %d with incoming input",opr_id);
      opr->cached = new tuple<int,void*>(get<0>(*x),op(new tuple<void*,void*>(get<1>(*x),get<1>(*y))));
      info("    delete incoming partial reduce block for opr %d",opr_id);
      delete_array(data,opr->encoded_type,0);
    } else {
      tuple<void*,void*>* x = opr->cached;
      tuple<void*,void*>* y = data;
      // merge the current state with the incoming partially reduced data
      info("    merge reduce opr %d with incoming input",opr_id);
      opr->cached = new tuple<void*,void*>(get<0>(*x),op(new tuple<void*,void*>(get<1>(*x),get<1>(*y))));
      info("    delete incoming partial reduce block for opr %d",opr_id);
      delete_array(data,opr->encoded_type,0);
    }
    opr->reduced_count--;
    if (opr->reduced_count == 0) {
      // completed final reduce
      info("    completed reduce opr %d",opr_id);
      opr->status = completed;
      enqueue_ready_operations(opr_id);
    }
  }
}

// check if there is a request to send data (array blocks); if there is, get the data and cache it
int check_communication () {
  static const char* buffer = new char[max_buffer_size];
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
    char* b = new char[len];
    memcpy(b,buffer+3*sizeof(int),len);
    deserialize(data,b,len,opr->encoded_type);
    delete b;
    MPI_Request_free(&receive_request);
    // prepare for the next receive (non-blocking)
    MPI_Irecv(buffer,max_buffer_size,MPI_BYTE,MPI_ANY_SOURCE,
              MPI_ANY_TAG,comm,&receive_request);
//    thread( [tag,opr_id,data,len] () {
    handle_received_message(tag,opr_id,data,len+2*sizeof(int));
//            }).detach();
} catch ( const exception &ex ) {
  info("*** fatal error: receiving the data for operation %d",opr_id);
  abort();
}
    return 1;
  }
  return 0;
}

void kill_receiver () {
  MPI_Cancel(&receive_request);
  MPI_Request_free(&receive_request);
  receive_request = nullptr;
}

void run_receiver () {
  if (receive_request != nullptr)
    kill_receiver();
  while (!stop_receiver) {
    check_communication();
    this_thread::sleep_for(chrono::milliseconds(1));
  }
}

void abort () {
  MPI_Abort(comm,0);
}

void send_data ( int rank, void* data, int opr_id, int tag ) {
  char* buffer = new char[max_buffer_size];
  if (data == nullptr) {
    info("null data sent for %d to %d",opr_id,rank);
    abort();
  }
  // serialize data into a byte array
  Opr* opr = operations[opr_id];
  *(int*)buffer = opr_id;
  *(int*)(buffer+sizeof(int)) = tag;
  int len = serialize(data,buffer+3*sizeof(int),opr->encoded_type);
  *(int*)(buffer+2*sizeof(int)) = len;
  info("    sending %d bytes to %d (opr %d)",
       len+3*sizeof(int),rank,opr_id);
  MPI_Send(buffer,len+3*sizeof(int),MPI_BYTE,rank,tag,comm);
  delete buffer;
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
  MPI_Init_thread(&argc,&argv,MPI_THREAD_FUNNELED,&ignore);
  MPI_Comm_set_errhandler(comm,MPI_ERRORS_RETURN);
  MPI_Comm_rank(comm,&executor_rank);
  MPI_Comm_size(comm,&num_of_executors);
  coordinator = 0;
  for ( int i = 0; i < num_of_executors; i++ )
    active_executors.push_back(i);
  char machine_name[256];
  gethostname(machine_name,255);
  int local_rank = atoi(getenv("OMPI_COMM_WORLD_LOCAL_RANK"));
  int ts;
  #pragma omp parallel
  { ts = omp_get_num_threads(); }
  printf("Using executor %d: %s/%d (threads: %d)\n",
         executor_rank,machine_name,local_rank,ts);
}

void mpi_finalize () {
  MPI_Finalize();
}
