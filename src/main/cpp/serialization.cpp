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
#include <stdint.h>

void info ( const char *fmt, ... );

void put_int ( ostringstream &out, const uintptr_t i ) {
  out.write((const char*)&i,sizeof(uintptr_t));
}

int serialize ( ostringstream &out, const void* data, vector<int>* encoded_type, int loc ) {
  int device_id = omp_get_default_device();
  int host_id = omp_get_initial_device();
  switch ((*encoded_type)[loc]) {
  case 0: case 1: // index
    put_int(out,(uintptr_t)data);
    return loc+1;
  case 10: { // tuple
    switch ((*encoded_type)[loc+1]) {
      case 0:
        return loc+2;
      case 2: {
        auto x = (tuple<void*,void*>*)data;
        int l2 = serialize(out,get<0>(*x),encoded_type,loc+2);
        return serialize(out,get<1>(*x),encoded_type,l2);
      }
      case 3: {
        auto x = (tuple<void*,void*,void*>*)data;
        int l2 = serialize(out,get<0>(*x),encoded_type,loc+2);
        int l3 = serialize(out,get<1>(*x),encoded_type,l2);
        return serialize(out,get<2>(*x),encoded_type,l3);
      }
    default:
      info("Unknown tuple: %d",(*encoded_type)[loc+1]);
    }
  }
  case 11:  // Vec
    switch ((*encoded_type)[loc+1]) {
    case 0: {
      auto x = (Vec<int>*)data;
      int n = x->size();
      put_int(out,n);
      int* buffer = x->buffer();
      if(is_GPU()) {
        int* cpu_buffer = new int[n];
        // copy values from device to host
        omp_target_memcpy(cpu_buffer, buffer, sizeof(int)*n, 0, 0, host_id, device_id);

        out.write((const char*)cpu_buffer,sizeof(int)*n);
        delete[] cpu_buffer;
      }
      else {
        out.write((const char*)buffer,sizeof(int)*n);
      }
      return loc+2;
    }
    case 1: {
      auto x = (Vec<long>*)data;
      int n = x->size();
      put_int(out,n);
      long* buffer = x->buffer();
      if(is_GPU()) {
        long* cpu_buffer = new long[n];
        // copy values from device to host
        omp_target_memcpy(cpu_buffer, buffer, sizeof(long)*n, 0, 0, host_id, device_id);

        out.write((const char*)cpu_buffer,sizeof(long)*n);
        delete[] cpu_buffer;
      }
      else {
        out.write((const char*)buffer,sizeof(long)*n);
      }
      return loc+2;
    }
    case 3: {
      auto x = (Vec<double>*)data;
      int n = x->size();
      put_int(out,n);
      double* buffer = x->buffer();
      if(is_GPU()) {
        double* cpu_buffer = new double[n];
        // copy values from device to host
        omp_target_memcpy(cpu_buffer, buffer, sizeof(double)*n, 0, 0, host_id, device_id);

        out.write((const char*)cpu_buffer,sizeof(double)*n);
        delete[] cpu_buffer;
      }
      else {
        out.write((const char*)buffer,sizeof(double)*n);
      }
      return loc+2;
    }
    default:
      info("Unknown Vec: %d",(*encoded_type)[loc+1]);
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

uintptr_t get_int ( istringstream &in ) {
  uintptr_t n;
  in.read((char*)&n,sizeof(uintptr_t));
  return n;
}

int deserialize ( istringstream &in, void* &data, vector<int>* encoded_type, int loc ) {
  int device_id = omp_get_default_device();
  int host_id = omp_get_initial_device();
  switch ((*encoded_type)[loc]) {
  case 0: case 1: // index
    data = (void*)get_int(in);
    return loc+1;
  case 10: { // tuple
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
      default:
        info("Unknown tuple: %d",(*encoded_type)[loc+1]);
    }
  }
  case 11:  // Vec
    switch ((*encoded_type)[loc+1]) {
    case 0: {
      int len = get_int(in);
      Vec<int>* x = new Vec<int>(len);
      int* buffer = x->buffer();
      if(is_GPU()) {
        int* cpu_buffer = new int[len];
        in.read((char*)cpu_buffer,sizeof(int)*len);
        // copy values from host to device
        omp_target_memcpy(buffer, cpu_buffer, sizeof(int)*len, 0, 0, device_id, host_id);
        delete[] cpu_buffer;
      }
      else {
        in.read((char*)buffer,sizeof(int)*len);
      }
      data = x;
      return loc+2;
    }
    case 1: {
      int len = get_int(in);
      Vec<long>* x = new Vec<long>(len);
      long* buffer = x->buffer();
      if(is_GPU()) {
        long* cpu_buffer = new long[len];
        in.read((char*)cpu_buffer,sizeof(long)*len);
        // copy values from host to device
        omp_target_memcpy(buffer, cpu_buffer, sizeof(long)*len, 0, 0, device_id, host_id);
        delete[] cpu_buffer;
      }
      else {
        in.read((char*)buffer,sizeof(long)*len);
      }
      data = x;
      return loc+2;
    }
    case 3: {
      int len = get_int(in);
      Vec<double>* x = new Vec<double>(len);
      double* buffer = x->buffer();
      if(is_GPU()) {
        double* cpu_buffer = new double[len];
        in.read((char*)cpu_buffer,sizeof(double)*len);
        // copy values from host to device
        omp_target_memcpy(buffer, cpu_buffer, sizeof(double)*len, 0, 0, device_id, host_id);
        delete[] cpu_buffer;
      }
      else {
        in.read((char*)buffer,sizeof(double)*len);
      }
      data = x;
      return loc+2;
    }
    default:
      info("Unknown Vec: %d",(*encoded_type)[loc+1]);
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
