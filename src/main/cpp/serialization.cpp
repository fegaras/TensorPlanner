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


void put_int ( ostringstream &out, const int i ) {
  out.write((const char*)&i,sizeof(int));
}

int serialize ( ostringstream &out, const void* data, vector<int>* encoded_type, int loc ) {
  switch ((*encoded_type)[loc]) {
  case 0: case 1: // index
    put_int(out,(long)data);
    return loc+1;
  case 10: { // tuple
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
  case 0: case 1: // index
    data = (long)get_int(in);
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
