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

// Create a dense boolean array and initialize it with the values of the boolean tensor init (if not null).
// It converts the sparse tensor init to a complete dense array where missing values are zero
Vec<bool>* array_buffer_bool ( int dsize, int ssize, tuple<Vec<int>*,Vec<bool>*>* init = nullptr ) {
  auto buffer = new Vec<bool>(dsize*ssize);
  bool* bv = buffer->buffer();
  if(is_GPU()) {
    #pragma omp target teams distribute parallel for is_device_ptr(bv)
    for (int i = 0; i < buffer->size(); i++ )
      bv[i] = false;
  }
  else {
    #pragma omp parallel for
    for (int i = 0; i < buffer->size(); i++ )
      bv[i] = false;
  }
  if (init != nullptr) {
    #pragma omp parallel for
    for ( int i = 0; i < get<0>(*init)->size()-1; i++ ) {
      int j = (*get<0>(*init))[i];
      while (j < (*get<0>(*init))[i+1]) {
        bv[i*ssize+(*get<1>(*init))[j]] = true;
        j++;
      }
    }
  }
  return buffer;
}

// Create a dense boolean array and initialize it with the values of the sparse boolean tensor init (if not null).
// It converts the sparse tensor init to a complete dense array where missing values are zero
Vec<bool>* array_buffer_sparse_bool ( int ssize, Vec<int>* init = nullptr ) {
  auto buffer = new Vec<bool>(ssize);
  bool* bv = buffer->buffer();
  #pragma omp parallel for
  for (int i = 0; i < buffer->size(); i++ )
    bv[i] = false;
  if (init != nullptr) {
    #pragma omp parallel for
    for ( int j = 0; j < init->size()-1; j++ )
      bv[(*init)[j]] = true;
  }
  return buffer;
}

// convert a dense boolean array to a boolean tensor (dense dimensions dn>0 & sparse dimensions sn>0)
tuple<Vec<int>*,Vec<int>*>* array2tensor_bool ( int dn, int sn, Vec<bool>* buffer ) {
  bool* bv = buffer->buffer();
  auto dense = new Vec<int>(dn+1);
  int* dv = dense->buffer();
  auto sparse = new vector<int>();
  dv[0] = 0;
  for ( int i = 0; i < dn; i++ ) {
    for ( int j = 0; j < sn; j++ ) {
      bool v = bv[i*sn+j];
      if (v)
        sparse->push_back(j);
    }
    dv[i+1] = sparse->size();
  }
  delete buffer;
  return new tuple<Vec<int>*,Vec<int>*>(dense,new Vec<int>(sparse));
}

// convert a dense boolean array to a sparse boolean tensor (dense dimensions dn=0 & sparse dimensions sn>0)
Vec<int>* array2tensor_bool ( int sn, Vec<bool>* buffer ) {
  bool* bv = buffer->buffer();
  auto sparse = new vector<int>();
  int j = 0;
  while ( j < sn ) {
    if ( bv[j] )
      sparse->push_back(j);
    j++;
  }
  delete buffer;
  return new Vec<int>(sparse);
}

// merge two boolean tensors using the monoid op/zero
tuple<Vec<int>*,Vec<int>*>*
     merge_tensors ( tuple<Vec<int>*,Vec<int>*>* x,
                     tuple<Vec<int>*,Vec<int>*>* y,
                     bool(*op)(tuple<bool,bool>*), bool zero ) {
  int i = 0;
  int len = min(get<0>(*x)->size(),get<0>(*y)->size())-1;
  auto dense = new Vec<int>(len+1);
  int* dv = dense->buffer();
  auto sparse = new vector<int>();
  dv[0] = 0;
  // don't create a tuple during loop
  auto t = new tuple<bool,bool>(zero,zero);
  while (i < len) {
    int xn = (*get<0>(*x))[i];
    int yn = (*get<0>(*y))[i];
    while (xn < (*get<0>(*x))[i+1] && yn < (*get<0>(*y))[i+1]) {
      if ((*get<1>(*x))[xn] == (*get<1>(*y))[yn]) {
        get<0>(*t) = true;
        get<1>(*t) = true;
        bool v = op(t);
        if (v)
          sparse->push_back((*get<1>(*x))[xn]);
        xn++; yn++;
      } else if ((*get<1>(*x))[xn] < (*get<1>(*y))[yn]) {
        get<0>(*t) = true;
        get<1>(*t) = false;
        bool v = op(t);
        if (v)
          sparse->push_back((*get<1>(*x))[xn]);
        xn++;
      } else {
        get<0>(*t) = false;
        get<1>(*t) = true;
        bool v = op(t);
        if (v)
          sparse->push_back((*get<1>(*y))[yn]);
        yn++;
      }
    }
    while (xn < (*get<0>(*x))[i+1]) {
      get<0>(*t) = true;
      get<1>(*t) = false;
      bool v = op(t);
      if (v)
        sparse->push_back((*get<1>(*x))[xn]);
      xn++;
    }
    while (yn < (*get<0>(*y))[i+1]) {
      get<0>(*t) = false;
      get<1>(*t) = true;
      bool v = op(t);
      if (v)
        sparse->push_back((*get<1>(*y))[yn]);
      yn++;
    }
    i++;
    dv[i] = sparse->size();
  }
  delete t;
  return new tuple<Vec<int>*,Vec<int>*>(dense,new Vec<int>(sparse));
}

// merge two boolean sparse tensors using the monoid op/zero
Vec<int>* merge_tensors ( Vec<int>* x, Vec<int>* y,
                          bool(*op)(tuple<bool,bool>*), bool zero ) {
  auto sparse = new vector<int>();
  // don't create a tuple during loop
  auto t = new tuple<bool,bool>(zero,zero);
  int xn = 0;
  int yn = 0;
  while (xn < x->size() && yn < y->size()) {
    if ((*x)[xn] == (*y)[yn]) {
      get<0>(*t) = true;
      get<1>(*t) = true;
      bool v = op(t);
      if (v)
        sparse->push_back((*x)[xn]);
      xn++; yn++;
    } else if ((*x)[xn] < (*y)[yn]) {
      get<0>(*t) = true;
      get<1>(*t) = false;
      bool v = op(t);
      if (v)
        sparse->push_back((*x)[xn]);
      xn++;
    } else {
      get<0>(*t) = false;
      get<1>(*t) = true;
      bool v = op(t);
      if (v)
        sparse->push_back((*y)[yn]);
      yn++;
    }
  }
  while (xn < x->size()) {
    get<0>(*t) = true;
    get<1>(*t) = false;
    bool v = op(t);
    if (v)
      sparse->push_back((*x)[xn]);
    xn++;
  }
  while (yn < y->size()) {
    get<0>(*t) = false;
    get<1>(*t) = true;
    bool v = op(t);
    if (v)
      sparse->push_back((*y)[yn]);
    yn++;
  }
  delete t;
  return new Vec<int>(sparse);
}

vector<int>* range ( long n1, long n2, long n3 ) {
  vector<int>* a = new vector<int>();
  for ( int i = n1; i <= n2; i += n3 )
    a->push_back(i);
  return a;
}
