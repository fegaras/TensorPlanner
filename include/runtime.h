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
#include "comm.h"

extern vector<void*(*)(void*)> functions;

vector<int>* range ( long n1, long n2, long n3 );

inline bool inRange ( long i, long n1, long n2, long n3 ) {
  return i>=n1 && i<= n2 && (i-n1)%n3 == 0;
}

Vec<bool>* array_buffer_bool ( int dsize, int ssize, tuple<Vec<int>*,Vec<bool>*>* init = nullptr );
Vec<bool>* array_buffer_sparse_bool ( int ssize, Vec<int>* init = nullptr );
tuple<Vec<int>*,Vec<int>*>* array2tensor_bool ( int dn, int sn, Vec<bool>* buffer );

tuple<Vec<int>*,Vec<int>*>* array2tensor_bool ( int dn, int sn, Vec<bool>* buffer );

Vec<int>* array2tensor_bool ( int sn, Vec<bool>* buffer );

tuple<Vec<int>*,Vec<int>*>*
     merge_tensors ( tuple<Vec<int>*,Vec<int>*>* x,
                     tuple<Vec<int>*,Vec<int>*>* y,
                     bool(*op)(tuple<bool,bool>*), bool zero );

Vec<int>* merge_tensors ( Vec<int>* x, Vec<int>* y,
                          bool(*op)(tuple<bool,bool>*), bool zero );

int loadOpr ( void* block, void* coord, vector<int>* encoded_type );
int loadOpr ( void* block, long coord, vector<int>* encoded_type ) {
  return loadOpr(block,(void*)coord,encoded_type);
}

int pairOpr ( int x, int y, void* coord, vector<int>* encoded_type );
int pairOpr ( int x, int y, long coord, vector<int>* encoded_type ) {
  return pairOpr(x,y,(void*)coord,encoded_type);
}

int applyOpr ( int x, int fnc, void* args, void* coord, int cost, vector<int>* encoded_type );
int applyOpr ( int x, int fnc, void* args, long coord, int cost, vector<int>* encoded_type ) {
  return applyOpr(x,fnc,args,(void*)coord,cost,encoded_type);
}

int reduceOpr ( const vector<long>* s, bool valuep, int op, void* coord, int cost, vector<int>* encoded_type );
int reduceOpr ( const vector<long>* s, bool valuep, int op, long coord, int cost, vector<int>* encoded_type ) {
  return reduceOpr(s,valuep,op,(void*)coord,cost,encoded_type);
}

void* evalTopDown ( void* plan );

void schedule ( void* plan );

void* eval ( void* plan );

void* evalOpr ( int opr_id );

void* collect ( void* plan );

void startup ( int argc, char* argv[], int block_dim_size );
