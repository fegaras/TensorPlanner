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

#include <cassert>
#include <cstdlib>
#include <vector>
#include <tuple>
#include <map>
#include <iostream>
#include <memory>
#include <thread>
#include <mutex>
#include <malloc.h>
#include "mpi.h"
#include "omp.h"

using namespace std;

extern int block_count;
extern int block_created;
extern int max_blocks;
extern bool trace_delete;

void abort ();


/* Array blocks are C arrays with length */
template< typename T >
class Vec {
private:
  size_t length;
  T* data;
  Vec ( const Vec& ) {}
  Vec<T>* Clone () { return this; }
  Vec<T>& operator= ( const Vec<T> &other ) {
    swap(*this,other);
    return *this;
  }
public:
  Vec ( size_t len = 0 ): length(len) {
    try {
      data = (len == 0) ? nullptr : new T[len];
    } catch ( bad_alloc &ex ) {
      cerr << "*** cannot allocate " << (len*sizeof(T)) << " bytes" << endl;
      abort();
    }
    block_count++;
    block_created++;
    max_blocks = max(max_blocks,block_count);
    if (trace_delete)
      printf("create %p %d\n",this,(int)mallinfo2().arena);
  }

  Vec ( vector<T>* x ): length(x->size()), data(new T[length]) {
    for (int i = 0; i < length; i++ )
      data[i] = (*x)[i];
    block_count++;
    block_created++;
    max_blocks = max(max_blocks,block_count);
    delete x;
    if (trace_delete)
      printf("create %p %d\n",this,(int)mallinfo2().arena);
  }

  inline size_t size () const { return length; }

  inline T* buffer () const { return data; }

  inline T& operator[] ( unsigned int n ) {
    return data[n];
  }

  inline const T& operator[] ( unsigned int n ) const {
    return data[n];
  }

  friend void swap ( Vec<T> x, Vec<T> y ) {
    using std::swap;
    swap(x.length,y.length);
    swap(x.data,y.data);
  }

  ~Vec () {
    block_count--;
    max_blocks = max(max_blocks,block_count);
    delete[] data;
    if (trace_delete)
      printf("delete %p %d\n",this,(int)mallinfo2().arena);
    data = nullptr;
  }
};

template< typename T >
Vec<T>* array_buffer_dense ( size_t dsize, const T zero ) {
  Vec<T>* a = new Vec<T>(dsize);
  T* av = a->buffer();
  #pragma omp parallel for
  for ( int i = 0; i < dsize; i++ )
    av[i] = zero;
  return a;
}

template< typename T >
Vec<T>* merge_tensors ( const Vec<T>* x, const Vec<T>* y, T(*op)(tuple<T,T>*), const T zero ) {
  int len = min(x->size(),y->size());
  Vec<T>* a = new Vec<T>(len);
  T* av = a->buffer();
  T* xv = x->buffer();
  T* yv = y->buffer();
  // don't create a tuple during loop
  static tuple<T,T>* t = new tuple<T,T>(zero,zero);
  #pragma omp parallel for
  for ( int i = 0; i < len; i++ ) {
    get<0>(*t) = xv[i];
    get<1>(*t) = yv[i];
    av[i] = op(t);
  }
  return a;
}

template< typename T >
vector<T>* parallelize ( vector<T>* x ) {
  return x;
}

template< typename T >
vector<T>* elem ( T x ) {
  vector<T>* a = new vector<T>({ x });
  return a;
}

template< typename T >
vector<T>* append1 ( vector<T>* x, const T y ) {
  x->push_back(y);
  return x;
}

template< typename T >
vector<T>* append ( vector<T>* x, vector<T>* y ) {
  for ( int i = 0; i < y->size(); i++ )
    x->push_back((*y)[i]);
  return x;
}

template< typename T, typename S >
S foreach ( void(*f)(T), vector<T>* x, S ret ) {
  for ( T e: *x )
    f(e);
  return ret;
}

template< typename T, typename S >
vector<S>* mapf ( S(*f)(T), vector<T>* x ) {
  vector<S>* a = new vector<S>();
  for ( T e: *x )
    a->push_back(f(e));
  return a;
}

template< typename T, typename S >
vector<S>* flatMap ( vector<S>*(*f)(T), vector<T>* x ) {
  vector<S>* a = new vector<S>();
  for ( T e: *x )
    for ( S u: *(f(e)) )
      a->push_back(u);
  return a;
}

template< typename K, typename T, typename S >
vector<tuple<K*,tuple<T,S>*>*>* join_nl ( vector<tuple<K*,T>*>* x,
                                          vector<tuple<K*,S>*>* y ) {
  vector<tuple<K*,tuple<T,S>*>*>* a = new vector<tuple<K*,tuple<T,S>*>*>();
  for ( tuple<K*,T>* ex: *x )
    for ( tuple<K*,S>* ey: *y )
      if (*get<0>(*ex) == *get<0>(*ey))
        a->push_back(new tuple<K*,tuple<T,S>*>(get<0>(*ex),
                           new tuple<T,S>(get<1>(*ex),get<1>(*ey))));
  return a;
}

template< typename T, typename S >
vector<tuple<long,tuple<T,S>*>*>* join_nl ( vector<tuple<long,T>*>* x,
                                            vector<tuple<long,S>*>* y ) {
  vector<tuple<long,tuple<T,S>*>*>* a = new vector<tuple<long,tuple<T,S>*>*>();
  for ( tuple<long,T>* ex: *x )
    for ( tuple<long,S>* ey: *y )
      if (get<0>(*ex) == get<0>(*ey))
        a->push_back(new tuple<long,tuple<T,S>*>(get<0>(*ex),
                           new tuple<T,S>(get<1>(*ex),get<1>(*ey))));
  return a;
}

template< typename K, typename T >
vector<tuple<K*,T>*>* reduceByKey ( vector<tuple<K*,T>*>* x, T(*op)(tuple<T,T>*) ) {
  vector<tuple<K*,T>*>* a = new vector<tuple<K*,T>*>();
  map<K,tuple<K*,T>*> h;
  for ( tuple<K*,T>* e: *x ) {
    auto p = h.find(*get<0>(*e));
    if (p == h.end())
      h.emplace(*get<0>(*e),e);
    else get<1>(*p->second) = op(new tuple<T,T>(get<1>(*e),get<1>(*p->second)));
  }
  for ( auto e: h )
    a->push_back(e.second);
  return a;
}

template< typename T >
vector<tuple<long,T>*>* reduceByKey ( vector<tuple<long,T>*>* x, T(*op)(tuple<T,T>*) ) {
  vector<tuple<long,T>*>* a = new vector<tuple<long,T>*>();
  map<long,T> h;
  for ( tuple<long,T>* e: *x ) {
    auto p = h.find(get<0>(*e));
    if (p == h.end())
      h.emplace(get<0>(*e),get<1>(*e));
    else p->second = op(new tuple<T,T>(get<1>(*e),p->second));
  }
  for ( auto e: h )
    a->push_back(new tuple<long,T>(e.first,e.second));
  return a;
}

template< typename K, typename T >
vector<tuple<K*,vector<T>*>*>* groupByKey ( vector<tuple<K*,T>*>* x ) {
  vector<tuple<K*,vector<T>*>*>* a = new vector<tuple<K*,vector<T>*>*>();
  map<K,tuple<K*,vector<T>*>*> h;
  for ( tuple<K*,T>* e: *x ) {
    auto p = h.find(*get<0>(*e));
    if (p == h.end())
      h.emplace(*get<0>(*e),new tuple<K*,vector<T>*>(get<0>(*e),elem(get<1>(*e))));
    else get<1>(*p->second)->push_back(get<1>(*e));
  }
  for ( auto e: h )
    a->push_back(e.second);
  return a;
}

template< typename T >
vector<tuple<long,vector<T>*>*>* groupByKey ( vector<tuple<long,T>*>* x ) {
  vector<tuple<long,vector<T>*>*>* a = new vector<tuple<long,vector<T>*>*>();
  map<long,vector<T>*> h;
  for ( tuple<long,T>* e: *x ) {
    auto p = h.find(get<0>(*e));
    if (p == h.end())
      h.emplace(get<0>(*e),elem(get<1>(*e)));
    else p->second->push_back(get<1>(*e));
  }
  for ( auto e: h )
    a->push_back(new tuple<long,vector<T>*>(e.first,e.second));
  return a;
}

template< typename K, typename T, typename S >
vector<tuple<K*,tuple<vector<T>*,vector<S>*>*>*>* cogroup ( vector<tuple<K*,T>*>* x,
                                                            vector<tuple<K*,S>*>* y ) {
  vector<tuple<K*,tuple<vector<T>*,vector<S>*>>>* a = new vector<tuple<K*,tuple<vector<T>*,vector<S>*>>>();
  map<K,tuple<vector<T>*,vector<S>*>*> h;
  for ( tuple<K*,T>* e: *x ) {
    auto p = h.find(*get<0>(*e));
    if (p == h.end())
      h.emplace(*get<0>(*e),
                new tuple<vector<T>*,vector<S>*>(elem(get<1>(*e)),new vector<S>()));
    else get<0>(p->second)->push_back(get<1>(*e));
  }
  for ( tuple<K*,S>* e: *y ) {
    auto p = h.find(*get<0>(*e));
    if (p == h.end())
      h.emplace(*get<0>(*e),
                new tuple<vector<T>*,vector<S>*>(new vector<T>(),elem(get<1>(*e))));
    else get<1>(p->second)->push_back(get<1>(*e));
  }
  for ( auto e: h )
    a->push_back(new tuple<K*,tuple<vector<T>*,vector<S>*>*>(&e.first,e.second));
  return a;
}

template< typename T, typename S >
vector<tuple<long,tuple<vector<T>*,vector<S>*>*>*>* cogroup ( vector<tuple<long,T>*>* x,
                                                              vector<tuple<long,S>*>* y ) {
  vector<tuple<long,tuple<vector<T>*,vector<S>*>>>* a = new vector<tuple<long,tuple<vector<T>*,vector<S>*>>>();
  map<long,tuple<vector<T>*,vector<S>>*> h;
  for ( tuple<long,T>* e: *x ) {
    auto p = h.find(get<0>(*e));
    if (p == h.end())
      h.emplace(get<0>(*e),
                new tuple<vector<T>*,vector<S>*>(elem(get<1>(*e)),new vector<S>()));
    else get<0>(p->second)->push_back(get<1>(*e));
  }
  for ( tuple<long,S>* e: *y ) {
    auto p = h.find(get<0>(*e));
    if (p == h.end())
      h.emplace(get<0>(*e),
                new tuple<vector<T>*,vector<S>*>(new vector<T>(),elem(get<1>(*e))));
    else get<1>(p->second)->push_back(get<1>(*e));
  }
  for ( auto e: h )
    a->push_back(new tuple<long,tuple<vector<T>*,vector<S>*>*>(e.first,e.second));
  return a;
}

template< typename K, typename T, typename S >
vector<tuple<K*,tuple<T,S>*>*>* join ( vector<tuple<K*,T>*>* x,
                                       vector<tuple<K*,S>*>* y ) {
  vector<tuple<K*,tuple<T,S>*>*>* a = new vector<tuple<K*,tuple<T,S>*>*>();
  map<K,vector<T>*> h;
  for ( tuple<K*,T>* ex: *x ) {
    auto p = h.find(*get<0>(*ex));
    if (p == h.end())
      h.emplace(*get<0>(*ex),elem(get<1>(*ex)));
    else p->second->push_back(get<1>(*ex));
  }
  for ( tuple<K*,S>* ey: *y ) {
    auto p = h.find(*get<0>(*ey));
    if (p != h.end())
      for ( T ex: *p->second )
        a->push_back(new tuple<K*,tuple<T,S>*>(get<0>(*ey),new tuple<T,S>(ex,get<1>(*ey))));
  }
  return a;
}

template< typename T, typename S >
vector<tuple<long,tuple<T,S>*>*>* join ( vector<tuple<long,T>*>* x,
                                         vector<tuple<long,S>*>* y ) {
  vector<tuple<long,tuple<T,S>*>*>* a = new vector<tuple<long,tuple<T,S>*>*>();
  map<long,vector<T>*> h;
  for ( tuple<long,T>* ex: *x ) {
    auto p = h.find(get<0>(*ex));
    if (p == h.end())
      h.emplace(get<0>(*ex),elem(get<1>(*ex)));
    else p->second->push_back(get<1>(*ex));
  }
  for ( tuple<long,S>* ey: *y ) {
    auto p = h.find(get<0>(*ey));
    if (p != h.end())
      for ( T ex: *p->second )
        a->push_back(new tuple<long,tuple<T,S>*>(get<0>(*ey),new tuple<T,S>(ex,get<1>(*ey))));
  }
  return a;
}

template< typename T >
Vec<T>* array_buffer ( int dsize, int ssize, T zero,
                       tuple<Vec<int>*,Vec<int>*,Vec<T>*>* init = nullptr ) {
  Vec<T>* buffer = new Vec<T>(dsize*ssize);
  T* bv = buffer->buffer();
  #pragma omp parallel for
  for (int i = 0; i < buffer->size(); i++ )
    bv[i] = zero;
  if (init != nullptr) {
    #pragma omp parallel for
    for ( int i = 0; i < get<0>(*init)->size()-1; i++ ) {
      int j = (*get<0>(*init))[i];
      while (j < (*get<0>(*init))[i+1]) {
        bv[i*ssize+(*get<1>(*init))[j]] = (*get<2>(*init))[j];
        j++;
      }
    }
  }
  return buffer;
}

// convert a dense array to a tensor (dense dimensions dn>0 & sparse dimensions sn>0)
template< typename T >
tuple<Vec<int>*,Vec<int>*,Vec<T>*>*
     array2tensor ( int dn, int sn, T zero, Vec<T>* buffer ) {
  T* bv = buffer->buffer();
  auto dense = new Vec<int>(dn+1);
  int* dv = dense->buffer();
  auto sparse = new vector<int>();
  auto values = new vector<T>();
  dv[0] = 0;
  for ( int i = 0; i < dn; i++ ) {
    for ( int j = 0; j < sn; j++ ) {
      T v = bv[i*sn+j];
      if (v != zero) {
        sparse->push_back(j);
        values->push_back(v);
      }
    }
    dv[i+1] = sparse->size();
  }
  return new tuple<Vec<int>*,Vec<int>*,Vec<T>*>(dense,new Vec<int>(sparse),new Vec<T>(values));
}

template< typename T >
tuple<Vec<int>*,Vec<int>*,Vec<T>*>*
     merge_tensors ( tuple<Vec<int>*,Vec<int>*,Vec<T>*>* x,
                     tuple<Vec<int>*,Vec<int>*,Vec<T>*>* y,
                     T(*op)(tuple<T,T>*), T zero ) {
  int i = 0;
  int len = min(get<0>(*x)->size(),get<0>(*y)->size())-1;
  auto dense = new Vec<int>(len+1);
  int* dv = dense->buffer();
  auto sparse = new vector<int>();
  auto values = new vector<T>();
  dv[0] = 0;
  // don't create a tuple during loop
  static tuple<T,T>* t = new tuple<T,T>(zero,zero);
  while (i < len) {
    int xn = (*get<0>(*x))[i];
    int yn = (*get<0>(*y))[i];
    while (xn < (*get<0>(*x))[i+1] && yn < (*get<0>(*y))[i+1]) {
      if ((*get<1>(*x))[xn] == (*get<1>(*y))[yn]) {
        get<0>(*t) = (*get<2>(*x))[xn];
        get<1>(*t) = (*get<2>(*y))[yn];
        T v = op(t);
        if (v != zero) {
          sparse->push_back((*get<1>(*x))[xn]);
          values->push_back(v);
        }
        xn++; yn++;
      } else if ((*get<1>(*x))[xn] < (*get<1>(*y))[yn]) {
        get<0>(*t) = (*get<2>(*x))[xn];
        get<1>(*t) = zero;
        T v = op(t);
        if (v != zero) {
          sparse->push_back((*get<1>(*x))[xn]);
          values->push_back(v);
        }
        xn++;
      } else {
        get<0>(*t) = zero;
        get<1>(*t) = (*get<2>(*y))[yn];
        T v = op(t);
        if (v != zero) {
          sparse->push_back((*get<1>(*y))[yn]);
          values->push_back(v);
        }
        yn++;
      }
    }
    while (xn < (*get<0>(*x))[i+1]) {
      get<0>(*t) = (*get<2>(*x))[xn];
      get<1>(*t) = zero;
      T v = op(t);
      if (v != zero) {
        sparse->push_back((*get<1>(*x))[xn]);
        values->push_back(v);
      }
      xn++;
    }
    while (yn < (*get<0>(*y))[i+1]) {
      get<0>(*t) = zero;
      get<1>(*t) = (*get<2>(*y))[yn];
      T v = op(t);
      if (v != zero) {
        sparse->push_back((*get<1>(*y))[yn]);
        values->push_back(v);
      }
      yn++;
    }
    i++;
    dv[i] = sparse->size();
  }
  return new tuple<Vec<int>*,Vec<int>*,Vec<T>*>(dense,new Vec<int>(sparse),new Vec<T>(values));
}

template< typename T >
T binarySearch ( int key, int from, int to, const Vec<int>* rows, const Vec<T>* values, T zero ) {
  while (from <= to) {
    int middle = (from+to)/2;
    if ((*rows)[middle] == key)
      return (*values)[middle];
    else if ((*rows)[middle] > key)
      to = middle-1;
    else from = middle+1;
  }
  return zero;
}

template< typename T >
bool binarySearch ( int key, int from, int to, const Vec<int>* rows, const Vec<T>* values ) {
  while (from <= to) {
    int middle = (from+to)/2;
    if ((*rows)[middle] == key)
      return true;
    else if ((*rows)[middle] > key)
      to = middle-1;
    else from = middle+1;
  }
  return false;
}
