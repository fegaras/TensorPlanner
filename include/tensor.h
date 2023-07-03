#include <cassert>
#include <cstdlib>
#include <vector>
#include <tuple>
#include <map>
#include <iostream>

using namespace std;

/* C arrays with length */
template< typename T >
class Vec {
public:
  size_t length;
  T* data;

  // required if we put it in a tuple
  Vec () {}

  Vec ( size_t len ) {
    length = len;
    data = (T*)malloc(len*sizeof(T));
    
  }

  T& get ( unsigned int n ) {
    assert(n < length);
    return data[n];
  }

  T& operator[] ( unsigned int n ) {
    return get(n);
  }

  void set ( unsigned int n, T value ) {
    assert(n < length);
    data[n] = value;
  }
};

bool inRange ( long i, long n1, long n2, long n3 ) {
  return i>=n1 && i<= n2 && (i-n1)%n3 == 0;
}

template< typename T >
Vec<T> array_buffer_dense ( size_t dsize, T zero ) {
  Vec<T> a = Vec<T>(dsize);
  for ( int i = 0; i < dsize; i++ )
    a[i] = zero;
  return a;
}

template< typename T >
Vec<T> merge_tensors ( Vec<T> x, Vec<T> y, T(*op)(tuple<T,T>), T zero ) {
  Vec<T> a = Vec<T>(x.length);
  for ( int i = 0; i < x.length; i++ )
    a[i] = op({x[i],y[i]});
  return a;
}

template< typename T >
vector<T> parallelize ( vector<T> x ) {
  return x;//vector<T>(x.length,x.data);
}

template< typename T, typename S >
vector<S> flatMap ( S(*f)(T), vector<T> x ) {
  vector<S> a = vector<S>();
  for ( T e: x )
    a.push_back(f(e));
  return a;
}

template< typename K, typename T, typename S >
vector<tuple<K,tuple<T,S>>> join ( vector<tuple<K,T>> x, vector<tuple<K,S>> y ) {
  vector<tuple<K,tuple<T,S>>> a = vector<tuple<K,tuple<T,S>>>();
  for ( tuple<K,T> ex: x )
    for ( tuple<K,S> ey: y ) {
      if (get<0>(ex) == get<0>(ey))
        a.push_back(make_tuple(get<0>(ex),make_tuple(get<1>(ex),get<1>(ey))));
    }
  return a;
}

template< typename K, typename T >
vector<tuple<K,T>> reduceByKey ( vector<tuple<K,T>> x, T(*op)(tuple<T,T>) ) {
  vector<tuple<K,T>> a = vector<tuple<K,T>>();
  map<K,T> h;
  for ( tuple<K,T> e: x )
    if (h.count(get<0>(e)) == 0)
      h.emplace(get<0>(e),get<1>(e));
    else { T old = h.find(get<0>(e))->second;
           h.emplace(get<0>(e),op(make_tuple(get<1>(e),old)));
         }
  for ( auto e: h )
    a.push_back(e);
  return a;
}

template< typename K, typename T >
vector<tuple<K,T>> groupByKey ( vector<tuple<K,T>> x ) {
  vector<tuple<K,vector<T>>> a = vector<tuple<K,vector<T>>>();
  map<K,vector<T>> h;
  for ( tuple<K,T> e: x )
    if (h.count(get<0>(e)) == 0)
      h.emplace(get<0>(e),
                vector<T>(get<1>(e)));
    else h.emplace(get<0>(e),
                   h.find(get<0>(e))->second.push_back(get<1>(e)));
  for ( auto e: h )
    a.push_back(e);
  return a;
}
