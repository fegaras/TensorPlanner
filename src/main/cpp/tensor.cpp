#include <cassert>
#include <cstdlib>
#include <map>
#include "tensor.h"

tuple<tuple<int,int>,vector<double>> f ( int x, int y, vector<double> v ) {
  return {{x,y},v};
}

void test () {
  tuple<tuple<int,int>,int> x = { {1,2}, 6 };
  Vec<double> z = array_buffer_dense(10,2.3);
  vector<double> y;
  cout << get<0>(get<0>(f(1,2,y)));
  y[2] = 8;
}
