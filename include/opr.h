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


enum Status {notReady,scheduled,ready,computed,completed,removed,locked,zombie};

enum OprType { applyOPR=0, pairOPR=1, loadOPR=2, reduceOPR=3 };

static const char* oprNames[] = { "apply", "pair", "load", "reduce" };

static const vector<int>* empty_vector = new vector<int>();

class Opr;

class LoadOpr {
public:
  int block;
  LoadOpr ( int block ) { this->block = block; }
  size_t hash () { return (block << 2) ^ 1; }
  bool operator== ( const LoadOpr& o ) const {
    return o.block == block;
  }
};

class PairOpr {
public:
  int x;
  int y;
  PairOpr ( int x, int y ) { this->x = x; this->y = y; }
  size_t hash () { return (x << 2) ^ (y << 3) ^ 2; }
  bool operator== ( const PairOpr& o ) const {
    return o.x == x && o.y == y;
  }
};

class ApplyOpr {
public:
  int x;
  int fnc;
  void* extra_args;
  ApplyOpr ( int x, int fnc, void* extra_args ) {
    this->x = x;
    this->fnc = fnc;
    this->extra_args = extra_args;
  }
  size_t hash () { return (x << 2) ^ (fnc << 3) ^ 4; }
  bool operator== ( const ApplyOpr& o ) const {
    return o.x == x && o.fnc == fnc;
  }
};

class ReduceOpr {
public:
  vector<int>* s;
  bool valuep;
  int op;
  ReduceOpr ( vector<int>* s, bool valuep, int op ) {
    this->s = s;
    this->valuep = valuep;
    this->op = op;
  }
  size_t hash () {
    size_t h = 0;
    for ( int i: *s )
      h = i ^ (h << 1);
    return h ^ (op << 2) ^ 2;
  }
  bool operator== ( const ReduceOpr& o ) const {
    return o.op == op && o.valuep == valuep && *o.s == *s;
  }
};

class Opr {
public:
  int node = 0;
  void* coord = nullptr;
  bool int_index = false;
  int size = -1;
  int static_blevel = -1;
  enum Status status = notReady;
  bool visited = false;
  void* cached = nullptr;
  vector<int>* encoded_type = nullptr;
  vector<int>* children;
  vector<int>* consumers;
  vector<int>* dependents;
  int count;
  int reduced_count;
  int first_reduced_input = -1;
  int cpu_cost;
  vector<int>* os;
  int oc;
  enum OprType type;
  union {
    LoadOpr* load_opr;
    PairOpr* pair_opr;
    ApplyOpr* apply_opr;
    ReduceOpr* reduce_opr;
  } opr;
  Opr () {}
  size_t hash () {
    switch (type) {
    case loadOPR: return opr.load_opr->hash();
    case pairOPR: return opr.pair_opr->hash();
    case applyOPR: return opr.apply_opr->hash();
    case reduceOPR: return opr.reduce_opr->hash();
    default: return 0;
    }
  }
  bool operator== ( const Opr& o ) const {
    if (o.type == type)
      return false;
    switch (type) {
    case loadOPR: return *opr.load_opr == *o.opr.load_opr;
    case pairOPR: return *opr.pair_opr == *o.opr.pair_opr;
    case applyOPR: return *opr.apply_opr == *o.opr.apply_opr;
    case reduceOPR: return *opr.reduce_opr == *o.opr.reduce_opr;
    default: return false;
    }
  }
};

void enqueue_ready_operations ( int opr_id );
void cache_data ( Opr* opr, const void* data );
