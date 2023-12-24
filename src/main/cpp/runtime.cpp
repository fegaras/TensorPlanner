/*
 * Copyright © 2023 University of Texas at Arlington
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

#include <vector>
#include <tuple>
#include <queue>
#include <cassert>
#include <chrono>
#include <algorithm>
#include <stdarg.h>
#include "tensor.h"

class Opr;

const bool trace = true;

static vector<Opr*> operations;
vector<void*(*)(void*)> functions;
static map<size_t,int> task_table;
static vector<void*> loadBlocks;
static vector<int>* exit_points;
static int executor_rank = 0;

static int block_count = 0;
static int block_created = 0;

enum Status {notReady,scheduled,ready,computed,completed,removed,locked,zombie};

enum OprType { applyOPR=0, pairOPR=1, loadOPR=2, reduceOPR=3 };

const char* oprNames[] = { "apply", "pair", "load", "reduce" };

static const vector<int>* empty_vector = new vector<int>();


bool compare_opr ( int x, int y );
auto cmp = [] ( int x, int y ) { return compare_opr(x,y); };
static priority_queue<int,vector<int>,decltype(cmp)> ready_queue(cmp);


static char buffer[256];
void info ( int tabs, const char *fmt, ... ) {
  using namespace std::chrono;
  if (trace) {
    auto millis = duration_cast<milliseconds>(system_clock::now()
                                  .time_since_epoch()).count() % 1000;
    time_t now = time(0);
    tm* t = localtime(&now);
    va_list args;
    va_start(args,tabs);
    for ( int i = 0; i < tabs*3; i++ )
      buffer[i] = ' ';
    vsprintf(buffer+3*tabs,fmt,args);
    va_end(args);
    printf("[%02d:%02d:%02d:%03d] %s\n",
           t->tm_hour,t->tm_min,t->tm_sec,millis,buffer);
  }
}

vector<int>* range ( long n1, long n2, long n3 ) {
  vector<int>* a = new vector<int>();
  for ( int i = n1; i <= n2; i += n3 )
    a->push_back(i);
  return a;
}

bool inRange ( long i, long n1, long n2, long n3 ) {
  return i>=n1 && i<= n2 && (i-n1)%n3 == 0;
}

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
  int node = -1;
  void* coord = NULL;
  bool int_index = false;
  int size = -1;
  int static_blevel = -1;
  enum Status status = notReady;
  bool visited = false;
  void* cached = NULL;
  vector<int>* encoded_type = NULL;
  vector<int>* children;
  vector<int>* consumers;
  int count;
  int reduced_count;
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

bool compare_opr ( int x, int y ) {
  return operations[x]->type > operations[y]->type;
};

int store_opr ( Opr* opr, const vector<int>* children,
                void* coord, int cost, vector<int>* encoded_type ) {
  opr->consumers = new vector<int>();
  opr->coord = coord;
  opr->cpu_cost = cost;
  opr->encoded_type = encoded_type;
  size_t key = opr->hash();
  auto p = task_table.find(key);
  if (p != task_table.end())
    if (operations[p->second] == opr)
      return p->second;
  int loc = operations.size();
  operations.push_back(opr);
  task_table.emplace(pair<size_t,int>(key,loc));
  opr->children = children;
  children->shrink_to_fit();
  for ( int c: *children ) {
    auto cs = operations[c]->consumers;
    if (find(cs->begin(),cs->end(),loc) == cs->end())
      cs->push_back(loc);
  }
  return loc;
}

int loadOpr ( void* block, void* coord, vector<int>* encoded_type ) {
  Opr* o = new Opr();
  o->type = loadOPR;
  o->opr.load_opr = new LoadOpr(loadBlocks.size());
  int loc = store_opr(o,empty_vector,coord,0,encoded_type);
  loadBlocks.push_back(block);
  return loc;
}

int loadOpr ( void* block, int coord, vector<int>* encoded_type ) {
  int* cp = new int(coord);
  int loc = loadOpr(block,cp,encoded_type);
  operations[loc]->int_index = true;
  return loc;
};

int pairOpr ( int x, int y, void* coord, vector<int>* encoded_type ) {
  Opr* o = new Opr();
  o->type = pairOPR;
  o->opr.pair_opr = new PairOpr(x,y);
  return store_opr(o,new vector<int>({ x, y }),coord,0,encoded_type);
}

int pairOpr ( int x, int y, int coord, vector<int>* encoded_type ) {
  int* cp = new int(coord);
  int loc = pairOpr(x,y,cp,encoded_type);
  operations[loc]->int_index = true;
  return loc;
};

int applyOpr ( int x, int fnc, void* args, void* coord, int cost, vector<int>* encoded_type ) {
  Opr* o = new Opr();
  o->type = applyOPR;
  o->opr.apply_opr = new ApplyOpr(x,fnc,args);
  return store_opr(o,new vector<int>({ x }),coord,cost,encoded_type);
}

int applyOpr ( int x, int fnc, void* args, int coord, int cost, vector<int>* encoded_type ) {
  int* cp = new int(coord);
  int loc = applyOpr(x,fnc,args,cp,cost,encoded_type);
  operations[loc]->int_index = true;
  return loc;
};

int reduceOpr ( const vector<int>* s, bool valuep, int op,
                void* coord, int cost, vector<int>* encoded_type ) {
  if (s->size() == 1)
    return (*s)[0];
  Opr* o = new Opr();
  o->type = reduceOPR;
  o->opr.reduce_opr = new ReduceOpr(s,valuep,op);
  return store_opr(o,s,coord,cost,encoded_type);
}

int reduceOpr ( const vector<int>* s, bool valuep, int op,
                int coord, int cost, vector<int>* encoded_type ) {
  int* cp = new int(coord);
  int loc = reduceOpr(s,valuep,op,cp,cost,encoded_type);
  operations[loc]->int_index = true;
  return loc;
};

void BFS ( const vector<int>* s, void(*f)(int) ) {
  for ( Opr* x: operations )
    x->visited = false;
  queue<int> opr_queue;
  for ( int c: *s )
    opr_queue.push(c);
  while (!opr_queue.empty()) {
    int c = opr_queue.front();
    opr_queue.pop();
    Opr* opr = operations[c];
    if (!opr->visited) {
      opr->visited = true;
      f(c);
      switch (opr->type) {
        case pairOPR:
          opr_queue.push(opr->opr.pair_opr->x);
          opr_queue.push(opr->opr.pair_opr->y);
          break;
        case applyOPR:
          opr_queue.push(opr->opr.apply_opr->x);
          break;
        case reduceOPR:
          for ( int x: *opr->opr.reduce_opr->s )
            opr_queue.push(x);
          break;
      }
    }
  }
}

bool hasCachedValue ( Opr* x ) {
  return (x->status == computed || x->status == completed
          || x->status == locked || x->status == zombie);
}

void cache_data ( Opr* opr, const void* data ) {
  opr->cached = data;
}

int delete_array ( void* data, vector<int>* encoded_type, int loc ) {
  switch ((*encoded_type)[loc]) {
  case 10: { // tuple
    if ((*encoded_type)[loc+2] == 0)
      return loc+(*encoded_type)[loc+1]+2;
    else {
      switch ((*encoded_type)[loc+1]) {
      case 0:
        return loc+2;
      case 2: {
        tuple<void*,void*>* x = data;
        int l2 = delete_array(get<0>(*x),encoded_type,loc+2);
        return delete_array(get<1>(*x),encoded_type,l2);
      }
      case 3: {
        tuple<void*,void*,void*>* x = data;
        int l2 = delete_array(get<0>(*x),encoded_type,loc+2);
        int l3 = delete_array(get<1>(*x),encoded_type,l2);
        return delete_array(get<2>(*x),encoded_type,l3);
      }
      }
    }
  }
  case 11:  // Vec
    switch ((*encoded_type)[loc+1]) {
    case 0: {
      Vec<int>* x = data;
      delete x;
      return loc+2;
    }
    case 1: {
      Vec<long>* x = data;
      delete x;
      return loc+2;
    }
    case 3: {
      Vec<double>* x = data;
      delete x;
      return loc+2;
    }
    }
  default:
    return loc+1;
  }
}

void delete_array ( int opr_id ) {
  Opr* opr = operations[opr_id];
  if (opr->cached != NULL
      && (opr->type == applyOPR || opr->type == reduceOPR)
      && opr->encoded_type->size() > 0 ) {
    info(0,"    delete block %d (%d/%d)",opr_id,block_count,block_created);
    delete_array(opr->cached,opr->encoded_type,0);
    opr->status = removed;
  }
}

void delete_if_ready ( int opr_id ) {
  Opr* opr = operations[opr_id];
  opr->count--;
   if (opr->count <= 0) {
     if (opr->type == pairOPR) {
       delete_if_ready(opr->opr.pair_opr->x);
       delete_if_ready(opr->opr.pair_opr->y);
     } else if (opr->type == applyOPR && opr->encoded_type->size() == 0)
              delete_if_ready(opr->opr.apply_opr->x);
            else delete_array(opr_id);
   }
}

void check_caches ( int opr_id ) {
  if (operations[opr_id]->encoded_type->size() > 0)
    for ( int c: *operations[opr_id]->children )
      delete_if_ready(c);
}

void* inMemEval ( int id, int tabs );

void* inMemEvalPair ( int lg, int tabs ) {
  switch (operations[lg]->type) {
    case pairOPR: {
      int x = operations[lg]->opr.pair_opr->x;
      int y = operations[lg]->opr.pair_opr->y;
      void* gx = inMemEvalPair(x,tabs);
      void* gy = inMemEvalPair(y,tabs);
      void* yy;
      if (operations[y]->int_index) {
        tuple<int,void*>* gty = gy;
        yy = (operations[y]->type == pairOPR) ? gy : get<1>(*gty);
      } else {
        tuple<void*,void*>* gty = gy;
        yy = (operations[y]->type == pairOPR) ? gy : get<1>(*gty);
      }
      void* xx;
      if (operations[x]->int_index) {
        tuple<int,void*>* gtx = gx;
        xx = (operations[x]->type == pairOPR) ? gx : get<1>(*gtx);
        return new tuple<int,void*>(get<0>(*gtx),new tuple(xx,yy));
      } else {
        tuple<void*,void*>* gtx = gx;
        xx = (operations[x]->type == pairOPR) ? gx : get<1>(*gtx);
        return new tuple<void*,void*>(get<0>(*gtx),new tuple(xx,yy));
      }
    }
    default: return inMemEval(lg,tabs+1);
  }
}

void* inMemEval ( int id, int tabs ) {
  Opr* e = operations[id];
  if (hasCachedValue(e))
    return e->cached;
  info(tabs,"*** %d: %d %s %p",tabs,id,oprNames[e->type],e);
  void* res = NULL;
  switch (e->type) {
    case loadOPR:
      res = loadBlocks[e->opr.load_opr->block];
      break;
    case applyOPR: {
      vector<void*>*(*f)(void*) = functions[e->opr.apply_opr->fnc];
      res = (*f(inMemEval(e->opr.apply_opr->x,tabs+1)))[0];
      break;
    }
    case pairOPR:
      res = inMemEvalPair(id,tabs);
      break;
    case reduceOPR: {
      vector<int>* rs = e->opr.reduce_opr->s;
      if (operations[(*rs)[0]]->int_index) {
        vector<tuple<int,void*>*>* sv = new vector<tuple<int,void*>*>();
        for ( int x: *rs ) {
          tuple<int,void*>* v = inMemEval(x,tabs+1);
          sv->push_back(v);
        }
        void*(*op)(tuple<void*,void*>*) = functions[e->opr.reduce_opr->op];
        void* av = NULL;
        for ( tuple<int,void*>* v: *sv ) {
          if (av == NULL)
            av = get<1>(*v);
          else av = op(new tuple<void*,void*>(av,get<1>(*v)));
        }
        res = new tuple(get<0>(*(*sv)[0]),av);
      } else {
        vector<tuple<void*,void*>*>* sv = new vector<tuple<void*,void*>*>();
        for ( int x: *rs ) {
          tuple<void*,void*>* v = inMemEval(x,tabs+1);
          sv->push_back(v);
        }
        void*(*op)(tuple<void*,void*>*) = functions[e->opr.reduce_opr->op];
        void* av = NULL;
        for ( tuple<void*,void*>* v: *sv ) {
          if (av == NULL)
            av = get<1>(*v);
          else av = op(new tuple<void*,void*>(av,get<1>(*v)));
        }
        res = new tuple(get<0>(*(*sv)[0]),av);
      }
      break;
    }
  }
  info(tabs,"*-> %d: %s %p",tabs,oprNames[e->type],res);
  e->cached = res;
  e->status = completed;
  return res;
}

void* evalTopDown ( void* plan ) {
  tuple<void*,void*,vector<tuple<void*,int>*>*>* p = plan;
  for ( Opr* x: operations )
    x->count = x->consumers->size();
  vector<tuple<void*,void*>*>* res = new vector<tuple<void*,void*>*>();
  for ( tuple<void*,int>* d: *get<2>(*p) ) {
    tuple<void*,void*>* v = inMemEval(get<1>(*d),0);
    res->push_back(v);
  }
  return new tuple(get<0>(*p),get<1>(*p),res);
}

void* compute ( int lg_id );

void* computePair ( int lg_id ) {
  Opr* lg = operations[lg_id];
  if (hasCachedValue(lg)) {
    assert(lg->cached !- NULL);
    return lg->cached;
  }
  switch (lg->type) {
    case pairOPR: {
      int x = lg->opr.pair_opr->x;
      int y = lg->opr.pair_opr->y;
      void* gx = computePair(x);
      void* gy = computePair(y);
      void* yy;
      if (operations[y]->int_index) {
        tuple<int,void*>* gty = gy;
        yy = (operations[y]->type == pairOPR) ? gy : get<1>(*gty);
      } else {
        tuple<void*,void*>* gty = gy;
        yy = (operations[y]->type == pairOPR) ? gy : get<1>(*gty);
      }
      void* xx;
      void* data;
      if (operations[x]->int_index) {
        tuple<int,void*>* gtx = gx;
        xx = (operations[x]->type == pairOPR) ? gx : get<1>(*gtx);
        data = new tuple<int,void*>(get<0>(*gtx),new tuple(xx,yy));
      } else {
        tuple<void*,void*>* gtx = gx;
        xx = (operations[x]->type == pairOPR) ? gx : get<1>(*gtx);
        data = new tuple<void*,void*>(get<0>(*gtx),new tuple(xx,yy));
      }
      cache_data(lg,data);
      lg->status = completed;
      return data;
    }
    default:
      assert(lg->cached !- NULL);
      return lg->cached;
  }
}

void* compute ( int opr_id ) {
  Opr* opr = operations[opr_id];
  info(0,"*** computing %d: %s %p",opr_id,oprNames[opr->type],opr);
  void* res = NULL;
  switch (opr->type) {
    case loadOPR:
      opr->status = computed;
      res = loadBlocks[opr->opr.load_opr->block];
      cache_data(opr,res);
      break;
    case applyOPR: {
      vector<void*>*(*f)(void*) = functions[opr->opr.apply_opr->fnc];
      opr->status = computed;
      res = (*(f(operations[opr->opr.apply_opr->x]->cached)))[0];
      cache_data(opr,res);
      break;
    }
    case pairOPR:
      res = computePair(opr_id);
      break;
    case reduceOPR: {
      // not used if partial reduce is enabled
      vector<int>* rs = opr->opr.reduce_opr->s;
      void*(*op)(tuple<void*,void*>*) = functions[opr->opr.reduce_opr->op];
      if (operations[(*rs)[0]]->int_index) {
        void* acc = NULL;
        int key = 0;
        for ( int x: *rs ) {
          tuple<int,void*>* v = operations[x]->cached;
          if (acc == NULL) {
            key = get<0>(*v);
            acc = get<1>(*v);
          } else acc = op(new tuple<void*,void*>(get<1>(*v),acc));
        }
        opr->status = completed;
        res = new tuple(key,acc);
        cache_data(opr,res);
      } else {
        void* acc = NULL;
        void* key = 0;
        for ( void* x: *rs ) {
          tuple<void*,void*>* v = operations[x]->cached;
          if (acc == NULL) {
            key = get<0>(*v);
            acc = get<1>(*v);
          } else acc = op(new tuple<void*,void*>(get<1>(*v),acc));
        }
        opr->status = completed;
        res = new tuple(key,acc);
        cache_data(opr,res);
      }
      break;
    }
  }
  info(0,"*-> result of opr %d: %s %p",opr_id,oprNames[opr->type],res);
  return res;
}

void enqueue_ready_operations ( int opr_id );

void enqueue_reduce_opr ( int opr_id, int c ) {
  Opr* opr = operations[opr_id];  // child of copr
  Opr* copr = operations[c];      // ReduceOpr
  // partial reduce opr inside the copr ReduceOpr
  void*(*op)(tuple<void*,void*>*) = functions[copr->opr.reduce_opr->op];
  if (copr->cached == NULL)
    copr->cached = opr->cached;
  else if (operations[(*copr->opr.reduce_opr->s)[0]]->int_index) {
    tuple<int,void*>* x = copr->cached;
    tuple<int,void*>* y = opr->cached;
    get<1>(*x) = op(new tuple<void*,void*>(get<1>(*x),get<1>(*y)));
    delete_if_ready(opr_id);
  } else {
    tuple<void*,void*>* x = copr->cached;
    tuple<void*,void*>* y = opr->cached;
    get<1>(*x) = op(new tuple<void*,void*>(get<1>(*x),get<1>(*y)));
    delete_if_ready(opr_id);
  }
  copr->reduced_count--;
  if (copr->reduced_count == 0) {
    info(0,"    completed reduce of opr %d",c);
    copr->status = computed;
    enqueue_ready_operations(c);
  }
}

// After opr is computed, check its consumers to see if anyone is ready to enqueue
void enqueue_ready_operations ( int opr_id ) {
  Opr* opr = operations[opr_id];
  for ( int c: *opr->consumers ) {
    Opr* copr = operations[c];
    switch (copr->type) {
    case reduceOPR: {
      enqueue_reduce_opr(opr_id,c);
      break;
    }
    default:
      if (!hasCachedValue(copr)) {
        bool cv = true;
        for ( int c: *copr->children )
          cv = cv && hasCachedValue(operations[c]);
        if (cv && copr->status != ready) {
          copr->status = ready;
          ready_queue.push(c);
        }
      }
    }
  }
  opr->status = completed;
}

vector<int>* entry_points ( const vector<int>* es ) {
  static vector<int>* b = new vector<int>();
  BFS(es,
      [] ( int c ) {
        Opr* copr = operations[c];
        if (copr->type == loadOPR) {
          bool member = false;
          for ( int x: *b )
            member = member || x == c;
          if (!member)
            b->push_back(c);
        }
      });
  return b;
}

void initialize_opr ( Opr* x ) {
  x->status = notReady;
  x->cached = NULL;
  if (x->type == loadOPR)
    cache_data(x,loadBlocks[x->opr.load_opr->block]);
  x->count = 0;
  for ( int c: *x->consumers )
    x->count++;
  if (x->type == reduceOPR) {
    x->reduced_count = 0;
    for ( int c: *x->opr.reduce_opr->s )
      x->reduced_count++;
  }
}

void* eval ( void* plan ) {
  if (trace) {
    cout << "Plan:" << endl;
    for ( int i = 0; i < operations.size(); i++ ) {
      Opr* opr = operations[i];
      cout << "   " << i << ":" << oprNames[opr->type] << "(";
      for ( int c: *opr->children )
        cout << " " << c;
      cout << " ) consumers: (";
      for ( int c: *opr->consumers )
        cout << " " << c;
      cout << " )" << endl;
    }
  }
  tuple<void*,void*,vector<tuple<void*,int>*>*>* p = plan;
  exit_points = new vector<int>();
  for ( Opr* x: operations )
    initialize_opr(x);
  for ( auto x: *get<2>(*p) )
    exit_points->push_back(get<1>(*x));
  vector<int>* entries = entry_points(exit_points);
  for ( int x: *entries )
    ready_queue.push(x);
  while (!ready_queue.empty()) {
    int opr_id = ready_queue.top();
    ready_queue.pop();
    compute(opr_id);
    enqueue_ready_operations(opr_id);
    if (operations[opr_id]->type != pairOPR)
      check_caches(opr_id);
  }
  vector<tuple<void*,void*>*>* res = new vector<tuple<void*,void*>*>();
  for ( tuple<void*,int>* d: *get<2>(*p) )
    res->push_back(new tuple(get<0>(*d),operations[get<1>(*d)]->cached));
  return new tuple(get<0>(*p),get<1>(*p),res);
}
