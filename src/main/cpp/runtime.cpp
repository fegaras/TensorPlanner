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

#include <queue>
#include <deque>
#include <cassert>
#include <sstream>
#include <chrono>
#include <algorithm>
#include <stdarg.h>
#include <unistd.h>
#include "tensor.h"
#include "opr.h"
#include "comm.h"

// trace execution
const bool trace = true;
// trace block deletes for debugging
static bool trace_delete = false;
// enables deletion of garbage blocks
bool delete_arrays = true;
const bool enable_partial_reduce = true;
// evaluate the plan without MPI using 1 executor
bool inMemory = false;

vector<Opr*> operations;
vector<void*(*)(void*)> functions;

static map<size_t,int> task_table;
static vector<void*> loadBlocks;
static vector<int>* exit_points;
extern int executor_rank;
extern int num_of_executors;

static int block_count = 0;
static int block_created = 0;
static int max_blocks = 0;


// used for comparing task priorities in the run-time queue
class task_cmp {
  int priority ( const int opr_id ) const {
    Opr* opr = operations[opr_id];
    if (opr->type == applyOPR) return 9-opr->cpu_cost;
    else if (opr->type == reduceOPR) return 10;
    else if (opr->type == pairOPR) return 8;
    else return 1;
  }
public:
  task_cmp () {}
  bool operator() ( const int &lhs, const int &rhs ) const {
    return priority(lhs) < priority(rhs);
  }
};

// run-time task queue
static priority_queue<int,vector<int>,task_cmp> ready_queue;
mutex ready_mutex;

// tasks waiting to send data (dest_id,src_id,tag)
static deque<tuple<int,int,int>*> send_queue;
mutex send_mutex;

mutex info_mutex;
// print tracing info
void info ( const char *fmt, ... ) {
  using namespace std::chrono;
  static char* cbuffer = new char[20000];
  if (trace) {
    lock_guard<mutex> lock(info_mutex);    
    auto millis = duration_cast<milliseconds>(system_clock::now()
                                  .time_since_epoch()).count() % 1000;
    time_t now = time(0);
    tm* t = localtime(&now);
    va_list args;
    va_start(args,fmt);
    cbuffer[0] = 0;
    vsprintf(cbuffer,fmt,args);
    va_end(args);
    printf("[%02d:%02d:%02d:%03d%3d]   %s\n",t->tm_hour,t->tm_min,
           t->tm_sec,millis,executor_rank,cbuffer);
  }
}

// print a vector
string sprint ( vector<int> v ) {
  ostringstream out;
  out << "(";
  auto it = v.begin();
  if (v.size() > 0) {
    out << *(it++);
    for ( ; it != v.end(); it++ )
      out << "," << *it;
  }
  out << ")";
  return out.str();
}

// print the task name, producers, and consumers
string sprint ( Opr* opr ) {
  return string(oprNames[opr->type])
    .append(sprint(*opr->children))
    .append("->")
    .append(sprint(*opr->consumers));
}

// print the task data
int print_block ( ostringstream &out, const void* data,
                  vector<int>* encoded_type, int loc ) {
  switch ((*encoded_type)[loc]) {
  case 0: case 1: // index
    out << (long)data;
    return loc+1;
  case 10: { // tuple
    switch ((*encoded_type)[loc+1]) {
      case 0:
        out << "()";
        return loc+2;
      case 2: {
        out << "(";
        tuple<void*,void*>* x = data;
        int l2 = print_block(out,get<0>(*x),encoded_type,loc+2);
        out << ",";
        int l3 = print_block(out,get<1>(*x),encoded_type,l2);
        out << ")";
        return l3;
      }
      case 3: {
        tuple<void*,void*,void*>* x = data;
        out << "(";
        int l2 = print_block(out,get<0>(*x),encoded_type,loc+2);
        out << ",";
        int l3 = print_block(out,get<1>(*x),encoded_type,l2);
        out << ",";
        int l4 = print_block(out,get<2>(*x),encoded_type,l3);
        out << ")";
        return l4;
      }
    }
  }
  case 11: { // data block
    switch ((*encoded_type)[loc+1]) {
      case 0: {
        Vec<int>* x = data;
        out << "Vec<int>(" << x->size() << ")";
      }
      case 1: {
        Vec<long>* x = data;
        out << "Vec<long>(" << x->size() << ")";
      }
      case 3: {
        Vec<double>* x = data;
        out << "Vec<double>(" << x->size() << ")";
      }
    }
    return loc+2;
  }
  default:
    return loc+1;
  }
}

string print_block ( Opr* opr ) {
  if (!trace)
    return string("");
  ostringstream out;
  print_block(out,opr->cached,opr->encoded_type,0);
  return out.str();
}

vector<int>* range ( long n1, long n2, long n3 ) {
  vector<int>* a = new vector<int>();
  for ( int i = n1; i <= n2; i += n3 )
    a->push_back(i);
  return a;
}

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

int pairOpr ( int x, int y, void* coord, vector<int>* encoded_type ) {
  Opr* o = new Opr();
  o->type = pairOPR;
  o->opr.pair_opr = new PairOpr(x,y);
  return store_opr(o,new vector<int>({ x, y }),coord,0,encoded_type);
}

int applyOpr ( int x, int fnc, void* args, void* coord, int cost, vector<int>* encoded_type ) {
  Opr* o = new Opr();
  o->type = applyOPR;
  o->opr.apply_opr = new ApplyOpr(x,fnc,args);
  return store_opr(o,new vector<int>({ x }),coord,cost,encoded_type);
}

int reduceOpr ( const vector<long>* s, bool valuep, int op,
                void* coord, int cost, vector<int>* encoded_type ) {
  if (s->size() == 1)
    return (*s)[0];
  Opr* o = new Opr();
  o->type = reduceOPR;
  auto ss = new vector<int>();
  for ( long x: *s )
    ss->push_back(x);
  o->opr.reduce_opr = new ReduceOpr(ss,valuep,op);
  return store_opr(o,ss,coord,cost,encoded_type);
}

// apply f to every task in the workflow starting with s using breadth-first search 
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

bool member ( int n, vector<int>* v ) {
  return find(v->begin(),v->end(),n) != v->end();
}

int delete_array ( void* &data, vector<int>* encoded_type, int loc ) {
  if (!delete_arrays)
    return 0;
  switch ((*encoded_type)[loc]) {
  case 0: case 1: // index
    return loc+1;
  case 10: { // tuple
    switch ((*encoded_type)[loc+1]) {
      case 0: case 1:
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
  case 11:  // data block
    switch ((*encoded_type)[loc+1]) {
    case 0: {
      Vec<int>* x = data;
      delete x;
      data = nullptr;
      return loc+2;
    }
    case 1: {
      Vec<long>* x = data;
      delete x;
      data = nullptr;
      return loc+2;
    }
    case 3: {
      Vec<double>* x = data;
      delete x;
      data = nullptr;
      return loc+2;
    }
    }
  default:
    return loc+1;
  }
}

int delete_array ( void* &data, vector<int>* encoded_type ) {
  try {
    delete_array(data,encoded_type,0);
  } catch (...) {
    info("cannot delete block %d",data);
  }
}


// delete the cache of a task
void delete_array ( int opr_id ) {
  if (!delete_arrays)
    return;
  Opr* opr = operations[opr_id];
  if (trace_delete)
    cout << executor_rank << " " << "delete array " << opr_id << " "
         << opr->status << " " << opr->cached << " " << opr->count << endl;
  if (opr->cached != nullptr) {
    delete_array(opr->cached,opr->encoded_type);
    opr->cached = nullptr;
    info("    delete block %d (%d/%d)",opr_id,block_count,block_created);
  }
}

// does this task create a new block?
bool block_constructor ( Opr* opr ) {
  // true for a reduce or apply task that create a block
  // and when receiving a remote block
  return opr->cpu_cost > 0 || opr->node != executor_rank;
}

bool sends_msgs_only ( Opr* opr ) {
  bool sends_msgs = true;
  for ( int c: *opr->consumers )
    sends_msgs = sends_msgs && operations[c]->node != opr->node;
  return sends_msgs;
}

void may_delete_deps ( int opr_id );

// delete the task cache and the caches of its dependencies
void may_delete ( int opr_id ) {
  Opr* opr = operations[opr_id];
  if (block_constructor(opr)) {// && opr->count > 0) {
    if (trace_delete)
      cout << executor_rank << " " << "may delete " << opr_id << endl;
    opr->count--;
    if (trace_delete)
      cout << executor_rank << " " << "decrement count " << opr_id
           << " " << opr->count << endl;
    if (opr->count <= 0 && opr->cached != nullptr)
      delete_array(opr_id);
  }
}

// delete the caches of the task dependencies
void may_delete_deps ( int opr_id ) {
  Opr* opr = operations[opr_id];
  if (opr->cpu_cost > 0) {
    if (trace_delete)
      cout << executor_rank << " " << "may delete deps " << opr_id << endl;
    for ( int c: *opr->dependents )
      may_delete(c);
  }
}

vector<int>* get_dependents ( Opr* opr ) {
  if (opr->dependents != nullptr)
    return opr->dependents;
  if (opr->node != executor_rank)
    return empty_vector;
  vector<int>* ds = new vector<int>();
  for ( int c: *opr->children ) {
    Opr* copr = operations[c];
    if (block_constructor(copr) && copr->type != loadOPR)
      ds->push_back(c);
    else {  // c has 0 cost and is local
      copr->dependents = get_dependents(copr);
      append(ds,copr->dependents);
    }
  }
  if (ds->size() == 0) {
    delete ds;
    return empty_vector;
  } else return ds;
}

void initialize_opr ( Opr* x ) {
  x->status = notReady;
  x->cached = nullptr;
  // a Load opr is cached in the coordinator too
  if (isCoordinator() && x->type == loadOPR)
    cache_data(x,loadBlocks[x->opr.load_opr->block]);
  // initial count is the number of local consumers
  x->count = 0;
  x->message_count = 0;
  x->dependents = get_dependents(x);
  if (x->type == reduceOPR) {
    // used for partial reduction
    x->reduced_count = 0;
    for ( int c: *x->opr.reduce_opr->s )
      if (operations[c]->node == executor_rank)
        x->reduced_count++;
    if (x->node == executor_rank) {
      vector<int> s;
      for ( int c: *x->opr.reduce_opr->s ) {
        int cnode = operations[c]->node;
        if (cnode != executor_rank
            && find(s.begin(),s.end(),cnode) == s.end())
          s.push_back(cnode);
      }
      x->reduced_count += s.size();
    }
  }
}

void schedule_plan ( void* plan );

void schedule ( void* plan ) {
  auto time = MPI_Wtime();
  tuple<void*,void*,vector<tuple<void*,int>*>*>* p = plan;
  schedule_plan(plan);
  for ( Opr* x: operations )
    x->dependents = nullptr;
  for ( Opr* x: operations )
    initialize_opr(x);
  for ( Opr* x: operations )
    if (x->node == executor_rank)
      if (x->cpu_cost > 0 || sends_msgs_only(x))
        for ( int c: *x->dependents ) {
          Opr* copr = operations[c];
          copr->count++;
        }
  if (trace && isCoordinator()) {
    cout << "Plan:" << endl;
    for ( int i = 0; i < operations.size(); i++ ) {
      Opr* opr = operations[i];
      if (trace_delete)
        printf("%d    %d:%s at %d cost %d    %d %s\n",
               executor_rank,i,sprint(opr).c_str(),opr->node,opr->cpu_cost,
               opr->count,sprint(*opr->dependents).c_str());
      else printf("    %d:%s at %d cost %d\n",
                  i,sprint(opr).c_str(),opr->node,opr->cpu_cost);
    }
    printf("Scheduling time: %.3f secs\n",MPI_Wtime()-time);
  } else if (trace && trace_delete) {
    cout << "Plan:" << endl;
    for ( int i = 0; i < operations.size(); i++ ) {
      Opr* opr = operations[i];
      printf("%d    %d:%s at %d cost %d    %d %s\n",
             executor_rank,i,sprint(opr).c_str(),opr->node,opr->cpu_cost,
             opr->count,sprint(*opr->dependents).c_str());
    }
  }
  if (!inMemory)
    barrier();
}

void* inMemEval ( int id, int tabs );

void* inMemEvalPair ( int lg, int tabs ) {
  switch (operations[lg]->type) {
    case pairOPR: {
      int x = operations[lg]->opr.pair_opr->x;
      int y = operations[lg]->opr.pair_opr->y;
      void* gx = inMemEvalPair(x,tabs);
      void* gy = inMemEvalPair(y,tabs);
      tuple<void*,void*>* gty = gy;
      void* yy = (operations[y]->type == pairOPR) ? gy : get<1>(*gty);
      tuple<void*,void*>* gtx = gx;
      void* xx = (operations[x]->type == pairOPR) ? gx : get<1>(*gtx);
      return new tuple<void*,void*>(get<0>(*gtx),new tuple<void*,void*>(xx,yy));
    }
    default: return inMemEval(lg,tabs+1);
  }
}

// top-down recursive evaluation
void* inMemEval ( int id, int tabs ) {
  Opr* e = operations[id];
  if (hasCachedValue(e))
    return e->cached;
  info("*** %d: %d %s %p",tabs,id,oprNames[e->type],e);
  void* res = nullptr;
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
      vector<tuple<void*,void*>*>* sv = new vector<tuple<void*,void*>*>();
      for ( int x: *rs ) {
        tuple<void*,void*>* v = inMemEval(x,tabs+1);
        sv->push_back(v);
      }
      void*(*op)(tuple<void*,void*>*) = functions[e->opr.reduce_opr->op];
      void* av = nullptr;
      for ( tuple<void*,void*>* v: *sv ) {
        if (av == nullptr)
          av = get<1>(*v);
        else av = op(new tuple<void*,void*>(av,get<1>(*v)));
      }
      res = new tuple<void*,void*>(get<0>(*(*sv)[0]),av);
      break;
    }
  }
  info("*-> %d: %s %p",tabs,oprNames[e->type],res);
  e->cached = res;
  e->status = completed;
  return res;
}

 // in-memory evaluation using top-down recursive evaluation on a plan tree
void* evalTopDown ( void* plan ) {
  tuple<void*,void*,vector<tuple<void*,int>*>*>* p = plan;
  for ( Opr* x: operations )
    x->count = x->consumers->size();
  vector<tuple<void*,void*>*>* res = new vector<tuple<void*,void*>*>();
  for ( tuple<void*,int>* d: *get<2>(*p) ) {
    tuple<void*,void*>* v = inMemEval(get<1>(*d),0);
    res->push_back(v);
  }
  return new tuple<void*,void*,void*>(get<0>(*p),get<1>(*p),res);
}

void* compute ( int lg_id );

void* computePair ( int lg_id ) {
  Opr* lg = operations[lg_id];
  if (hasCachedValue(lg)) {
    assert(lg->cached != nullptr);
    return lg->cached;
  }
  switch (lg->type) {
    case pairOPR: {
      int x = lg->opr.pair_opr->x;
      int y = lg->opr.pair_opr->y;
      void* gx = computePair(x);
      void* gy = computePair(y);
      tuple<void*,void*>* gty = gy;
      void* yy = (operations[y]->type == pairOPR) ? gy : get<1>(*gty);
      tuple<void*,void*>* gtx = gx;
      void* xx = (operations[x]->type == pairOPR) ? gx : get<1>(*gtx);
      void* data = new tuple<void*,void*>(get<0>(*gtx),new tuple<void*,void*>(xx,yy));
      cache_data(lg,data);
      lg->status = completed;
      return data;
    }
    default:
      assert(lg->cached != nullptr);
      return lg->cached;
  }
}

void* compute ( int opr_id ) {
  Opr* opr = operations[opr_id];
  info("*** computing %d:%s%s",opr_id,
       oprNames[opr->type],sprint(*opr->children).c_str());
  void* res = nullptr;
try {
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
      void* acc = nullptr;
      void* key = 0;
      int i = 0;
      for ( void* x: *rs ) {
        tuple<void*,void*>* v = operations[x]->cached;
        if (acc == nullptr) {
          key = get<0>(*v);
          acc = get<1>(*v);
        } else {
          res = new tuple<void*,void*>(key,acc);
          acc = op(new tuple<void*,void*>(acc,get<1>(*v)));
          if (i > 1)
            delete_array(res,opr->encoded_type);
          delete res;
        }
        i++;
      }
      opr->status = completed;
      res = new tuple<void*,void*>(key,acc);
      cache_data(opr,res);
      break;
    }
  }
  info("*-> result of opr %d:%s  %s",opr_id,
       oprNames[opr->type],print_block(opr).c_str());
} catch ( const exception &ex ) {
  info("*** fatal error: computing the operation %d",opr_id);
  abort();
}
  return res;
}

void enqueue_ready_operations ( int opr_id );

// delete the first local reduce input
void delete_first_reduce_input ( int rid ) {
  Opr* reduce_opr = operations[rid];
  if (delete_arrays && reduce_opr->first_reduced_input >= 0
      && reduce_opr->children->size() > 1) {
    info("    delete first reduce input %d of %d",
         reduce_opr->first_reduced_input,rid);
    may_delete(reduce_opr->first_reduced_input);
    reduce_opr->first_reduced_input = -1;
  }
}

void enqueue_reduce_opr ( int opr_id, int rid ) {
  Opr* opr = operations[opr_id];      // child of reduce_opr
  Opr* reduce_opr = operations[rid];  // Reduce operation
  // partial reduce inside reduce opr
  void*(*op)(tuple<void*,void*>*) = functions[reduce_opr->opr.reduce_opr->op];
  if (reduce_opr->cached == nullptr) {
    // first reduction (may delete later)
    info("    set reduce opr %d to the first input opr %d",rid,opr_id);
    reduce_opr->cached = opr->cached;
    reduce_opr->first_reduced_input = opr_id;
  } else if (reduce_opr->opr.reduce_opr->valuep)
    // total aggregation
    reduce_opr->cached = op(new tuple<void*,void*>(reduce_opr->cached,opr->cached));
  else {
    tuple<void*,void*>* x = reduce_opr->cached;
    tuple<void*,void*>* y = opr->cached;
    // merge the current state with the partially reduced data
    info("    merge reduce opr %d with input opr %d",rid,opr_id);
    auto old = (void*)x;
    reduce_opr->cached = new tuple<void*,void*>(get<0>(*x),op(new tuple<void*,void*>(get<1>(*x),get<1>(*y))));
    // remove the old reduce result
    if (delete_arrays && reduce_opr->first_reduced_input < 0) {
      info("    delete current reduce result in opr %d",rid);
      delete_array(old,reduce_opr->encoded_type);
    }
    // when reduce_opr finished with input opr => check if we can delete opr
    delete_first_reduce_input(rid);
    may_delete(opr_id);
  }
  reduce_opr->reduced_count--;
  if (reduce_opr->reduced_count <= 0) {
    if (reduce_opr->node == executor_rank) {
      // completed final reduce
      info("    completed reduce opr %d",rid);
      reduce_opr->status = computed;
      enqueue_ready_operations(rid);
    } else {
      // completed local reduce => send it to the reduce owner
      info("    sending partial reduce result of opr %d to %d",
           rid,reduce_opr->node);
      send_data(reduce_opr->node,reduce_opr->cached,rid,1);
      delete_array(rid);
    }
    delete_first_reduce_input(rid);
  }
}

// After opr is computed, check its consumers to see if anyone is ready to enqueue
void enqueue_ready_operations ( int opr_id ) {
  Opr* opr = operations[opr_id];
  if (opr->node == executor_rank) {
    // send the opr cached result to the non-local consumers
    vector<int> nodes_to_send_data;
    for ( int c: *opr->consumers ) {
      Opr* copr = operations[c];
      if (copr->node != executor_rank
          && (!enable_partial_reduce
              || copr->type != reduceOPR)
          && !member(copr->node,&nodes_to_send_data))
        nodes_to_send_data.push_back(copr->node);
    }
    { lock_guard<mutex> lock(send_mutex);
      for ( int c: nodes_to_send_data ) {
        // lock the task until all messages have been sent
        opr->message_count++;
        send_queue.push_back(new tuple<int,int,int>(c,opr_id,0));
      }
    }
  }
  for ( int c: *opr->consumers ) {
    Opr* copr = operations[c];
    if (copr->node >= 0) {
      switch (copr->type) {  // partial reduce
      case reduceOPR:
        if (enable_partial_reduce) {
          enqueue_reduce_opr(opr_id,c);
          break;
        }
      default:
        // enqueue the local consumers that are ready
        if (copr->node == executor_rank
            && !hasCachedValue(copr)) {
          bool cv = true;
          for ( int c: *copr->children )
            cv = cv && hasCachedValue(operations[c]);
          lock_guard<mutex> lock(ready_mutex);
          if (cv && copr->status == notReady) {
            copr->status = ready;
            ready_queue.push(c);
          }
        }
      }
    }
  }
  while (opr->message_count > 0)
    this_thread::sleep_for(chrono::milliseconds(1));
  // delete the completed dependents
  may_delete_deps(opr_id);
  opr->status = completed;
}

// exit when the queues of all executors are empty and exit points have been computed
bool exit_poll () {
  bool b = ready_queue.empty() && send_queue.empty();
  for ( int x: *exit_points )
    b = b && (operations[x]->node != executor_rank
              || hasCachedValue(operations[x]));
  return wait_all(b);
}

bool stop_sender = false;
extern bool stop_receiver;

bool in_send_queue ( int opr_id ) {
  lock_guard<mutex> lock(send_mutex);
  for ( tuple<int,int,int>* x: send_queue )
    if (get<1>(*x) == opr_id)
      return true;
  return false;
}

void run_sender () {
  while (!stop_sender) {
    if (!send_queue.empty()) {
     tuple<int,int,int>* x;
      {
        lock_guard<mutex> lock(send_mutex);
        x = send_queue.back();
        send_queue.pop_back();
      }
      int opr_id = get<1>(*x);
      Opr* opr = operations[opr_id];
      send_data(get<0>(*x),opr->cached,opr_id,0);
      opr->message_count--;
      if (trace_delete)
        cout << "finished sent " << opr_id << " " << opr->message_count
             << " " << sends_msgs_only(opr) << endl;
      if (opr->message_count == 0 && sends_msgs_only(opr)) {
        // when all sends from opr have been completed and opr is a sink (sends data to remote only)
        if (opr->cpu_cost > 0)
          may_delete(opr_id);
        else for ( int c: *opr->dependents )
               may_delete(c);
      }
    } else this_thread::sleep_for(chrono::milliseconds(1));
  }
}

void work () {
  bool exit = false;
  int count = 0;
  float t = MPI_Wtime();
  while (!exit) {
    count = (count+1)%100;
    if (count == 0)
      exit = exit_poll();
    if (!ready_queue.empty()) {
      int opr_id;
      { lock_guard<mutex> lock(ready_mutex);
        opr_id = ready_queue.top();
        ready_queue.pop();
      };
      compute(opr_id);
      enqueue_ready_operations(opr_id);
      t = MPI_Wtime();
    } else if (MPI_Wtime()-t > 10.0) {
      info("... starving");
      vector<int> v;
      for ( int x: *exit_points )
        if (operations[x]->node == executor_rank
            && !hasCachedValue(operations[x]))
          v.push_back(x);
      info("... missing: %s",sprint(v).c_str());
      t = MPI_Wtime();
    }
  }
  info("Executor %d has finished execution",executor_rank);
}

vector<int>* entry_points ( const vector<int>* es ) {
  static vector<int>* b = new vector<int>();
  BFS(es,
      [] ( int c ) {
        Opr* copr = operations[c];
        if (copr->type == loadOPR && copr->node == executor_rank)
          if (!member(c,b))
            b->push_back(c);
      });
  return b;
}

void* eval ( void* plan ) {
  auto time = MPI_Wtime();
  tuple<void*,void*,vector<tuple<void*,int>*>*>* p = plan;
  exit_points = new vector<int>();
  for ( auto x: *get<2>(*p) )
    exit_points->push_back(get<1>(*x));
  if (trace && isCoordinator())
    printf("Exit points: %s\n",sprint(*exit_points).c_str());
  vector<int>* entries = entry_points(exit_points);
  for ( int x: *entries )
    ready_queue.push(x);
  info("Queue on %d: %s",executor_rank,sprint(*entries).c_str());
  if (!inMemory) {
    barrier();
    thread rrp(run_receiver);
    thread rsp(run_sender);
    work();
    barrier();
    stop_receiver = true;
    stop_sender = true;
    kill_receiver();
    rrp.join();
    rsp.join();
  } else work();
  vector<tuple<void*,void*>*>* res = new vector<tuple<void*,void*>*>();
  for ( tuple<void*,int>* d: *get<2>(*p) )
    res->push_back(new tuple<void*,void*>(get<0>(*d),operations[get<1>(*d)]->cached));
  if (isCoordinator())
    printf("*** Evaluation time: %.3f secs\n",MPI_Wtime()-time);
  printf("*** Executor %d:  final blocks: %d,  max blocks: %d,  created blocks: %d\n",
         executor_rank,block_count,max_blocks,block_created);
  vector<int> unclaimed;
  for ( int i = 0; i < operations.size(); i++ ) {
    Opr* opr = operations[i];
    if (opr->cached != nullptr && opr->type != loadOPR
        && ((opr->type == applyOPR && opr->cpu_cost > 0)
            || opr->type == reduceOPR
            || opr->node != executor_rank)
        && !member(i,exit_points))
      unclaimed.push_back(i);
  }
  info("There are %d unclaimed blocks on executor %d: %s",
       unclaimed.size(),executor_rank,sprint(unclaimed).c_str());
  if (trace_delete) // unclaimed blocks
    for ( int i: unclaimed ) {
      Opr* opr = operations[i];
      cout << "Unclaimed block at " << executor_rank << ": " << i << ":"
           << sprint(opr) << " " << opr->node << " " << opr->count << endl;
    }
  return new tuple<void*,void*,void*>(get<0>(*p),get<1>(*p),res);
}

void* evalOpr ( int opr_id ) {
  auto plan = new tuple<int,nullptr_t,vector<tuple<int,int>*>*>(1,nullptr,
                          new vector<tuple<int,int>*>({new tuple<int,int>(0,opr_id)}));
  schedule(plan);
  eval(plan);
  Opr* opr = operations[opr_id];
  opr->status = computed;
  tuple<void*,void*>* res = opr->cached;
  return get<1>(*res);
}

void* collect ( void* plan ) {
  tuple<void*,void*,vector<tuple<void*,int>*>*>* p = plan;
  vector<tuple<void*,int>*>* es = get<2>(*p);
  if (inMemory) {
    vector<void*>* blocks = new vector<void*>();
    for ( auto x: *es )
      blocks->push_back(operations[get<1>(*x)]->cached);
    return new tuple<void*,void*,void*>(get<0>(*p),get<1>(*p),blocks);
  }
  if (isCoordinator()) {
    info("Collecting the task blocks at the coordinator");
    stop_receiver = false;
    thread rrp(run_receiver);
    bool b = false;
    while (!b) {
      this_thread::sleep_for(chrono::milliseconds(100));
      b = true;
      for ( auto x: *es )
        b = b && hasCachedValue(operations[get<1>(*x)]);
    }
    vector<void*>* blocks = new vector<void*>();
    for ( auto x: *es )
      blocks->push_back(operations[get<1>(*x)]->cached);
    stop_receiver = true;
    kill_receiver();
    rrp.join();
    barrier();
    return new tuple<void*,void*,void*>(get<0>(*p),get<1>(*p),blocks);
  } else {
    for ( auto x: *es ) {
      Opr* opr = operations[get<1>(*x)];
      if (opr->node == executor_rank)
        send_data(getCoordinator(),opr->cached,get<1>(*x),2);
    }
    barrier();
    return new tuple<void*,void*,void*>(get<0>(*p),get<1>(*p),nullptr);
  }
}
