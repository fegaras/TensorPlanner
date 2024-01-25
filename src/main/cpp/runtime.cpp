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

#define GC_THREADS
#include <gc.h>
#include <vector>
#include <tuple>
#include <queue>
#include <cassert>
#include <sstream>
#include <chrono>
#include <algorithm>
#include <thread>
#include <mutex>
#include <stdarg.h>
#include <unistd.h>
#include "mpi.h"
#include "omp.h"
#include "tensor.h"
#include "opr.h"
#include "comm.h"

const bool trace = true;
const bool enable_partial_reduce = true;
bool enable_gc = true;

vector<Opr*> operations;
vector<void*(*)(void*)> functions;

static map<size_t,int> task_table;
static vector<void*> loadBlocks;
static vector<int>* exit_points;

extern int executor_rank;
extern int num_of_executors;

static int block_count = 0;
static int blocks_created = 0;
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

// tasks waiting to send data
static vector<tuple<int,int>*> send_queue;
mutex send_mutex;

// tracing info
void info ( const char *fmt, ... ) {
  using namespace std::chrono;
  static char* cbuffer = new char[20000];
  if (trace) {
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
  auto it = v.cbegin();
  if (v.size() > 0) {
    out << *(it++);
    for ( ; it != v.cend(); it++ )
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
  opr->encoded_type->shrink_to_fit();
  size_t key = opr->hash();
  auto p = task_table.find(key);
  if (p != task_table.end())
    if (operations[p->second] == opr)
      return p->second;
  int loc = operations.size();
  operations.push_back(opr);
  task_table.emplace(pair<size_t,int>(key,loc));
  opr->children = children;
  opr->children->shrink_to_fit();
  children->shrink_to_fit();
  for ( int c: *children ) {
    auto cs = operations[c]->consumers;
    if (find(cs->cbegin(),cs->cend(),loc) == cs->cend())
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

bool member ( int n, const vector<int>* v ) {
  return find(v->cbegin(),v->cend(),n) != v->cend();
}

// garbage-collect the opr block
void gc ( int opr_id ) {
  Opr* opr = operations[opr_id];
  if (enable_gc && opr->cached != nullptr && opr->type != loadOPR) {
    if (opr->status == locked)
      opr->status = zombie;
    else {
      info("    discard the block of %d (%d/%d)",
           opr_id,block_count,blocks_created);
      opr->status = removed;
      // make the cached block available to the garbage collector
      opr->cached = nullptr;
    }
  }
}

// after opr_id is finished, check if we can release the cache of its children
void check_caches ( int opr_id ) {
  Opr* e = operations[opr_id];
  for ( int c: *e->children ) {
    Opr* copr = operations[c];
    copr->count--;
    if (copr->count <= 0)
      gc(c);
  }
}

void initialize_opr ( Opr* x ) {
  x->status = notReady;
  x->cached = nullptr;
  // a Load opr is cached in the coordinator too
  if (isCoordinator() && x->type == loadOPR)
    cache_data(x,loadBlocks[x->opr.load_opr->block]);
  // initial count is the number of local consumers
  x->count = 0;
  for ( int c: *x->consumers )
    if (operations[c]->node == executor_rank)
      x->count++;
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
            && find(s.cbegin(),s.cend(),cnode) == s.cend())
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
    initialize_opr(x);
  if (trace && isCoordinator()) {
    cout << "Plan:" << endl;
    for ( int i = 0; i < operations.size(); i++ ) {
      Opr* opr = operations[i];
      printf("    %d:%s at %d cost %d\n",
             i,sprint(opr).c_str(),opr->node,opr->cpu_cost);
    }
    printf("Scheduling time: %.3f secs\n",MPI_Wtime()-time);
  }
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
      lg->status = computed;
      return data;
    }
    default:
      return lg->cached;
  }
}

void* compute ( int opr_id ) {
  Opr* opr = operations[opr_id];
  info("*** computing %d:%s",opr_id,oprNames[opr->type]);
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
      for ( void* x: *rs ) {
        tuple<void*,void*>* v = operations[x]->cached;
        if (acc == nullptr) {
          key = get<0>(*v);
          acc = get<1>(*v);
        } else acc = op(new tuple<void*,void*>(get<1>(*v),acc));
      }
      opr->status = computed;
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

void delete_reduce_input ( int opr_id ) {
  Opr* opr = operations[opr_id];
  opr->count--;
  if (opr->count <= 0)
    gc(opr_id);
}

void enqueue_reduce_opr ( int opr_id, int rid ) {
  Opr* opr = operations[opr_id];      // child of reduce_opr
  Opr* reduce_opr = operations[rid];  // Reduce operation
  // partial reduce inside reduce opr
  void*(*op)(tuple<void*,void*>*) = functions[reduce_opr->opr.reduce_opr->op];
  if (reduce_opr->cached == nullptr) {
    // first reduction
    reduce_opr->cached = opr->cached;
    delete_reduce_input(opr_id);
  } else if (reduce_opr->opr.reduce_opr->valuep)
    // total aggregation
    reduce_opr->cached = op(new tuple<void*,void*>(reduce_opr->cached,opr->cached));
  else {
    tuple<void*,void*>* x = reduce_opr->cached;
    tuple<void*,void*>* y = opr->cached;
    // merge the current state with the partially reduced data
    reduce_opr->cached = new tuple<void*,void*>(get<0>(*x),
                                 op(new tuple<void*,void*>(get<1>(*x),get<1>(*y))));
    delete_reduce_input(opr_id);
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
      gc(rid);
    }
    // copr is done with opr => check if we can GC opr
    opr->count--;
    if (opr->count <= 0)
      gc(opr_id);
  }
}

// After opr is computed, check its consumers to see if anyone is ready to enqueue
void enqueue_ready_operations ( int opr_id ) {
  Opr* opr = operations[opr_id];
  if (opr->node == executor_rank) {
    // send the opr cached result to the non-local consumers (must be done first)
    vector<int> nodes_to_send_data;
    for ( int c: *opr->consumers ) {
      Opr* copr = operations[c];
      if (copr->node != executor_rank
          && copr->node >= 0
          && (!enable_partial_reduce
              || copr->type != reduceOPR)
          && !member(copr->node,&nodes_to_send_data))
        nodes_to_send_data.push_back(copr->node);
    }
    if (!nodes_to_send_data.empty()) {
      lock_guard<mutex> lock(send_mutex);
      for ( int c: nodes_to_send_data ) {
        opr->status = locked;
        send_queue.push_back(new tuple<int,int>(c,opr_id));
      }
    }
  }
  for ( int c: *opr->consumers ) {
    Opr* copr = operations[c];
    if (copr->node >= 0) {
      switch (copr->type) {  // partial reduce
      case reduceOPR:
        if (enable_partial_reduce)
          enqueue_reduce_opr(opr_id,c);
        break;
      default:
        // enqueue the local consumers that are ready
        if (copr->node == executor_rank
            && !hasCachedValue(copr)) {
          bool cv = true;
          for ( int c: *copr->children )
            cv = cv && hasCachedValue(operations[c]);
          if (cv && copr->status == notReady) {
            lock_guard<mutex> lock(ready_mutex);
            copr->status = ready;
            ready_queue.push(c);
          }
        }
      }
    }
  }
  // if there is no consumer on the same node, garbage collect the task cache
  if (opr->node == executor_rank && opr->consumers->size() > 0) {
    bool sends_msgs = true;
    for ( int c: *opr->consumers )
      sends_msgs = sends_msgs && operations[c]->node != executor_rank;
    if (sends_msgs && !member(opr_id,exit_points))
      gc(opr_id);
  }
  if (opr->status != locked && opr->status != zombie)
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
  for ( tuple<int,int>* x: send_queue )
    if (get<1>(*x) == opr_id)
      return true;
  return false;
}

int run_sender () {
  struct GC_stack_base sb;
  GC_get_stack_base(&sb);
  GC_register_my_thread(&sb);
  while (!stop_sender) {
    if (!send_queue.empty()) {
      tuple<int,int>* x;
      {
        lock_guard<mutex> lock(send_mutex);
        x = send_queue.back();
        send_queue.pop_back();
      }
      int opr_id = get<1>(*x);
      Opr* opr = operations[opr_id];
      send_data(get<0>(*x),opr->cached,opr_id,0);
      if (opr->status == zombie && !in_send_queue(opr_id)) {
        opr->status = computed;
        gc(opr_id);
      } else if (opr->status == locked)
        opr->status = computed;
    } else this_thread::sleep_for(chrono::milliseconds(1));
  }
  GC_unregister_my_thread();
  return 0;
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
      check_caches(opr_id);
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
  barrier();
  thread rrp(run_receiver);
  thread rsp(run_sender);
  work();
  barrier();
  stop_receiver = true;
  rrp.join();
  stop_sender = true;
  rsp.join();
  kill_receiver();
  vector<tuple<void*,void*>*>* res = new vector<tuple<void*,void*>*>();
  for ( tuple<void*,int>* d: *get<2>(*p) )
    res->push_back(new tuple<void*,void*>(get<0>(*d),operations[get<1>(*d)]->cached));
  if (isCoordinator()) {
    info("Evaluation time: %.3f secs",MPI_Wtime()-time);
  }
  info("Executor %d:  final blocks: %d,  max blocks: %d,  created blocks: %d",
       executor_rank,block_count,max_blocks,blocks_created);
  vector<int> unclaimed;
  for ( int i = 0; i < operations.size(); i++ ) {
    Opr* opr = operations[i];
    if (opr->cached != nullptr && opr->type != loadOPR
        && !member(i,exit_points))
      unclaimed.push_back(i);
  }
  info("There are %d unclaimed blocks by GC in executor %d: %s",
       unclaimed.size(),executor_rank,sprint(unclaimed).c_str());
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
  void* res = nullptr;
  if (isCoordinator()) {
    info("Collecting the task blocks at the coordinator");
    { stop_receiver = false;
      thread rrp(run_receiver);
      bool b = false;
      while (!b) {
        this_thread::sleep_for(chrono::milliseconds(10));
        b = true;
        for ( auto x: *es )
          b = b && hasCachedValue(operations[get<1>(*x)]);
      }
      stop_receiver = true;
      rrp.join();
    }
    barrier();
    vector<void*>* blocks = new vector<void*>();
    for ( auto x: *es )
      blocks->push_back(operations[get<1>(*x)]->cached);
    res = blocks;
  } else {
    for ( auto x: *es ) {
      Opr* opr = operations[get<1>(*x)];
      if (opr->node == executor_rank)
        send_data(getCoordinator(),opr->cached,get<1>(*x),2);
    }
    barrier();
  }
  return new tuple<void*,void*,void*>(get<0>(*p),get<1>(*p),res);
}
