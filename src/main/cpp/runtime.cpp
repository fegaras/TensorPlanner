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
#include <cstring>
#include <sstream>
#include <chrono>
#include <algorithm>
#include <stdarg.h>
#include <unistd.h>
#include <stdint.h>
#include "tensor.h"
#include "opr.h"
#include "comm.h"

// trace execution
bool trace = true;
// trace block deletes for debugging
bool trace_delete = false;
// enables deletion of garbage blocks
bool delete_arrays = true;
const bool enable_partial_reduce = true;
// cache replication of the Load tasks
const int replication = 2;
// evaluate the plan without MPI using 1 executor
bool inMemory = false;
// collect the resulting blocks of the workflow at the coordinator
bool enable_collect = false;
// enable fault-tolerance and recovery
bool enable_recovery = false;
// test recovery by killing the executor #1
bool test_recovery = false;
// number of steps before we abort executor #1 to test recovery
const int steps_before_abort = 10;
// suspend work during recovery
bool suspend_work = false;
// abort the MPI data receiver process
bool stop_receiver = false;
// abort the MPI data sender process
bool stop_sender = false;

// list of all tasks
vector<Opr*> operations;

// list of all Apply functions
vector<void*(*)(void*)> functions;

// list of cached Load blocks
vector<void*> loadBlocks;

// the exit points of the task workflow
vector<int>* exit_points;

// from MPI
extern int executor_rank;
extern int num_of_executors;

// statistics
int block_count = 0;
int block_created = 0;
int max_blocks = 0;

void delete_block ( void* &data, vector<int>* encoded_type );

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
priority_queue<int,vector<int>,task_cmp> ready_queue;
mutex ready_mutex;

// tasks waiting to send data; have entries: (dest_id,src_id,tag)
deque<tuple<int,int,int>*> send_queue;
mutex send_mutex;

// print an int vector to a string
string sprint ( vector<int> v ) {
  ostringstream out;
  out << "(";
  auto it = v.begin();
  int i = 0;
  if (v.size() > 0) {
    out << *(it++);
    for ( ; it != v.end() && i < 100; it++, i++ )
      out << "," << *it;
  }
  if (i >= 100)
    out << ",...";
  out << ")";
  return out.str();
}

// print the task name, producers, and consumers to a string
string sprint ( Opr* opr ) {
  return string(oprNames[opr->type])
    .append(sprint(*opr->children))
    .append("->")
    .append(sprint(*opr->consumers));
}

string print_ready_queue () {
  vector<int> vrq;
  priority_queue<int,vector<int>,task_cmp> rq(ready_queue);
  while (!rq.empty()) {
    vrq.push_back(rq.top());
    rq.pop();
  }
  return sprint(vrq);
}

// print tracing info
void info ( const char* fmt, ... ) {
  using namespace std::chrono;
  static mutex info_mutex;
  if (trace) {
    lock_guard<mutex> lock(info_mutex);    
    auto millis = duration_cast<milliseconds>(system_clock::now()
                                  .time_since_epoch()).count() % 1000;
    time_t now = time(0);
    tm* t = localtime(&now);
    va_list args;
    va_start(args,fmt);
    printf("[%02d:%02d:%02d:%03d%3d]   ",
           t->tm_hour,t->tm_min,t->tm_sec,(int)millis,executor_rank);
    vprintf(fmt,args);
    printf("\n");
    va_end(args);
  }
}

// print the task block data to a string
int print_block ( ostringstream &out, const void* data,
                  vector<int>* encoded_type, int loc ) {
  switch ((*encoded_type)[loc]) {
  case 0: case 1:   // an int index
    out << (uintptr_t)data;
    return loc+1;
  case 10: {   // a tuple
    switch ((*encoded_type)[loc+1]) {
      case 0:
        out << "()";
        return loc+2;
      case 2: {
        out << "(";
        auto x = (tuple<void*,void*>*)data;
        int l2 = print_block(out,get<0>(*x),encoded_type,loc+2);
        out << ",";
        int l3 = print_block(out,get<1>(*x),encoded_type,l2);
        out << ")";
        return l3;
      }
      case 3: {
        auto x = (tuple<void*,void*,void*>*)data;
        out << "(";
        int l2 = print_block(out,get<0>(*x),encoded_type,loc+2);
        out << ",";
        int l3 = print_block(out,get<1>(*x),encoded_type,l2);
        out << ",";
        int l4 = print_block(out,get<2>(*x),encoded_type,l3);
        out << ")";
        return l4;
      }
      default:
        info("Unknown tuple: %d",(*encoded_type)[loc+1]);
    }
  }
  case 11: { // a data block
    switch ((*encoded_type)[loc+1]) {
      case 0: {
        auto x = (Vec<int>*)data;
        out << "Vec<int>(" << x->size() << ")";
        return loc+2;
      }
      case 1: {
        auto x = (Vec<long>*)data;
        out << "Vec<long>(" << x->size() << ")";
        return loc+2;
      }
      case 3: {
        auto x = (Vec<double>*)data;
        out << "Vec<double>(" << x->size() << ")";
        return loc+2;
      }
      default:
        info("Unknown Vec: %d",(*encoded_type)[loc+1]);
    }
  }
  default:
    return loc+1;
  }
}

// print the task block data to a string
string print_block ( Opr* opr ) {
  if (!trace)
    return string("");
  try {
    ostringstream out;
    print_block(out,opr->cached,opr->encoded_type,0);
    return out.str();
  } catch (...) {
    return "<wrong cache type>";
  }
}

// true if n is a member of vector v
inline bool member ( int n, vector<int>* v ) {
  return find(v->begin(),v->end(),n) != v->end();
}

// process and store a task
int store_opr ( Opr* opr, const vector<int>* children,
                void* coord, int cost, vector<int>* encoded_type ) {
  static map<size_t,int> task_table;
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
  opr->children = (vector<int>*)children;
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
  // the loaded data is used by a num of executors = replication
  //   (the other executors delete the block)
  o->node = loadBlocks.size() % num_of_executors;
  o->opr.load_opr->replica_nodes = new vector<int>();
  for ( int i = 1; i < replication; i++ ) {
    int n = (o->node + i) % num_of_executors;
    o->opr.load_opr->replica_nodes->push_back(n);
  }
  if (o->node == executor_rank
      || member(executor_rank,o->opr.load_opr->replica_nodes))
    loadBlocks.push_back(block);
  else {
    delete_block(block,encoded_type);
    loadBlocks.push_back(nullptr);
  }
  return loc;
}

int pairOpr ( int x, int y, void* coord, vector<int>* encoded_type ) {
  Opr* o = new Opr();
  o->type = pairOPR;
  o->opr.pair_opr = new PairOpr(x,y);
  return store_opr(o,new vector<int>({ x, y }),coord,0,encoded_type);
}

int applyOpr ( int x, int fnc, void* args, void* coord, int cost,
               vector<int>* encoded_type ) {
  Opr* o = new Opr();
  o->type = applyOPR;
  o->opr.apply_opr = new ApplyOpr(x,fnc,args);
  return store_opr(o,new vector<int>({ x }),coord,cost,encoded_type);
}

int reduceOpr ( const vector<uintptr_t>* s, bool valuep, int op,
                void* coord, int cost, vector<int>* encoded_type ) {
  if (s->size() == 1)
    return (*s)[0];
  Opr* o = new Opr();
  o->type = reduceOPR;
  auto ss = new vector<int>();
  for ( uintptr_t x: *s )
    ss->push_back((int)x);
  o->opr.reduce_opr = new ReduceOpr(ss,valuep,op);
  return store_opr(o,ss,coord,cost,encoded_type);
}

// apply f to every task in the workflow starting with s
//    using breadth-first search 
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

inline bool hasCachedValue ( Opr* x ) {
  return x->cached != nullptr;
}

void cache_data ( Opr* opr, void* data ) {
  opr->cached = data;
}

// delete the block data
int delete_array_ ( void* &data, vector<int>* encoded_type, int loc ) {
  if (!delete_arrays)
    return 0;
  switch ((*encoded_type)[loc]) {
  case 0: case 1: // an int index
    return loc+1;
  case 10: { // a tuple
    switch ((*encoded_type)[loc+1]) {
      case 0: case 1:
        return loc+2;
      case 2: {
        auto x = (tuple<void*,void*>*)data;
        int l2 = delete_array_(get<0>(*x),encoded_type,loc+2);
        return delete_array_(get<1>(*x),encoded_type,l2);
      }
      case 3: {
        auto x = (tuple<void*,void*,void*>*)data;
        int l2 = delete_array_(get<0>(*x),encoded_type,loc+2);
        int l3 = delete_array_(get<1>(*x),encoded_type,l2);
        return delete_array_(get<2>(*x),encoded_type,l3);
      }
      default:
        info("Unknown tuple: %d",(*encoded_type)[loc+1]);
    }
  }
  case 11:  // a data block
    switch ((*encoded_type)[loc+1]) {
    case 0: {
      auto x = (Vec<int>*)data;
      delete x;
      data = nullptr;
      return loc+2;
    }
    case 1: {
      auto x = (Vec<long>*)data;
      delete x;
      data = nullptr;
      return loc+2;
    }
    case 3: {
      auto x = (Vec<double>*)data;
      delete x;
      data = nullptr;
      return loc+2;
    }
    default:
      info("Unknown Vec: %d",(*encoded_type)[loc+1]);
    }
  default:
    return loc+1;
  }
}

// delete the block data
void delete_block ( void* &data, vector<int>* encoded_type ) {
  try {
    delete_array_(data,encoded_type,0);
    data = nullptr;
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
    delete_block(opr->cached,opr->encoded_type);
    opr->cached = nullptr;
    info("    delete block %d (%d/%d)",opr_id,block_count,block_created);
  }
}

// does this task create a new block?
inline bool block_constructor ( Opr* opr ) {
  // true for a reduce or apply task that create a block
  //   and when receiving a remote block
  return opr->cpu_cost > 0 || opr->node != executor_rank;
}

bool sends_msgs_only ( Opr* opr ) {
  bool sends_msgs = true;
  for ( int c: *opr->consumers )
    sends_msgs = sends_msgs && operations[c]->node != opr->node;
  return sends_msgs;
}

// delete the task cache and the caches of its dependents
void may_delete ( int opr_id ) {
  Opr* opr = operations[opr_id];
  if (block_constructor(opr)) {
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

// delete the caches of the task dependents
void may_delete_deps ( int opr_id ) {
  Opr* opr = operations[opr_id];
  if (opr->cpu_cost > 0) {
    if (trace_delete)
      cout << executor_rank << " " << "may delete deps " << opr_id << endl;
    for ( int c: *opr->dependents )
      may_delete(c);
  }
}

// all local tasks that create blocks used by opr
vector<int>* get_local_dependents ( int opr_id ) {
  Opr* opr = operations[opr_id];
  if (opr->local_dependents != nullptr)
    return opr->local_dependents;
  if (opr->node != executor_rank) {
    //opr->local_dependents = new vector({opr_id});
    opr->local_dependents = empty_vector;
    return opr->local_dependents;
  }
  vector<int>* ds = new vector<int>();
  for ( int c: *opr->children ) {
    Opr* copr = operations[c];
    if (block_constructor(copr))
      ds->push_back(c);
    else {  // c has 0 cost
      copr->local_dependents = get_local_dependents(c);
      append(ds,copr->local_dependents);
    }
  }
  if (ds->size() == 0) {
    delete ds;
    opr->local_dependents = empty_vector;
  } else opr->local_dependents = ds;
  return opr->local_dependents;
}

bool has_remote_consumers ( Opr* opr ) {
  for ( int c: *opr->consumers )
    if (operations[c]->node != opr->node)
      return true;
  return false;
}

vector<int>* get_deps_of_deps ( int opr_id ) {
  Opr* opr = operations[opr_id];
  vector<int>* v = new vector<int>();
  for ( int d: *get_local_dependents(opr_id) )
    if (operations[d]->node != opr->node)
      v->push_back(d);
    else append(v,get_local_dependents(d));
  return v;
}

vector<int>* get_deps_of_deps_non_local ( int opr_id ) {
  if (operations[opr_id]->node == executor_rank)
    return get_deps_of_deps(opr_id);
  vector<int>* ds = new vector<int>();
  for ( int c: *operations[opr_id]->children )
    append(ds,get_deps_of_deps_non_local(c));
  return ds;
}

// all local tasks that create blocks that are used directly
//   or indirectly by opr (used for GC with recovery)
vector<int>* get_closest_descendants ( int opr_id ) {
  Opr* opr = operations[opr_id];
  if (opr->closest_descendants == nullptr) {
    opr->closest_descendants = (opr->node == executor_rank)
                               ? (has_remote_consumers(opr))
                                 ? get_local_dependents(opr_id)
                                 : get_deps_of_deps_non_local(opr_id)
                               : empty_vector;
  }
  return opr->closest_descendants;
}

// initialize a task
void initialize_opr ( int opr_id ) {
  Opr* opr = operations[opr_id];
  assert(opr->node >= 0);
  opr->status = notReady;
  opr->cached = nullptr;
  if (opr->type == loadOPR
      && loadBlocks[opr->opr.load_opr->block] != nullptr)
    cache_data(opr,loadBlocks[opr->opr.load_opr->block]);
  // initial count is the number of local consumers
  opr->count = 0;
  opr->message_count = 0;
  opr->local_dependents = get_local_dependents(opr_id);
  if (enable_recovery) {
    opr->closest_descendants = get_closest_descendants(opr_id);
    opr->dependents = opr->closest_descendants;
  } else opr->dependents = opr->local_dependents;
  if (opr->type == reduceOPR) {
    // used for partial reduction
    opr->reduced_count = 0;
    for ( int c: *opr->opr.reduce_opr->s )
      if (operations[c]->node == executor_rank)
        opr->reduced_count++;
    if (opr->node == executor_rank) {
      vector<int> s;
      for ( int c: *opr->opr.reduce_opr->s ) {
        int cnode = operations[c]->node;
        if (cnode != executor_rank
            && find(s.begin(),s.end(),cnode) == s.end())
          s.push_back(cnode);
      }
      opr->reduced_count += s.size();
    }
  }
}

void schedule_plan ( void* plan );

// schedule the tasks of a plan to run on processes
void schedule ( void* plan ) {
  auto time = MPI_Wtime();
  auto p = (tuple<void*,void*,vector<tuple<void*,uintptr_t>*>*>*)plan;
  int nodes[operations.size()];
  if (isCoordinator()) {
    schedule_plan(plan);
    for ( int i = 0; i < operations.size(); i++ )
      nodes[i] = operations[i]->node;
    // broadcast the schedule (just the opr->node)
    for ( int rank = 0; rank < num_of_executors; rank++ )
      if (rank != getCoordinator())
        send_data(rank,nodes,operations.size()*sizeof(int));
  } else {
    // receive the schedule
    receive_data(nodes);
    for ( int i = 0; i < operations.size(); i++ )
      operations[i]->node = nodes[i];
  }
  mpi_barrier_no_recovery();
  for ( Opr* x: operations ) {
    x->local_dependents = nullptr;
    x->closest_descendants = nullptr;
  }
  for ( int i = 0; i < operations.size(); i++ )
    initialize_opr(i);
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
    mpi_barrier_no_recovery();
}

// top-down recursive evaluation in one executor (for testing only)
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
      auto f = (vector<void*>*(*)(void*))functions[e->opr.apply_opr->fnc];
      res = (*f(inMemEval(e->opr.apply_opr->x,tabs+1)))[0];
      break;
    }
    case pairOPR: {
      int x = e->opr.pair_opr->x;
      int y = e->opr.pair_opr->y;
      void* cx = inMemEval(x,tabs+1);
      void* cy =  inMemEval(y,tabs+1);
      void* jx = get<1>(*(tuple<void*,void*>*)cx);
      void* jy = get<1>(*(tuple<void*,void*>*)cy);
      void* data = new tuple<void*,void*>(e->coord,
                          new tuple<void*,void*>(jx,jy));
      break;
    }
    case reduceOPR: {
      vector<int>* rs = e->opr.reduce_opr->s;
      vector<tuple<void*,void*>*>* sv = new vector<tuple<void*,void*>*>();
      for ( int x: *rs ) {
        auto v = (tuple<void*,void*>*)inMemEval(x,tabs+1);
        sv->push_back(v);
      }
      auto op = (void*(*)(tuple<void*,void*>*))functions[e->opr.reduce_opr->op];
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
  auto p = (tuple<void*,void*,vector<tuple<void*,uintptr_t>*>*>*)plan;
  for ( Opr* x: operations )
    x->count = x->consumers->size();
  vector<tuple<void*,void*>*>* res = new vector<tuple<void*,void*>*>();
  for ( auto d: *get<2>(*p) ) {
    auto v = (tuple<void*,void*>*)inMemEval(get<1>(*d),0);
    res->push_back(v);
  }
  return new tuple<void*,void*,void*>(get<0>(*p),get<1>(*p),res);
}

// compute the operation of one task
void* compute ( int opr_id ) {
  Opr* opr = operations[opr_id];
  info("*** computing %d:%s%s",opr_id,
       oprNames[opr->type],sprint(*opr->children).c_str());
  void* res = nullptr;
  try {
  switch (opr->type) {
    case loadOPR:
      res = loadBlocks[opr->opr.load_opr->block];
      assert(res != nullptr);
      break;
    case applyOPR: {
      auto f = (vector<void*>*(*)(void*))functions[opr->opr.apply_opr->fnc];
      res = (*(f(operations[opr->opr.apply_opr->x]->cached)))[0];
      break;
    }
    case pairOPR: {
      int x = opr->opr.pair_opr->x;
      int y = opr->opr.pair_opr->y;
      void* cx = operations[x]->cached;
      void* cy =  operations[y]->cached;
      void* jx = get<1>(*(tuple<void*,void*>*)cx);
      void* jy = get<1>(*(tuple<void*,void*>*)cy);
      res = new tuple<void*,void*>(opr->coord,
                          new tuple<void*,void*>(jx,jy));
      break;
    }
    case reduceOPR: {
      // not used if partial reduce is enabled
      vector<int>* rs = opr->opr.reduce_opr->s;
      auto op = (void*(*)(tuple<void*,void*>*))functions[opr->opr.reduce_opr->op];
      void* acc = nullptr;
      void* key = 0;
      int i = 0;
      for ( int x: *rs ) {
        auto v = (tuple<void*,void*>*)operations[x]->cached;
        if (acc == nullptr) {
          key = get<0>(*v);
          acc = get<1>(*v);
        } else {
          auto tres = new tuple<void*,void*>(key,acc);
          res = tres;
          acc = op(new tuple<void*,void*>(acc,get<1>(*v)));
          if (i > 1)
            delete_block(res,opr->encoded_type);
        }
        i++;
      }
      res = new tuple<void*,void*>(key,acc);
      break;
    }
  }
  cache_data(opr,res);
  opr->status = completed;
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

// enqueue the consumers of a partial reduce operation
void enqueue_reduce_opr ( int opr_id, int rid ) {
  Opr* opr = operations[opr_id];      // child of reduce_opr
  Opr* reduce_opr = operations[rid];  // Reduce operation
  static tuple<void*,void*>* op_arg = new tuple<void*,void*>(nullptr,nullptr);
  // partial reduce inside reduce opr
  auto op = (void*(*)(tuple<void*,void*>*))functions[reduce_opr->opr.reduce_opr->op];
  if (reduce_opr->cached == nullptr) {
    // first reduction (may delete later)
    info("    set reduce opr %d to the first input opr %d",rid,opr_id);
    reduce_opr->cached = opr->cached;
    reduce_opr->first_reduced_input = opr_id;
  } else if (reduce_opr->opr.reduce_opr->valuep) {
    // total aggregation
    get<0>(*op_arg) = reduce_opr->cached;
    get<1>(*op_arg) = opr->cached;
    reduce_opr->cached = op(op_arg);
  } else {
    auto x = (tuple<void*,void*>*)reduce_opr->cached;
    auto y = (tuple<void*,void*>*)opr->cached;
    // merge the current state with the partially reduced data
    info("    merge reduce opr %d with input opr %d",rid,opr_id);
    auto old = (void*)x;
    get<0>(*op_arg) = get<1>(*x);
    get<1>(*op_arg) = get<1>(*y);
    reduce_opr->cached = new tuple<void*,void*>(get<0>(*x),op(op_arg));
    // remove the old reduce result
    if (delete_arrays && reduce_opr->first_reduced_input < 0) {
      info("    delete current reduce result in opr %d",rid);
      delete_block(old,reduce_opr->encoded_type);
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
      if (send_data(reduce_opr->node,reduce_opr->cached,rid,1))
        delete_array(rid);
    }
    delete_first_reduce_input(rid);
  }
}

// after opr is computed, check its consumers to see if anyone is ready to enqueue
void enqueue_ready_operations ( int opr_id ) {
  Opr* opr = operations[opr_id];
  if (opr->node == executor_rank) {
    // send the opr cached result to the non-local consumers
    auto nodes_to_send_data = new vector<int>();
    for ( int c: *opr->consumers ) {
      Opr* copr = operations[c];
      if (copr->node != executor_rank
          && (!enable_partial_reduce
              || copr->type != reduceOPR)
          && !member(copr->node,nodes_to_send_data))
        nodes_to_send_data->push_back(copr->node);
    }
    { lock_guard<mutex> lock(send_mutex);
      for ( int c: *nodes_to_send_data ) {
        // lock the task until all messages have been sent
        opr->message_count++;
        send_queue.push_back(new tuple<int,int,int>(c,opr_id,0));
      }
    }
    delete nodes_to_send_data;
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
  const bool trace = false;
  bool b = ready_queue.empty() && send_queue.empty();
  for ( int x: *exit_points )
    b = b && (operations[x]->node != executor_rank
              || hasCachedValue(operations[x]));
  if (trace && !b) {
    vector<int> missing;
    for ( int x: *exit_points )
      if (operations[x]->node == executor_rank
          && !hasCachedValue(operations[x]))
        missing.push_back(x);
    info("ready: %d, send: %d, missing: %s",
         ready_queue.size(),send_queue.size(),
         sprint(missing).c_str());
  }
  return wait_all(b);
}

// kill the executor to test recovery
void kill_executor ( int executor ) {
  static int abort_count = 0;
  abort_count++;
  if (executor_rank == executor && abort_count == steps_before_abort) {
    info("Committing suicide to test recovery");
    if (!accumulator_exit())  // abort exit_poll
      send_long(getCoordinator(),0L,5);
    suspend_work = true;
    stop_sender = true;
    stop_receiver = true;
    // wait forever (will not MPI_Finalize)
    while (true)
      this_thread::sleep_for(chrono::milliseconds(1000));
  }
}

// is operation opr_id in the send queue?
bool in_send_queue ( int opr_id ) {
  lock_guard<mutex> lock(send_mutex);
  for ( tuple<int,int,int>* x: send_queue )
    if (get<1>(*x) == opr_id)
      return true;
  return false;
}

// the sender thread that sends queued messages
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
      if (opr->message_count == 0 && sends_msgs_only(opr)) {
        // when all sends from opr have been completed and opr is a sink
        //  (sends data to remote only)
        if (opr->cpu_cost > 0)
          may_delete(opr_id);
        else for ( int c: *opr->dependents )
               may_delete(c);
      }
    } else this_thread::sleep_for(chrono::milliseconds(1));
  }
}

// evaluate the tasks of the workflow using the ready_queue
void work () {
  bool exit = false;
  double t = MPI_Wtime();
  double st = t;
  while (!exit) {
    exit = false;
    // check for exit every 1 secs
    if (!suspend_work && t-st > 1.0) {
      exit = exit_poll();
      st = t;
      if (exit) break;
    }
    if (enable_recovery && test_recovery)
      kill_executor(1);  // kill executor 1 to test recovery
    if (enable_recovery && MPI_Wtime()-t > 1.0)
      ping(); // send a ping to the next executor every 1 secs
    if (!ready_queue.empty() && !suspend_work) {
      int opr_id;
      { lock_guard<mutex> lock(ready_mutex);
        opr_id = ready_queue.top();
        ready_queue.pop();
      };
      try {
        compute(opr_id);
      } catch ( const exception &ex ) {
        info("*** fatal error: %s",ex.what());
        abort();
      } catch (...) {
        info("*** fatal error: unrecognized exception");
        abort();
      }
      enqueue_ready_operations(opr_id);
    } else if (suspend_work) {
      suspend_work = false;
      mpi_barrier();
    } else if (MPI_Wtime()-t > 30.0) {
      int n = 0;
      for ( int x: *exit_points )
        if (operations[x]->node == executor_rank
            && !hasCachedValue(operations[x]))
          n++;
      info("... starving (remaining exit points: %d, ready queue: %d, send queue: %d)",
           n,ready_queue.size(),send_queue.size());
    }
    t = MPI_Wtime();
  }
  info("Executor %d has finished execution",executor_rank);
}

// the entry points of the task workflow (Load oprs)
vector<int>* entry_points ( const vector<int>* es ) {
  static vector<int>* b;
  b = new vector<int>();
  BFS(es,
      [] ( int c ) {
        Opr* copr = operations[c];
        if (copr->type == loadOPR && copr->node == executor_rank)
          if (!member(c,b))
            b->push_back(c);
      });
  return new vector<int>(*b);
}

// evaluate the plan
void* eval ( void* plan ) {
  auto time = MPI_Wtime();
  auto p = (tuple<void*,void*,vector<tuple<void*,uintptr_t>*>*>*)plan;
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
    mpi_barrier_no_recovery();
    thread rrp(run_receiver);
    thread rsp(run_sender);
    work();
    mpi_barrier();
    stop_receiver = true;
    stop_sender = true;
    kill_receiver();
    rrp.join();
    rsp.join();
  } else work();
  vector<tuple<void*,void*>*>* res = new vector<tuple<void*,void*>*>();
  for ( tuple<void*,uintptr_t>* d: *get<2>(*p) )
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

// evaluate an accumulation operation (a sub-workflow with one exit point)
void* evalOpr ( int opr_id ) {
  auto plan = new tuple<int,nullptr_t,vector<tuple<int,int>*>*>(1,nullptr,
                          new vector<tuple<int,int>*>({new tuple<int,int>(0,opr_id)}));
  schedule(plan);
  eval(plan);
  Opr* opr = operations[opr_id];
  opr->status = computed;
  auto res = (tuple<void*,void*>*)opr->cached;
  return get<1>(*res);
}

// bring the plan results to the coordinator
void* collect ( void* plan ) {
  if (!enable_collect)
    return nullptr;
  auto p = (tuple<void*,void*,vector<tuple<void*,uintptr_t>*>*>*)plan;
  vector<tuple<void*,uintptr_t>*>* es = get<2>(*p);
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
    mpi_barrier();
    return new tuple<void*,void*,void*>(get<0>(*p),get<1>(*p),blocks);
  } else {
    for ( auto x: *es ) {
      Opr* opr = operations[get<1>(*x)];
      if (opr->node == executor_rank)
        send_data(getCoordinator(),opr->cached,get<1>(*x),2);
    }
    mpi_barrier();
    return new tuple<void*,void*,void*>(get<0>(*p),get<1>(*p),nullptr);
  }
}

inline int replace_executor ( int c, int failed_executor, int new_executor ) {
  return (operations[c]->node == failed_executor)
          ? new_executor
          : operations[c]->node;
}

// return all operations that have a cached value but their parents have not
vector<int>* completed_front ( int failed_executor, int new_executor ) {
  for ( Opr* x: operations )
    x->visited = false;
  queue<int> opr_queue;
  vector<int>* front = new vector<int>();
  for ( int c: *exit_points)
    opr_queue.push(c);
  while (!opr_queue.empty()) {
    int c = opr_queue.front();
    opr_queue.pop();
    Opr* opr = operations[c];
    if (!opr->visited) {
      opr->visited = true;
      switch (opr->type) {
      case reduceOPR:
        // clear partial reduction
        opr->cached = nullptr;
        opr->reduced_count = 0;
        for ( int x: *opr->opr.reduce_opr->s )
          if (replace_executor(x,failed_executor,new_executor) == executor_rank)
            opr->reduced_count++;
        vector<int> v;
        if (replace_executor(c,failed_executor,new_executor) == executor_rank)
          for ( int x: *opr->opr.reduce_opr->s ) {
            int z = replace_executor(x,failed_executor,new_executor);
            if (z != executor_rank && !member(z,&v))
              v.push_back(z);
          }
        opr->reduced_count += v.size();
      }
      if (opr->node == executor_rank && hasCachedValue(opr)
          && block_constructor(opr)) {
        opr->status = computed;
        front->push_back(c);
      } else if (opr->type == loadOPR
                 && loadBlocks[opr->opr.load_opr->block] != nullptr) {
        cache_data(opr,loadBlocks[opr->opr.load_opr->block]);
        opr->node = executor_rank;
        opr->status = computed;
        front->push_back(c);
      } else {
        opr->status = notReady;
        opr->cached = nullptr;
        for ( int c: *opr->children)
          opr_queue.push(c);
      }
    }
  }
  return front;
}

// recovery from failure
void recover ( int failed_executor, int new_executor ) {
  suspend_work = true;
  info("Recovering from the failed executor by replacing %d with %d",
       failed_executor,new_executor);
  vector<int>* front = completed_front(failed_executor,new_executor);
  info("Completed front at recovery: %s",sprint(*front).c_str());
  if (enable_partial_reduce)
    for ( int opr_id: *front )
      for ( int c: *operations[opr_id]->consumers )
        if (operations[c]->type == reduceOPR)
          enqueue_reduce_opr(opr_id,c);
  vector<int> ev;
  if (executor_rank == new_executor) {
    for ( int opr_id: *front ) {
      operations[opr_id]->status = completed;
      ev.push_back(opr_id);
    } 
  } else {
    vector<int> sv;
    for ( int opr_id: *front ) {
      Opr* opr = operations[opr_id];
      bool feb = false;
      bool neb = false;
      for ( int c: *opr->consumers ) {
        feb = feb || operations[c]->node == failed_executor;
        neb = neb || operations[c]->node == new_executor;
      }
      if (feb && (opr->status != completed || !neb)) {
        opr->message_count++;
        sv.push_back(opr_id);
        send_queue.push_back(new tuple<int,int,int>(new_executor,opr_id,0));
      }
    }
    info("Send message queue: %s",sprint(sv).c_str());
  }
  for ( Opr* opr: operations )
    if (opr->node == failed_executor) {
      if (opr->cached != nullptr)
        opr->status = computed;
      opr->node = new_executor;
    }
  // recalculate dependencies (deps) and dependent counters (dc) at the replacing executor
  if (executor_rank == new_executor) {
    for ( Opr* opr: operations ) {
      opr->local_dependents = opr->closest_descendants = nullptr;
      opr->count = 0;
    }
    for ( int i = 0; i < operations.size(); i++ ) {
      Opr* opr = operations[i];
      opr->local_dependents = get_local_dependents(i);
      opr->closest_descendants = get_closest_descendants(i);
      opr->dependents = opr->closest_descendants;
    }
    for ( Opr* x: operations )
      if (x->node == executor_rank
          && (x->cpu_cost > 0 || sends_msgs_only(x)))
        for ( int c: *x->dependents )
          operations[c]->count++;
    for ( int opr_id: *front )
      for ( int d: *operations[opr_id]->dependents )
        may_delete(d);
  }
  if (executor_rank == new_executor) {
    info("Enqueue tasks on replacing executor: %s",sprint(ev).c_str());
    for ( int opr_id: ev )
      enqueue_ready_operations(opr_id);
  }
  info("Recovered from the failed executor %d",failed_executor);
}

const vector<string> env_names {
  "inMemory", "trace", "collect", "recovery", "test_recovery", "delete"
};

const vector<bool*> env_vars {
  &inMemory, &trace, &enable_collect, &enable_recovery, &test_recovery, &delete_arrays
};

// read environmental parameters and start MPI
void startup ( int argc, char* argv[], int block_dim_size ) {
  static char name[100];
  for ( int i = 0; i < env_names.size(); i++ ) {
    char* value = getenv(env_names[i].c_str());
    if (value != nullptr)
      *env_vars[i] = strcmp(value,"true") == 0 || strcmp(value,"y") == 0;
  }
  mpi_startup(argc,argv,block_dim_size);
}
