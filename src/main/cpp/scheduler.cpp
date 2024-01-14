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

#include <vector>
#include <tuple>
#include <queue>
#include <cmath>
#include "tensor.h"
#include "opr.h"
#include "comm.h"

vector<int> work;
vector<int> tasks;
vector<int> ready_pool;

extern int coordinator;
extern int num_of_executors;
extern vector<Opr*> operations;

int size ( Opr* e ) {
  if (e->size <= 0) {
    e->size = 1;
    if (e->type == pairOPR)
      e->size = size(operations[e->opr.pair_opr->x])
                  + size(operations[e->opr.pair_opr->y]);
  }
  return e->size;
}

void set_sizes () {
  for ( Opr* x: operations )
    size(x);
}

int cpu_cost ( Opr* opr ) {
  return opr->cpu_cost;
}

int communication_cost ( Opr* opr, int node ) {
  int n = 0;
  for ( int c: *opr->children ) {
    Opr* copr = operations[c];
    if (copr->node != node)
      n += size(copr);
  }
  return n;
}

struct Worker {
  int workDone;
  int numTasks;
  int id;
};

auto work_cmp = [] ( Worker x, Worker y ) {
  if (x.workDone == y.workDone)
    return y.numTasks <= x.numTasks;
  else return y.workDone <= x.workDone;
};
static priority_queue<Worker,vector<Worker>,decltype(work_cmp)>
            workerQueue(work_cmp);

void schedule_plan ( void* plan ) {
  set_sizes();
  queue<int> task_queue;
  for ( int i = 0; i < num_of_executors; i++ ) {
    work.push_back(0);
    tasks.push_back(0);
  }
  vector<int> in_degree(operations.size() );
  for ( Opr* op: operations )
    for ( int c: *op->consumers )
      in_degree[c]++;
  vector<int> ep;
  for ( int i = 0; i < in_degree.size(); i++ )
    if (in_degree[i] == 0)
      ep.push_back(i);
  for ( int op: ep )
    task_queue.push(op);
  for ( int i = 0; i < num_of_executors; i++ )
    workerQueue.push({ 0, 0, i });
  float total_time = 0.0;
  float worker_time = 0.0;
  int k = min(num_of_executors,(int)(sqrt(num_of_executors))+1);
  while ( !task_queue.empty() ) {
    int c = task_queue.front();
    task_queue.pop();
    Opr* opr = operations[c];
    vector<Worker> tmp_workers;
    Worker w = { 0, 0, 0 };
    int min_comm_cost = 100000;
    for ( int i = 0; i < k; i++ ) {
      Worker tmp_worker = workerQueue.top();
      workerQueue.pop();
      tmp_workers.push_back(tmp_worker);
      int tmp_cost = communication_cost(opr,tmp_worker.id);
      if (tmp_cost < min_comm_cost) {
        w = tmp_worker;
        min_comm_cost = tmp_cost;
      }
    }
    for ( Worker tw: tmp_workers )
      if (tw.id != w.id)
        workerQueue.push(tw);
    // opr is allocated to worker w
    if (opr->type == reduceOPR && opr->opr.reduce_opr->valuep)
      opr->node = coordinator;  // total aggregation result is on coordinator
    else opr->node = w.id;
    int work_done = cpu_cost(opr) + communication_cost(opr,w.id);
    workerQueue.push({ w.workDone + work_done, w.numTasks+1, w.id });
    work[w.id] += work_done;
    tasks[w.id]++;
    // add more ready nodes
    for ( int c: *opr->consumers ) {
      Opr* copr = operations[c];
      in_degree[c]--;
      if (in_degree[c] == 0)
          task_queue.push(c);
    }
  }
}
