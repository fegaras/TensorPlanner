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

long get_coord_hash(vector<long>& coord) {
  long seed = rand();
  for(int i : coord) {
    seed ^= (i + (seed << 3) + (seed >> 1));
    seed = seed % 5381;
  }
  return abs(seed);
}

long get_worker_node(vector<long>& coord, long row_cnt, long col_cnt, long row_sz, long row_sz1, long col_sz) {
  if(coord.size() == 1)
    return coord[0]/row_sz1;
  if(coord.size() == 2)
    return (coord[0]/row_sz)*col_cnt + coord[1]/col_sz;
  return get_coord_hash(coord) % num_of_executors;
}

void add_tasks(vector<int>& task_list, int node, queue<int>& q) {
  for (int c: task_list) {
    Opr* copr = operations[c];
    if(copr->node == -1) {
      if(copr->type == reduceOPR && copr->opr.reduce_opr->valuep) // total aggregation result is on coordinator
        copr->node = coordinator;
      else
        copr->node = node;
      q.push(c);
    }
  }
}

void get_coord( void* coord, vector<int>* encoded_type, vector<long>& coords, int loc ) {
  long i = 0;
  switch ((*encoded_type)[loc]) {
    case 0: case 1: // index
      i = (long)coord;
      coords.push_back(i);
      return;
    case 10: { // tuple
      switch ((*encoded_type)[loc+1]) {
        case 2: {
          tuple<long,long>* index = coord;
          coords.push_back((long)get<0>(*index));
          coords.push_back((long)get<1>(*index));
          return;
        }
        case 3: {
          tuple<long,long,long>* index = coord;
          coords.push_back((long)get<0>(*index));
          coords.push_back((long)get<1>(*index));
          coords.push_back((long)get<2>(*index));
          return;

        }
      }
    }
  }
}

void schedule_plan ( void* plan ) {
  set_sizes();
  queue<int> task_queue;
  vector<vector<long>> op_coords(operations.size(),vector<long>());
  long n = 0, m = 0;
  for (int opr_id = 0; opr_id < operations.size(); opr_id++) {
    Opr* opr = operations[opr_id];
    get_coord(opr->coord, opr->encoded_type, op_coords[opr_id], 2);
    if(op_coords[opr_id].size() == 0) {
      continue;
    }
    n = max(n, op_coords[opr_id][0]);
    if(op_coords[opr_id].size() == 1)
      m = max(m, op_coords[opr_id][0]);
    else if(op_coords[opr_id].size() > 1)
      m = max(m, op_coords[opr_id][1]);
  }
  n++,m++;
  long row_cnt = sqrt(num_of_executors);
  long col_cnt = num_of_executors/row_cnt;
  long row_sz = ceil((double)n/row_cnt);
  long row_sz1 = ceil((double)n/min(n,(long)num_of_executors));
  long col_sz = ceil((double)m/col_cnt);

  for (int opr_id = 0; opr_id < operations.size(); opr_id++) {
    Opr* opr = operations[opr_id];
    if(opr->node != -1)
      continue;
    int w = (int)get_coord_hash(op_coords[opr_id]) % num_of_executors;
    if((*opr->consumers).size() == 0)
      continue;
    Opr* p_opr = operations[(*opr->consumers)[0]];
    switch (opr->type) {
      case pairOPR:
        if(op_coords[opr_id].size() == 1)
          w = op_coords[opr_id][0] % num_of_executors;
        if((*p_opr->consumers).size() > 0) {
          Opr* gp_opr = operations[(*p_opr->consumers)[0]];
          if(gp_opr->type == reduceOPR  && !gp_opr->opr.reduce_opr->valuep)
            opr->node = (int)get_worker_node(op_coords[(*opr->consumers)[0]],row_cnt,col_cnt,row_sz,row_sz1,col_sz);
          else if(gp_opr->type != reduceOPR)
            opr->node = w;
        }
        if(opr->node != -1)
          task_queue.push(opr_id);
        break;
      case loadOPR:
        opr->node = (int)get_worker_node(op_coords[opr_id],row_cnt,col_cnt,row_sz,row_sz1,col_sz);
        task_queue.push(opr_id);
        break;
    }
  }
  vector<int> work_done(num_of_executors), tasks(num_of_executors);
  while (!task_queue.empty()) {
    int cur_task = task_queue.front();
    task_queue.pop();
    Opr* opr = operations[cur_task];
    work_done[opr->node] += opr->cpu_cost;
    tasks[opr->node]++;
    // add more ready nodes
    add_tasks(*opr->consumers, opr->node, task_queue);
    add_tasks(*opr->children, opr->node, task_queue);
  }
}
