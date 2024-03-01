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

long get_worker_node(vector<long>& coord, long n, long m) {
  long row_cnt = sqrt(num_of_executors);
  long col_cnt = num_of_executors/row_cnt;
  long row_sz = ceil((double)m/row_cnt);
  long row_sz1 = ceil((double)m/min(m,(long)num_of_executors));
  long col_sz = ceil((double)n/col_cnt);
  long col_sz1 = ceil((double)n/min(n,(long)num_of_executors));
  if(coord.size() == 1 || n == 1)
    return coord[0]/row_sz1;
  if(m == 1)
    return coord[1]/col_sz1;
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

  long total_work = 0, total_pairs = 0;
  for (int opr_id = 0; opr_id < operations.size(); opr_id++) {
    Opr* opr = operations[opr_id];
    total_work += opr->cpu_cost;
    if(opr->type == pairOPR)
      total_pairs++;
    get_coord(opr->coord, opr->encoded_type, op_coords[opr_id], 2);
  }
  long pair_threshold = ceil(1.1*total_pairs/(double)num_of_executors);
  vector<int> pair_count(num_of_executors);
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
          Opr* copr1 = operations[(*opr->children)[0]];
          Opr* copr2 = operations[(*opr->children)[1]];
          Opr* gp_opr = operations[(*p_opr->consumers)[0]];
          if(gp_opr->type == reduceOPR  && !gp_opr->opr.reduce_opr->valuep)
            opr->node = (int)get_worker_node(op_coords[(*opr->consumers)[0]],(long)(*copr1->consumers).size(),(long)(*copr2->consumers).size());
          else if(gp_opr->type != reduceOPR) {
            if((*copr1->consumers).size() > 1) {
              opr->node = (int)get_worker_node(op_coords[(*opr->consumers)[0]],(long)(*copr1->consumers).size(),(long)(*copr2->consumers).size());
            }
          }
        }
        else {
          Opr* copr1 = operations[(*opr->children)[0]];
          Opr* copr2 = operations[(*opr->children)[1]];
          if((*copr1->consumers).size() > 1) {
            opr->node = (int)get_worker_node(op_coords[(*opr->consumers)[0]],(*copr1->consumers).size(),(*copr2->consumers).size());
          }
        }
        if(opr->node == -1) {
          opr->node = w;
        }
        while(pair_count[opr->node] >= pair_threshold) {
          opr->node = rand() % num_of_executors;
        }
        pair_count[opr->node]++;
        task_queue.push(opr_id);
        break;
      case loadOPR:
        if((*opr->consumers).size() == 1)
          opr->node = rand() % num_of_executors;
        break;
      case reduceOPR:
        // total aggregation in executor 0
        if(opr->opr.reduce_opr->valuep)
          opr->node = 0;
    }
  }
  long threshold = ceil(1.1*total_work/(double)num_of_executors);
  vector<int> work_done(num_of_executors), task_count(num_of_executors);
  while (!task_queue.empty()) {
    int cur_task = task_queue.front();
    task_queue.pop();
    Opr* opr = operations[cur_task];
    work_done[opr->node] += opr->cpu_cost;
    task_count[opr->node]++;
    int w = opr->node;
    if(opr->type != pairOPR && work_done[opr->node] >= threshold) {
      w = rand() % num_of_executors;
      while(work_done[w] >= threshold) {
        w = rand() % num_of_executors;
      }
    }
    // add more ready nodes
    add_tasks(*opr->consumers, w, task_queue);
    add_tasks(*opr->children, w, task_queue);
  }
}
