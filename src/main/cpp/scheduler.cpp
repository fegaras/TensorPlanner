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
#include <unordered_map>
#include <unordered_set>
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

void set_blevel( void*& plan, unordered_map<int,int>& offset_map) {
  queue<pair<Opr*,int>> opr_queue;
  auto p = (tuple<void*,void*,vector<tuple<void*,uintptr_t>*>*>*)plan;
  for ( auto x: *get<2>(*p) ) {
    Opr* op = operations[get<1>(*x)];
    opr_queue.push({op,0});
  }
  while(!opr_queue.empty()) {
    //auto [cur_opr, cur_blevel] = opr_queue.front();
    auto p = opr_queue.front();
    auto cur_opr = p.first;
    auto cur_blevel = p.second;
    opr_queue.pop();
    if (cur_blevel > cur_opr->static_blevel) {
      cur_opr->static_blevel = cur_blevel;
      for (int c: *cur_opr->children) {
        Opr* copr = operations[c];
        opr_queue.push({copr,cur_blevel+1});
      }
    }
  }
  int offset = 0;
  for ( Opr* opr: operations )
    if (opr->type == pairOPR) {
      opr->static_blevel += size(opr);
      if(offset_map.find(opr->static_blevel) == offset_map.end()) {
        offset_map[opr->static_blevel] = offset++;
      }
    }
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

int get_coord_hash(vector<int>& coord) {
  int seed = 5381;
  for ( int i: coord )
    seed = i ^ (seed << 2) ^ (seed >> 1);
  return abs(seed);
}

int get_worker_node(vector<int>& coord, int n, int m, int row_cnt, int col_cnt, int offset) {
  if(coord.size() == 1 || n == 1)
    return abs(coord[0]+offset)%num_of_executors;
  if(m == 1)
    return abs(coord[1]+offset)%num_of_executors;
  if(coord.size() <= 3)
    return abs((coord[0]%row_cnt)*col_cnt + coord[1]%col_cnt + offset)%num_of_executors;
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

void get_coord( void* coord, vector<int>* encoded_type, vector<int>& coords, int loc ) {
  int i = 0;
  switch ((*encoded_type)[loc]) {
    case 0: case 1: // index
      i = (uintptr_t)coord;
      coords.push_back(i);
      return;
    case 10: { // tuple
      switch ((*encoded_type)[loc+1]) {
        case 2: {
          auto index = (tuple<uintptr_t,uintptr_t>*)coord;
          coords.push_back(get<0>(*index));
          coords.push_back(get<1>(*index));
          return;
        }
        case 3: {
          auto index = (tuple<uintptr_t,uintptr_t,uintptr_t>*)coord;
          coords.push_back(get<0>(*index));
          coords.push_back(get<1>(*index));
          coords.push_back(get<2>(*index));
          return;

        }
      }
    }
  }
}

void schedule_plan ( void* plan ) {
  set_sizes();
  unordered_map<int,int> offset_map;
  set_blevel(plan,offset_map);
  work = vector<int>(num_of_executors);
  tasks = vector<int>(num_of_executors);
  vector<vector<int>> op_coords(operations.size(),vector<int>());
  vector<int> in_degree(operations.size());
  for (int opr_id = 0; opr_id < operations.size(); opr_id++) {
    Opr* opr = operations[opr_id];
    get_coord(opr->coord, opr->encoded_type, op_coords[opr_id], 2);
    for ( int c: *opr->consumers )
      in_degree[c]++;
  }
  auto p = (tuple<void*,void*,vector<tuple<void*,uintptr_t>*>*>*)plan;
  vector<int> exit_points;
  for ( auto x: *get<2>(*p) )
    exit_points.push_back(get<1>(*x));
  unordered_set<int> entry_points, visited;
  queue<int> opr_queue;
  for(int ep: exit_points) {
    opr_queue.push(ep);
    visited.insert(ep);
  }
  while(!opr_queue.empty()) {
    int cur = opr_queue.front();
    opr_queue.pop();
    Opr* opr = operations[cur];
    if(opr->type == loadOPR) {
      entry_points.insert(cur);
    }
    else {
      for(int ch: *opr->children) {
        if(visited.count(ch) == 0) {
          opr_queue.push(ch);
          visited.insert(ch);
        }
      }
    }
  }
  queue<int> task_queue;
  for ( int i : entry_points ) {
    task_queue.push(i);
  }

  int row_cnt = sqrt(num_of_executors);
  int col_cnt = num_of_executors/row_cnt;

  while ( !task_queue.empty() ) {
    int c = task_queue.front();
    task_queue.pop();
    Opr* opr = operations[c];
    // add more ready nodes
    for ( int c: *opr->consumers ) {
      Opr* copr = operations[c];
      in_degree[c]--;
      if (in_degree[c] == 0) {
        // find the node of children with min workload
        int min_work = 2e9, min_tasks = 2e9, min_work_node = -1;
        for(int child: *copr->children) {
          Opr* ch_opr = operations[child];
          int tmp_work = work[ch_opr->node]+communication_cost(copr,ch_opr->node);
          if(tmp_work < min_work) {
            min_work = tmp_work;
            min_tasks = tasks[ch_opr->node];
            min_work_node = ch_opr->node;
          }
          else if(tmp_work == min_work && tasks[ch_opr->node] < min_tasks) {
            min_tasks = tasks[ch_opr->node];
            min_work_node = ch_opr->node;
          }
        }
        if(min_work_node == -1)
          min_work_node = (int)get_coord_hash(op_coords[c]) % num_of_executors;

        if(copr->type == reduceOPR) {
          if(copr->opr.reduce_opr->valuep)
            copr->node = coordinator;  // total aggregation result is on coordinator
          else
            copr->node = min_work_node;
        }
        else if(copr->type == pairOPR) {
          if((*copr->consumers).size() != 1) {
            copr->node = min_work_node;
          }
          else {
            Opr* p_opr = operations[(*copr->consumers)[0]];
            Opr* ch_opr1 = operations[(*copr->children)[0]];
            Opr* ch_opr2 = operations[(*copr->children)[1]];
            if((*p_opr->consumers).size() > 0) {
              Opr* gp_opr = operations[(*p_opr->consumers)[0]];
              int child1_size = (int)(*ch_opr1->consumers).size(), child2_size = (int)(*ch_opr2->consumers).size();
              // reduce -> apply -> pair GBJ pattern
              if(gp_opr->type == reduceOPR || (child1_size > 1 || child2_size > 1)) {
                copr->node = (int)get_worker_node(op_coords[(*copr->consumers)[0]],child1_size,child2_size,row_cnt,col_cnt,offset_map[copr->static_blevel]);
              }
              else
                copr->node = min_work_node;
            }
            else
              copr->node = min_work_node;
          }
        }
        else
          copr->node = min_work_node;
        int work_done = copr->cpu_cost + communication_cost(copr,copr->node);
        work[copr->node] += work_done;
        tasks[copr->node]++;
        task_queue.push(c);
      }
    }
  }
}
