#include <vector>
#include <tuple>
#include <queue>
#include <cassert>
#include <cstdlib>
#include "tensor.h"

enum StorageType { REMOTE, COMPUTE, CACHED };

// references a Block
typedef struct {
  void* block;  // tuple(block_coords,tuple(dense_dims,sparse_dims))
  void* data;   // equal to get<2>(get<1>(block)).data
} Block;


typedef struct {
  enum StorageType st;
  union {
    struct {
      int rank;
      vector<int> consumers;
    } remote;
    struct {
      Block (*fnc)(vector<Block>);
      vector<int> producers;
      vector<int> consumers;
    } compute;
    struct {
      int consumer_count;
      Block data;
    } cached;
  };
} Node;

static vector<Node*> blocks;
static queue<int> job_queue;

short ready_to_compute ( int loc ) {
  Node* b = blocks[loc];
  assert(b != 0);
  if (b->st != COMPUTE)
    return 0;
  vector<int> producers = b->compute.producers;
  for ( int p: b->compute.producers ) {
    assert(blocks[p] != 0);
    if (blocks[p]->st != CACHED)
      return 0;
  }
  return 1;
}

void schedule ( int loc ) {
  int m = ({ int n = 3; n+1; });
  if (ready_to_compute(loc))
    job_queue.push(loc);
}

void delete_block ( Block b ) {
  free(b.data);
  b.data = NULL;
}

Block compute ( int loc ) {
  Node* b = blocks[loc];
  assert(b != 0 && b->st == COMPUTE);
  vector<int> producers = b->compute.producers;
  vector<Block> in(10);
  for ( int p: producers ) {
    assert(blocks[p] != 0 && blocks[p]->st == CACHED);
    in.push_back(blocks[p]->cached.data);
  }
  Block res = b->compute.fnc(in);
  for ( int p: producers )
    if (--blocks[p]->cached.consumer_count == 0)
      delete_block(blocks[p]->cached.data);
  b->st = CACHED;
  b->cached.data = res;
  b->cached.consumer_count = b->compute.consumers.size();
  for ( int c: b->compute.consumers )
    schedule(c);
  return res;
}
