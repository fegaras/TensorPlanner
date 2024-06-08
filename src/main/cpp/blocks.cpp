#include "omp.h"
#include <stdio.h>
#include <iostream>
#include <cstdlib>
#include "blocks.h"

bool is_GPU() {
  int local_rank = 0;
  char* lc = getenv("OMPI_COMM_WORLD_LOCAL_RANK");
  if (lc == nullptr)
    lc = getenv("MV2_COMM_WORLD_LOCAL_RANK");
  if (lc != nullptr)
    local_rank = atoi(lc);
  return omp_get_num_devices() > 0 && omp_get_default_device() == local_rank;
}

const int block_list_size = 100000;
void* blocks[block_list_size];
int gc_first = 0;
bool is_initialized = false;

// find how freelist is implemented for garbage collector
//Initially, blocks[i]=(void*)(uintptr_t)(i+1)
void init_blocks() {
    for (int i = 0; i < block_list_size-1; i++) {
        blocks[i] = (void*)(uintptr_t)(i+1);
    }
}

// store block and return its location in blocks
int new_block(size_t t, size_t len) {
    int loc = gc_first;
    int device_id = omp_get_default_device();
    void* d_block = omp_target_alloc(len*t, device_id);
    gc_first = (int)(uintptr_t)blocks[gc_first];
    printf("gc_first: %d\n",gc_first);
    blocks[loc] = d_block;
    return loc;
}

void* get_block(int loc) {
    return blocks[loc];
}

void delete_block(int loc) {
    void *d_block = blocks[loc];
    omp_target_free(d_block, omp_get_default_device());
    // update gc_first
    blocks[loc] = (void*)(uintptr_t)gc_first;
    gc_first = loc;
}