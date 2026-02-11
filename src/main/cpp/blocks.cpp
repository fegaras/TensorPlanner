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

#include <omp.h>
#include <openacc.h>
#include <cuda_runtime.h>
#include <cuda.h>
#include <stdio.h>
#include <iostream>
#include <cstdlib>
#include "blocks.h"
#include <mutex>
#include <vector>

using namespace std;

int get_gpu_id() {
  int local_rank = 0;
  char* lc = getenv("OMPI_COMM_WORLD_LOCAL_RANK");
  if (lc == nullptr)
    lc = getenv("MV2_COMM_WORLD_LOCAL_RANK");
  if (lc != nullptr)
    local_rank = atoi(lc);
  cudaSetDevice(local_rank);
  return local_rank;
}

int getDeviceCount() {
  int deviceCount;
  cudaGetDeviceCount(&deviceCount);
  return deviceCount;
}

void setDevice(int device_id) {
  cudaSetDevice(device_id);
}

bool is_GPU() {
  if(!use_GPU) return false;
  return get_gpu_id() < getDeviceCount();
}

const int block_list_size = 100000;
// use dynamic allocation after executing MPI_INIT
vector<void*> blocks;
int gc_first = 0;
mutex block_mutex;

// find how freelist is implemented for garbage collector
// Initially, blocks[i]=(void*)(uintptr_t)(i+1)
void init_blocks() {
  int device_id = get_gpu_id();
  setDevice(device_id);
  lock_guard<mutex> lock(block_mutex);
  blocks.resize(block_list_size);
  for (int i = 0; i < block_list_size - 1; i++) {
    blocks[i] = (void *)(uintptr_t)(i + 1);
  }
}

// store block and return its location in blocks
int new_block(size_t t, size_t len) {
  lock_guard<mutex> lock(block_mutex);
  int loc = gc_first;
  int device_id = get_gpu_id();
  setDevice(device_id);
  void *d_block;
  cudaMalloc(&d_block, len * t);
  gc_first = (int)(uintptr_t)blocks[gc_first];
  blocks[loc] = d_block;
  return loc;
}

void *get_block(int loc) {
  int device_id = get_gpu_id();
  setDevice(device_id);
  return blocks[loc];
}

void delete_block(int loc) {
  lock_guard<mutex> lock(block_mutex);
  void *d_block = blocks[loc];
  int device_id = get_gpu_id();
  setDevice(device_id);
  cuMemFree((CUdeviceptr)d_block);
  // update gc_first
  blocks[loc] = (void *)(uintptr_t)gc_first;
  gc_first = loc;
}

void* allocate_memory(size_t t) {
  int device_id = get_gpu_id();
  setDevice(device_id);
  void* block;
  cuMemAlloc((CUdeviceptr*)&block, t);
  return block;
}

void copy_block(char *data, const char *buffer, size_t len, int memcpy_kind) {
  int device_id = get_gpu_id();
  setDevice(device_id);
  switch(memcpy_kind) {
    case cudaMemcpyH2D:
      cuMemcpyHtoD((CUdeviceptr)data, (const void*)buffer, len);
      break;
    case cudaMemcpyD2H:
      cuMemcpyDtoH((void*)data, (CUdeviceptr)buffer, len);
      break;
    case cudaMemcpyD2D:
      cuMemcpyDtoD((CUdeviceptr)data, (CUdeviceptr)buffer, len);
      break;
    default:
      cuMemcpyHtoD((CUdeviceptr)data, (const void*)buffer, len);
  }
}

void initMatrix(float* A, float a, int N) {
  int device_id = get_gpu_id();
  setDevice(device_id);
#pragma acc parallel loop gang vector_length(1024) deviceptr(A)
  for (int i = 0; i < N; i++) {
    A[i] = a;
  }
}

void mergeMatrix(float* A, float* B, float* C, int N) {
  int device_id = get_gpu_id();
  setDevice(device_id);
#pragma acc parallel loop gang vector_length(1024) deviceptr(A,B,C)
  for (int i = 0; i < N; i++) {
    C[i] = A[i] + B[i];
  }
}
