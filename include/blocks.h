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

/**
 * CUDA memory copy types
 */
enum memCpyKind
{
    cudaMemcpyH2H       =   0,      /**< Host   -> Host */
    cudaMemcpyH2D       =   1,      /**< Host   -> Device */
    cudaMemcpyD2H       =   2,      /**< Device -> Host */
    cudaMemcpyD2D       =   3,      /**< Device -> Device */
    cudaMemcpyDef       =   4       /**< Direction of the transfer is inferred from the pointer values. Requires unified virtual addressing */
};

bool is_GPU();

int getDeviceCount();

void setDevice(int device_id);

int get_gpu_id();

void init_blocks();

int new_block(size_t t, size_t len);

void* get_block(int loc);

void delete_block(int loc);

void* allocate_memory(size_t t);

void copy_block(char *data, const char *buffer, size_t len, int memcpy_kind);

void initMatrix(double* A, double a, int N);

void mergeMatrix(double* A, double* B, double* C, int N);

