#include <cuda.h>

std::string loadPTX(const char* filename);

void launchCudaKernel(CUfunction cuFunction, int device_id, float* d_A, float* d_B, float* d_C, uint64_t offset0, int block_dim, int grid_dim);
