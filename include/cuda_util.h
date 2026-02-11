#include <cuda.h>

std::string loadPTX(const char* filename);

void launchCudaKernel(CUfunction cuFunction, int device_id, float* d_C, float* d_B, float* d_A);
