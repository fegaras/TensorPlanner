#include <cuda.h>
#include <fstream>
#include <cassert>

// Helper to load PTX from file
std::string loadPTX(const char* filename) {
    std::ifstream file(filename);
    assert(file && "Failed to open PTX file");
    return std::string((std::istreambuf_iterator<char>(file)),
                        std::istreambuf_iterator<char>());
}

void launchCudaKernel(CUfunction cuFunction, int device_id, float* d_A, float* d_B, float* d_C, uint64_t offset0, int block_dim, int grid_dim) {
    void* args[] = { &offset0, &d_A, &d_B, &d_C };

    int blockDimX = block_dim;
    int blockDimY = 1;
    int gridDimX = grid_dim;
    int gridDimY = 1;
    // Launch kernel
    CUresult res = cuLaunchKernel(cuFunction,
                   gridDimX, gridDimY, 1,      // grid dim
                   blockDimX, blockDimY, 1,      // block dim
                   0, 0,         // shared mem, stream
                   args, 0);
    assert(res == CUDA_SUCCESS);
    cuCtxSynchronize();
}
