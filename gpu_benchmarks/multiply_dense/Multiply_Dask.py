import cupy as cp
from dask.distributed import Client, wait
from dask_cuda import LocalCUDACluster

import dask.array as da
import sys
import os
import time

if __name__ == '__main__':
    N = 4096
    n = int(sys.argv[1])
    cluster = LocalCUDACluster()
    client = Client(cluster)
    rs = da.random.RandomState(RandomState=cp.random.RandomState)
    x = rs.normal(10, 1, size=(n, n), chunks=(N, N), dtype=cp.float32)
    y = rs.normal(10, 1, size=(n, n), chunks=(N, N), dtype=cp.float32)
    x.persist()
    y.persist()
    z = da.matmul(x,y)
    start = time.time()
    z = z.persist()
    _ = wait(z)
    print(z[0][0])
    run_time = time.time()-start
    print("Time: ",run_time)
