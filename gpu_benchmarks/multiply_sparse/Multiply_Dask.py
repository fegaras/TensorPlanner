import cupy as cp
from dask.distributed import Client, wait
from dask_cuda import LocalCUDACluster

import dask.array as da
import sparse
from cupyx.scipy.sparse import csr_matrix
import sys
import os
import time

if __name__ == '__main__':
    N = 4096
    n = int(sys.argv[1])
    cluster = LocalCUDACluster()
    client = Client(cluster)
    
    # rng = cp.random.default_rng(42)
    # DENSITY = 0.01
    # a = sparse.random((n, n), density=DENSITY)
    # b = sparse.random((n, n), density=DENSITY)

    # a_dask = da.from_array(a, chunks=(N,N))
    # b_dask = da.from_array(b, chunks=(N,N))
    # print(a_dask)
    # assert sparse.all(a + b == (a_dask + b_dask).compute())
    
    
    # rs = da.random.RandomState(RandomState=cp.random.RandomState)
    # x = rs.normal(10, 1, size=(n, n), chunks=(N, N), dtype=cp.float32)
    # y = rs.normal(10, 1, size=(n, n), chunks=(N, N), dtype=cp.float32)
    # x[x < 0.99] = 0
    # xs = x.map_blocks(sparse.COO)
    # y[y < 0.99] = 0
    # ys = y.map_blocks(sparse.COO)
    # xs.persist()
    # ys.persist()
    # z = da.matmul(xs,ys)
    
    rs = da.random.RandomState(RandomState=cp.random.RandomState)
    x = rs.normal(1.0, 1, size=(n, n), chunks=(N, N), dtype=cp.float32)
    y = rs.normal(1.0, 1, size=(n, n), chunks=(N, N), dtype=cp.float32)
    x[x < 0.99] = 0
    y[y < 0.99] = 0
    xs = x.map_blocks(csr_matrix,dtype=cp.float32)
    ys = y.map_blocks(csr_matrix,dtype=cp.float32)
    xs.persist()
    ys.persist()
    z = da.matmul(xs,ys)
    
    start = time.time()
    z = z.persist()
    _ = wait(z)
    z = z.todense()
    # print(z[0][0])
    run_time = time.time()-start
    print("Time: ",run_time)
