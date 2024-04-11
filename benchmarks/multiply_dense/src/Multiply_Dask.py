import numpy as np
from dask_mpi import initialize
import dask.array as da
from dask.distributed import Client
import sys
import os
import time

if __name__ == '__main__':
    initialize()
    N = 1000
    n,m,iterations = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
    print(f"n: {n}, m: {m}, iterations: {iterations}")
    client = Client()
    xd = da.random.normal(0.13, 0.5, size=(n, m), chunks=(N,N))
    yd = da.random.normal(1.0/n, 0.5, size=(m, n), chunks=(N,N))
    start = time.time()
    for _ in range(iterations):
        xd = da.matmul(xd,yd)
    res = xd.compute()
    print(len(res[0]))
    run_time = time.time()-start
    print("Time: ",run_time)
    client.shutdown()
