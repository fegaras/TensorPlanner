import numpy as np
import dask.array as da
from dask.distributed import Client
import sys
import time

if __name__ == '__main__':
    N = 1000
    n,m = int(sys.argv[1]), int(sys.argv[2])
    print(f"n: {n}, m: {m}")
    client = Client()
    xd = da.random.normal(1.0, 0.5, size=(n, m), chunks=(N,N))
    yd = da.random.normal(1.0, 0.5, size=(m, n), chunks=(N,N))
    start = time.time()
    zd = da.matmul(xd,yd)
    zd.compute()
    print("Time: ", time.time()-start)
