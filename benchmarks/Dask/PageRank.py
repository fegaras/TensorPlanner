import numpy as np
import dask.array as da
from dask.distributed import Client
import sys
import time
import sparse

if __name__ == '__main__':
    N = 1000
    n,reps = int(sys.argv[1]), int(sys.argv[2])
    b = 0.85
    print(f"n: {n}, iterations: {reps}")
    client = Client()
    rng = da.random.default_rng()
    x = rng.random((n, n), chunks=(N, N))
    x[x < 0.9] = 0
    x[x >= 0.9] = 1
    graph = x.map_blocks(sparse.COO)
    C = graph.sum(axis=0)
    start = time.time()
    print("Time: ", time.time()-start)
