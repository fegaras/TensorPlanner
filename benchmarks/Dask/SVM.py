import numpy as np
import dask.array as da
from dask.distributed import Client
import sys
import time

if __name__ == '__main__':
    N = 1000
    n,m,reps = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
    lrate=0.001
    ld = 1/reps
    print(f"n: {n}, m: {m}, iterations: {reps}")
    client = Client()
    X_d = da.random.normal(1.0, 0.5, size=(n, m), chunks=(N,N))
    Y_d = da.random.normal(1.0, 0.5, size=(n,1), chunks=(N,1))
    W_d = da.random.normal(1.0, 0.5, size=(n,m), chunks=(N,N))
    start = time.time()
    iter = 0
    while(iter < reps):
        Y1 = da.multiply(W_d, X_d)
        Y2 = da.sum(Y1,axis=0)
        Y3 = da.multiply(Y2, Y_d)
        Y4 = da.map_blocks(lambda a, b: a*(1-b), Y_d, Y3)
        Y5 = da.matmul(X_d, Y4)
        W_d = da.subtract(W_d,ld*lrate*Y5)
        iter = iter + 1
    W_d.compute()
    print("Time: ", time.time()-start)
