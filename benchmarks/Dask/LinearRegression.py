import numpy as np
import dask.array as da
from dask.distributed import Client
import sys
import time

if __name__ == '__main__':
    N = 1000
    n,m,reps = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
    lrate=0.001
    print(f"n: {n}, m: {m}, iterations: {reps}")
    client = Client()
    A_d = da.random.normal(1.0, 0.5, size=(n, m), chunks=(N,N))
    C_d = da.random.normal(1.0, 0.5, size=(m,1), chunks=(N,1))
    B_d = da.matmul(A_d,C_d)
    theta = da.random.normal(1.0, 0.5, size=(m,1), chunks=(N,1))
    start = time.time()
    iter = 0
    while(iter < reps):
        b1 = da.matmul(A_d, theta)
        b1 = da.subtract(b1,B_d)
        d_th = da.matmul(da.transpose(A_d),b1)
        theta = da.subtract(theta,lrate*d_th)
        iter = iter + 1
    theta.compute()
    print("Time: ", time.time()-start)
