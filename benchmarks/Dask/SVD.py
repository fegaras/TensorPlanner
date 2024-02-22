import numpy as np
import dask.array as da
from dask.distributed import Client
import sys
import time

if __name__ == '__main__':
    N = 1000
    n,m,reps = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])

    print(f"n: {n}, m: {m}, iterations: {reps}")
    client = Client()
    X_d = da.random.normal(1.0, 0.5, size=(n, m), chunks=(N,N))
    U_d = da.random.normal(1.0, 0.5, size=(n,1), chunks=(N,1))
    V_d = da.random.normal(1.0, 0.5, size=(m,1), chunks=(N,1))
    start = time.time()
    iter = 0
    while(iter < reps):
        X1 = da.matmul(U_d, da.transpose(V_d))
        X2 = da.subtract(X_d,X1)
        covariance = da.matmul(da.transpose(X2),X2)
        rep = 0
        while(rep < reps):
            V_d = da.matmul(covariance,V_d)
            ret = rep + 1
        U_d = da.matmul(X_d, V_d)
        iter = iter + 1
    U_d.compute()
    V_d.compute()
    print("Time: ", time.time()-start)
