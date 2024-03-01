import numpy as np
import dask.array as da
from dask.distributed import Client
import sys
import math
import time

def sigmoid(x):
    return np.array([[1.0/(1.0+math.exp(-j)) for j in x[i]] for i in range(x.shape[0])])

def sigmoid_der(x):
    s = sigmoid(x)
    return s*(1-s)

def apply_map(x, func):
    return np.array([[func(j) for j in x[i]] for i in range(x.shape[0])])

if __name__ == '__main__':
    N = 1000
    n,m,reps = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
    lrate=0.5
    print(f"n: {n}, m: {m}, iterations: {reps}")
    client = Client()
    
    X_id = da.random.normal(1.0, 0.5, size=(n, m), chunks=(N,N))
    Y_id = da.random.normal(1.0, 0.5, size=(n,1), chunks=(N,1))

    layer = N
    w1 = da.random.normal(1.0, 0.5, size=(m,layer), chunks=(N,N))
    w2 = da.random.normal(1.0, 0.5, size=(layer,1), chunks=(N,1))
    Z_arr1 = da.random.normal(1.0, 0.5, size=(n,m), chunks=(N,N))
    Z_arr2 = da.random.normal(1.0, 0.5, size=(n,1), chunks=(N,1))
    A_arr1 = da.random.normal(1.0, 0.5, size=(n,m), chunks=(N,N))
    A_arr2 = da.random.normal(1.0, 0.5, size=(n,m), chunks=(N,N))
    start = time.time()
    iter = 0
    while(iter < reps):
        A_curr1 = X_id
        A_arr1 = A_curr1
        Z_arr1 = da.matmul(A_curr1, w1)
        A_curr2 = Z_arr1.map_blocks(lambda a: apply_map(a, lambda v: max(0,v)))
        
        A_arr2 = A_curr2
        Z_arr2 = da.matmul(A_curr2, w2)
        Y1 = Z_arr2.map_blocks(sigmoid)

        dA_prev1 = da.subtract(Y_id,Y1)
        dA2 = dA_prev1.map_blocks(sigmoid_der)
        dZ2 = da.multiply(dA2, Z_arr2)
        dW_curr2 = da.matmul(da.transpose(A_arr2), dZ2)
        dA_prev2 = da.matmul(dZ2, da.transpose(w2))
        w2 = da.subtract(w2,lrate*dW_curr2)
        dA1 = dA_prev2
        dZ1 = da.multiply(dA1,Z_arr1.map_blocks(lambda a: apply_map(a, lambda v: 0 if(v <= 0) else 1)))
        dW_curr1 = da.matmul(da.transpose(A_arr1), dZ1)
        w1 = da.subtract(w1,lrate*dW_curr1)

        iter = iter + 1
    print(w1.compute())
    print(w2.compute())
    print("Time: ", time.time()-start)
