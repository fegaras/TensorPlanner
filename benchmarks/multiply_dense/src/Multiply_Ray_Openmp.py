import ray
import os
import sys
import numpy as np
import time
from matmult import *

n, iterations = int(sys.argv[1]), int(sys.argv[2])
m = n
print(f"iterations: {iterations}, n: {n}, m: {m}")
ray.init(address=os.environ["ip_head"])
print("Nodes in the Ray cluster:", len(ray.nodes()))

N = 1000

def cpp_add(x,y):
	add_matrix()
	return x

def cpp_mult(x,y):
	multiply_matrix()
	return x

@ray.remote
def create_block(v):
	return v*np.ones((N,N),dtype=np.float32)

@ray.remote
def add_blocks(x,y):
    return cpp_add(x,y)

@ray.remote
def multiply_blocks(x,y):
    return cpp_mult(x,y)

def create_matrix(n,m,v):
	rows = n//N
	cols = m//N
	return [[create_block.remote(v) for _ in range(cols)] for _ in range(rows)]


def multiply_matrices(x,y,x_is_transpose=False,y_is_transpose=False):
	rows1 = len(x)
	cols1 = len(x[0])
	rows2 = len(y)
	cols2 = len(y[0])
	if(x_is_transpose):
		rows1,cols1 = cols1,rows1
	if(y_is_transpose):
		rows2,cols2 = cols2,rows2
	ret_tensor = [[create_block.remote(0.0) for _ in range(cols2)] for _ in range(rows1)]
	for i in range(rows1):
		for j in range(cols2):
			for k in range(cols1):
				i1,j1,k1,k2=i,j,k,k
				if(x_is_transpose):
					i1,k1 = k1,i1
				if(y_is_transpose):
					k2,j1 = j1,k2
				ret_tensor[i][j] = add_blocks.remote(ret_tensor[i][j],multiply_blocks.remote(x[i1][k1],y[k2][j1]))
	return ret_tensor

def get_matrix(x_val):
    x = [[ray.get(x_val[i][j]) for j in range(len(x_val[0]))] for i in range(len(x_val))]
    return x

x_id = create_matrix(n,m,0.13)
y_id = create_matrix(m,n,1.0/n)
for _ in range(iterations):
	x_id = multiply_matrices(x_id,y_id)
t=time.time()
z = get_matrix(x_id)
print(len(z[0][0]))
run_time = time.time()-t
print("Time: ",run_time)

ray.shutdown()
