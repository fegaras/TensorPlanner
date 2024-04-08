import ray
import os
import sys
import time
import numpy as np

n, iterations = int(sys.argv[1]), int(sys.argv[2])
m = n
print(f"iterations: {iterations}, n: {n}, m: {m}")
ray.init(address=os.environ["ip_head"])

print("Nodes in the Ray cluster:",len(ray.nodes()))
N = 1000

@ray.remote
def create_block():
	return (1.0/n) * np.random.randn(N,N)

@ray.remote
def add_blocks(x,y):
	return np.add(x,y)

@ray.remote
def multiply_blocks(x,y):
	return np.matmul(x,y)

def create_matrix(n,m):
	rows = n//N
	cols = m//N
	return [[create_block.remote() for _ in range(cols)] for _ in range(rows)]

def multiply_matrices(x,y):
	rows1 = len(x)
	cols1 = len(x[0])
	cols2 = len(y[0])
	ret_tensor = [[create_block.remote() for _ in range(cols2)] for _ in range(rows1)]
	for i in range(rows1):
		for j in range(cols2):
			for k in range(cols1):
				ret_tensor[i][j] = add_blocks.remote(ret_tensor[i][j],multiply_blocks.remote(x[i][k],y[k][j]))
	return ret_tensor

def get_matrix(x_val):
    x = [[ray.get(x_val[i][j]) for j in range(len(x_val[0]))] for i in range(len(x_val))]
    return x

x_id = create_matrix(n,m)
y_id = create_matrix(m,n)
for _ in range(iterations):
	x_id = multiply_matrices(x_id,y_id)
t=time.time()
z = get_matrix(x_id)
print(len(z[0][0]))
run_time = time.time()-t
print("Time: ",run_time)

ray.shutdown()
