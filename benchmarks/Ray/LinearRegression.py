import torch
import ray
import os
import sys
import time
import math

num_cpus, n, m, reps = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
lrate = 0.001
print(f"CPUs: {num_cpus}, n: {n}, m: {m}, iterations: {reps}")
#ray.init(address=os.environ["ip_head"])
ray.init(num_cpus=num_cpus)
print("Nodes in the Ray cluster:")
print(ray.nodes())
N = 1000
@ray.remote
def create_block():
	return torch.randn(N,N)

@ray.remote
def add_blocks(x,y):
	return torch.add(x,y)

@ray.remote
def sub_blocks(x,y,alpha=1):
	return torch.sub(x,y,alpha=alpha)

@ray.remote
def multiply_blocks(x,y):
	return torch.matmul(x,y)

@ray.remote
def create_matrix(n,m):
	rows = math.ceil(n/N)
	cols = math.ceil(m/N)
	return [[create_block.remote() for _ in range(cols)] for _ in range(rows)]

@ray.remote
def add_matrices(x,y):
	rows = len(x)
	cols = len(x[0])
	return [[add_blocks.remote(x[i][j],y[i][j]) for j in range(cols)] for i in range(rows)]

@ray.remote
def sub_matrices(x,y,alpha=1):
	rows = len(x)
	cols = len(x[0])
	return [[sub_blocks.remote(x[i][j],y[i][j],alpha) for j in range(cols)] for i in range(rows)]

@ray.remote
def multiply_matrices(x,y,x_is_transpose=False,y_is_transpose=False):
	rows1 = len(x)
	cols1 = len(x[0])
	rows2 = len(y)
	cols2 = len(y[0])
	if(x_is_transpose):
		rows1,cols1 = cols1,rows1
	if(y_is_transpose):
		rows2,cols2 = cols2,rows2
	ret_tensor = [[create_block.remote() for _ in range(cols2)] for _ in range(rows1)]
	for i in range(rows1):
		for j in range(cols2):
			for k in range(cols1):
				i1,j1,k1=i,j,k
				if(x_is_transpose):
					i1,k1 = k1,i1
				if(y_is_transpose):
					k1,j1 = j1,k1
				ret_tensor[i][j] = add_blocks.remote(ret_tensor[i][j],multiply_blocks.remote(x[i1][k1],y[k1][j1]))
	return ret_tensor

a_id = create_matrix.remote(n,m)
c_id = create_matrix.remote(m,1)
b_id = multiply_matrices.remote(a_id,c_id)
theta_id = create_matrix.remote(m,1)

t=time.time()
iter = 0
while(iter < reps):
    b1 = multiply_matrices.remote(a_id,theta_id)
    b1 = sub_matrices.remote(b1,b_id)
    d_th = multiply_matrices.remote(a_id,b1,True,False)
    theta_id = sub_matrices.remote(theta_id,d_th,lrate)
    iter = iter + 1

theta = ray.get(theta_id)
res = [[ray.get(theta[i][j]) for j in range(len(theta[0]))] for i in range(len(theta))]
print("Time: ",time.time()-t)
ray.shutdown()