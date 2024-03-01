import torch
import ray
import os
import sys
import time

num_cpus, n, m = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
print(f"CPUs: {num_cpus}, n: {n}, m: {m}")
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
def multiply_blocks(x,y):
	return torch.matmul(x,y)

@ray.remote
def create_matrix(n,m):
	rows = n//N
	cols = m//N
	return [[create_block.remote() for _ in range(cols)] for _ in range(rows)]

@ray.remote
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

x_id = create_matrix.remote(n,m)
y_id = create_matrix.remote(m,n)
z_id = multiply_matrices.remote(x_id,y_id)
t=time.time()
z = ray.get(z_id)
rows = n//N
cols = m//N

res = [[ray.get(z[i][j]) for j in range(cols)] for i in range(rows)]

print("Time: ",time.time()-t)
ray.shutdown()