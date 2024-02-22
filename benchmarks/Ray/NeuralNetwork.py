import torch
import ray
import os
import sys
import time
import math

num_cpus, n, m, reps = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
lrate = 0.5
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
def elem_multiply_blocks(x,y):
	return torch.mul(x,y)

@ray.remote
def multiply_blocks(x,y):
	return torch.matmul(x,y)

@ray.remote
def apply_blocks(x,func):
	return x.apply_(func)

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
def apply_matrices(x,func):
	rows = len(x)
	cols = len(x[0])
	return [[apply_blocks.remote(x[i][j],func) for j in range(cols)] for i in range(rows)]

@ray.remote
def elem_multiply_matrices(x,y):
	rows = len(x)
	cols = len(x[0])
	return [[elem_multiply_blocks.remote(x[i][j],y[i][j]) for j in range(cols)] for i in range(rows)]

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

def sigmoid(x):
    return 1.0/(1.0+math.exp(-x))

def sigmoid_der(x):
    s = sigmoid(x)
    return s*(1-s)

def get_matrix(x_id):
    x_val = ray.get(x_id)
    x = [[ray.get(x_val[i][j]) for j in range(len(x_val[0]))] for i in range(len(x_val))]
    return x

X_id = create_matrix.remote(n,m)
Y_id = create_matrix.remote(n,1)

layer = N
w1 = create_matrix.remote(m,layer)
w2 = create_matrix.remote(layer,1)
Z_arr1 = create_matrix.remote(n,m)
Z_arr2 = create_matrix.remote(n,1)
A_arr1 = create_matrix.remote(n,m)
A_arr2 = create_matrix.remote(n,m)
t=time.time()
iter = 0
while(iter < reps):
	A_curr = X_id
	A_arr1 = A_curr
	Z_arr1 = multiply_matrices.remote(A_curr, w1)
	A_curr = apply_matrices.remote(Z_arr1,lambda a: max(0.0,a))
	
	A_arr2 = A_curr
	Z_arr2 = multiply_matrices.remote(A_curr, w2)
	Y1 = apply_matrices.remote(Z_arr2,sigmoid)

	dA_prev1 = sub_matrices.remote(Y_id,Y1)
	dA2 = apply_matrices.remote(dA_prev1,sigmoid_der)
	dZ2 = elem_multiply_matrices.remote(dA2, Z_arr2)
	dW_curr2 = multiply_matrices.remote(A_arr2, dZ2, True, False)
	dA_prev2 = multiply_matrices.remote(dZ2, w2, False, True)
	w2 = sub_matrices.remote(w2,dW_curr2,alpha=lrate)
	dA1 = dA_prev2
	dZ1 = elem_multiply_matrices.remote(dA1, apply_matrices.remote(Z_arr1,lambda a: 0 if(a <= 0) else 1))
	dW_curr1 = multiply_matrices.remote(A_arr1, dZ1, True, False)
	w1 = sub_matrices.remote(w1,dW_curr1,alpha=lrate)

	iter = iter + 1

get_matrix(w1)
get_matrix(w2)
print("Time: ",time.time()-t)
ray.shutdown()