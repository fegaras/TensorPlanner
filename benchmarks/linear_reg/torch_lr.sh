#!/bin/bash
#SBATCH --job-name="torch_lr"
#SBATCH --output=torch_lr_%j.out

source ../env_setup.sh
echo "PyTorch Linear Regression Job"
nodes=$SLURM_NNODES
echo "Number of nodes = " $nodes

node_list=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
nodes_array=($node_list)

head_node=${nodes_array[0]}
MASTER_ADDR=$(srun --nodes=1 --ntasks=1 -w "$head_node" hostname --ip-addr)

MASTER_PORT=29400
MASTER_IP=$MASTER_ADDR:$MASTER_PORT
export MASTER_IP
echo "Node IP: $MASTER_IP"

source "$PYTHON_ENV/bin/activate" # Activate the virtual environment

export LOGLEVEL=ERROR

export EXP_HOME="$(pwd -P)"
export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
n=$1
m=$2
iterations=$3
echo "n: $n, m: $m, iterations: $iterations"
srun torchrun --nnodes $N_NODES --nproc_per_node $N_TASKS_PER_NODE \
--rdzv_id=100 --rdzv_backend=c10d --rdzv_endpoint=$MASTER_IP \
$EXP_HOME/src/lr_torch.py $n $m $iterations