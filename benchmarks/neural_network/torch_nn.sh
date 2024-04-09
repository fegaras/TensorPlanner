#!/bin/bash

echo "PyTorch Neural Network Job"
nodes=$SLURM_NNODES
echo "Number of nodes = " $nodes

module purge
module load slurm cpu/0.17.3b

export MASTER_ADDR=$(scontrol show hostname ${SLURM_NODELIST} | head -n 1)
echo Node IP: $MASTER_ADDR
export LOGLEVEL=ERROR

export EXP_HOME="$(pwd -P)"

n=$1
m=$2
iterations=$3
num_classes=$4

srun torchrun --nnodes $N_NODES --nproc_per_node $N_TASKS_PER_NODE \
--rdzv_id=100 --rdzv_backend=c10d --rdzv_endpoint=$MASTER_ADDR:29400 \
$EXP_HOME/src/nn_torch.py $n $m $iterations $num_classes
