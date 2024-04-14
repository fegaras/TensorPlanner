#!/bin/bash
#SBATCH --job-name="tp_lr"
#SBATCH --output=tp_mvapich_lr_%j.out

echo "TensorPlanner Linear Regression Job"
nodes=$SLURM_NNODES
echo "Number of nodes = " $nodes

##########################
# Load required modules
##########################
module purge
module load $TP_MVAPICH2_MODULES

export EXP_HOME="$(pwd -P)"
rm -rf classes
mkdir -p classes
file="lr_tp.diablo"
echo compiling $file ...
tp ${EXP_HOME}/src/$file
echo "Compilation done..."

ulimit -l unlimited
ulimit -s unlimited

export MV2_SMP_USE_CMA=0
export MV2_USE_RDMA_CM=0
export MV2_HOMOGENEOUS_CLUSTER=1
export MV2_ENABLE_AFFINITY=0
export MV2_USE_ALIGNED_ALLOC=1
export MV2_CPU_BINDING_LEVEL=socket

export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
export collect=false
export trace=false

n=$1
m=$2
iterations=$3
echo "n: $n, m: $m, iterations: $iterations"
srun --mpi=pmi2 -n $SLURM_NTASKS -c $SLURM_CPUS_PER_TASK ./a.out $n $m $iterations
