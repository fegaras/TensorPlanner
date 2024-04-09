#!/bin/bash
#SBATCH --job-name="tp_factorization"
#SBATCH --output=tp_mvapich_factorization_%j.out

echo "TensorPlanner Factorization Job"
nodes=$SLURM_NNODES
echo "Number of nodes = " $nodes
module purge
module load slurm cpu/0.17.3b gcc/10.2.0/npcyll4 mvapich2/2.3.7/iyjtn3x
export EXP_HOME="$(pwd -P)"

rm -rf classes
mkdir -p classes
file="factorization_tp.diablo"
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
d=$3
iterations=$4
srun --mpi=pmi2 -n $SLURM_NTASKS -c $SLURM_CPUS_PER_TASK ./a.out $n $m $d $iterations
