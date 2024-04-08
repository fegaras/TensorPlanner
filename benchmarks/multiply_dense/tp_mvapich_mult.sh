#!/bin/bash
#SBATCH --job-name="tp_mult"
#SBATCH --output="tp_mvapich_n_($N_NODES)_%j.out"

echo "TensorPlanner Multiply Job"
module purge
module load slurm cpu/0.17.3b gcc/10.2.0/npcyll4 mvapich2/2.3.7/iyjtn3x
export EXP_HOME="$(pwd -P)"

rm -rf classes
mkdir -p classes
file="mult_tp.diablo"
echo compiling $file ...
diablo ${EXP_HOME}/src/$file

ulimit -l unlimited
ulimit -s unlimited

export MV2_SMP_USE_CMA=0
export MV2_USE_RDMA_CM=0
export MV2_HOMOGENEOUS_CLUSTER=1
export MV2_ENABLE_AFFINITY=0
export MV2_USE_ALIGNED_ALLOC=1
export MV2_CPU_BINDING_LEVEL=socket

export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
export diablo_collect=false
export diablo_trace=false

n=$1
iterations=$2
srun --mpi=pmi2 -n $SLURM_NTASKS -c $SLURM_CPUS_PER_TASK ./a.out $n $iterations