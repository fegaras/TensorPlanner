#!/bin/bash
#SBATCH --job-name="dask_mult"
#SBATCH --output="dask_multiply_%j.out"

nodes=$SLURM_NNODES
echo "Number of nodes = " $nodes

##########################
# Load required modules
##########################
module purge
module load slurm cpu/0.17.3b  gcc/10.2.0 openmpi/4.1.3

export EXP_HOME="$(pwd -P)"
n=$1
m=$2
iters=$3
mpirun -np $SLURM_NTASKS python3 $EXP_HOME/src/Multiply_Dask.py $n $m $iters
