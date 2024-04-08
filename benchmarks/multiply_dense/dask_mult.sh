#!/bin/bash
#SBATCH --job-name="dask_mult"
#SBATCH --output="dask_multiply_%j.out"

nodes=$SLURM_NNODES
echo "Number of nodes = " $nodes
module purge
module load slurm cpu/0.17.3b  gcc/10.2.0 openmpi/4.1.3

#python3 -m pip install "dask[complete]"
#pip install dask_mpi --upgrade

export EXP_HOME="$(pwd -P)"
n=$1
iters=$2
mpirun -np $SLURM_NTASKS python3 $EXP_HOME/src/Multiply_Dask.py $n $iters
