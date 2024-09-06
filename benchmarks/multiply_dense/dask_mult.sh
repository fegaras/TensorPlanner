#!/bin/bash
#SBATCH --job-name="dask_mult"
#SBATCH --output="dask_multiply_%j.out"

source ../env_setup.sh
echo "Dask matrix multiplication job"
nodes=$SLURM_NNODES
echo "Number of nodes = " $nodes

##########################
# Load required modules
##########################
module purge
module load $TP_OPENMPI_MODULES
source "$PYTHON_ENV/bin/activate" # Activate the virtual environment

export EXP_HOME="$(pwd -P)"
n=$1
m=$2
iterations=$3
echo "n: $n, m: $m, iterations: $iterations"
mpirun -np $SLURM_NTASKS python3 $EXP_HOME/src/Multiply_Dask.py $n $m $iterations
