#!/bin/bash
#SBATCH --job-name="tp_nn"
#SBATCH --output=tp_openmpi_nn_%j.out

source ../env_setup.sh
echo "TensorPlanner Neural Network Job"
nodes=$SLURM_NNODES
echo "Number of nodes = " $nodes

##########################
# Load required modules
##########################
module purge
module load $TP_OPENMPI_MODULES

export EXP_HOME="$(pwd -P)"
rm -rf classes
mkdir -p classes
file="nn_tp.diablo"
echo compiling $file ...
tp ${EXP_HOME}/src/$file
echo "Compilation done..."

ulimit -l unlimited
ulimit -s unlimited

export collect=false
export trace=false

SOCKETS=2
n=$1
m=$2
iterations=$3
echo "n: $n, m: $m, iterations: $iterations"
# for each expanse node: 2 sockets, 1 executor per socket, 64 threads per executor
mpirun -N $SOCKETS --bind-to socket a.out $n $m $iterations
