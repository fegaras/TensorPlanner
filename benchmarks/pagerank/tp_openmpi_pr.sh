#!/bin/bash
#SBATCH --job-name="tp_pagerank"
#SBATCH --output=tp_openmpi_pagerank_%j.out

source ../env_setup.sh
echo "TensorPlanner Pagerank Job"
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
file="pagerank_tp.diablo"
echo compiling $file ...
tp ${EXP_HOME}/src/$file
echo "Compilation done..."

ulimit -l unlimited
ulimit -s unlimited

export collect=false
export trace=false

SOCKETS=2
n=$1
iterations=$2
echo "n: $n, iterations: $iterations"
# for each expanse node: 2 sockets, 1 executor per socket, 64 threads per executor
mpirun -N $SOCKETS --bind-to socket a.out $n $iterations
