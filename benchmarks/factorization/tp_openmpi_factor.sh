#!/bin/bash
#SBATCH --job-name="tp_factorization"
#SBATCH --output=tp_openmpi_factorization_%j.out

source ../env_setup.sh
echo "TensorPlanner Factorization Job"
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
file="factorization_tp.diablo"
echo compiling $file ...
tp ${EXP_HOME}/src/$file
echo "Compilation done..."

ulimit -l unlimited
ulimit -s unlimited

export collect=false
##########################
# Set trace=true to print generated plan, schedules and execution logs
##########################
export trace=false

SOCKETS=2
n=$1
m=$2
d=$3
iterations=$4
echo "n: $n, m: $m, d: $d, iterations: $iterations"
# for each expanse node: 2 sockets, 1 executor per socket, 64 threads per executor
mpirun -N $SOCKETS --bind-to socket a.out $n $m $d $iterations
