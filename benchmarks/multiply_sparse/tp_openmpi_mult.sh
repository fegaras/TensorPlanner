#!/bin/bash
#SBATCH --job-name="tp_mult"
#SBATCH --output=tp_openmpi_multiply_%j.out

echo "TensorPlanner Multiply Job"
nodes=$SLURM_NNODES
echo "Number of nodes = " $nodes

##########################
# Load required modules
##########################
module purge
module load slurm cpu/0.17.3b  gcc/10.2.0/npcyll4 openmpi/4.1.3

export EXP_HOME="$(pwd -P)"
rm -rf classes
mkdir -p classes
file="mult_sp_tp.diablo"
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
