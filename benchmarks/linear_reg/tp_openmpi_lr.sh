#!/bin/bash
#SBATCH --job-name="tp_lr"
#SBATCH --output=tp_openmpi_lr_%j.out

echo "TensorPlanner Linear Regression Job"
nodes=$SLURM_NNODES
echo "Number of nodes = " $nodes

module purge
module load slurm cpu/0.17.3b  gcc/10.2.0/npcyll4 openmpi/4.1.3
export EXP_HOME="$(pwd -P)"

rm -rf classes
mkdir -p classes
file="lr_tp.diablo"
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
