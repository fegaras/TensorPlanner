#!/bin/bash
#SBATCH --job-name="ray_mult_openmp"
#SBATCH --output="ray_openmp_multiply_%j.out"

module purge
module load slurm cpu/0.17.3b  gcc/10.2.0 openmpi/4.1.3
export EXP_HOME="$(pwd -P)"

node_count=$SLURM_NNODES
echo "Number of nodes = " $node_count
nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
nodes_array=($nodes)

head_node=${nodes_array[0]}
export head_node_ip=$(srun --nodes=1 --ntasks=1 -w "$head_node" hostname --ip-addr)

port=6379
ip_head=$head_node_ip:$port
export ip_head
echo "IP Head: $ip_head"

#pip install -U "ray[default]"
#pip install numpy

export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
echo "Starting HEAD at $head_node"
srun --nodes=1 --ntasks=1 -w "$head_node" ray start --head --node-ip-address="$head_node_ip" --port=$port --block &
sleep 10

worker_num=$((SLURM_JOB_NUM_NODES - 1))

for((i=1;i<=worker_num; i++)); do
	node_i=${nodes_array[$i]}
	echo "Starting Worker $i at $node_i"
	srun --nodes=1 --ntasks=1 -w "$node_i" ray start --address "$ip_head" --block &
	sleep 5
done

n=$1
iters=$2
echo "n: $n, m: $n, iterations: $iters"
python3 $EXP_HOME/src/Multiply_Ray_Openmp.py $n $iters
