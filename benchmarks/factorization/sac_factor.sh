#!/bin/bash
#SBATCH --job-name="sac_factorization"
#SBATCH --output="sac_factorization_%j.out"

source ../env_setup.sh
echo "SAC Factorization Job"
nodes=$SLURM_NNODES
echo "Number of nodes = " $nodes

# Expanse node: 128 cores (126 available), 256 GB RAM
#   executor-cores = 12   (10 executors/node)
#   executor-memory = 24GB
#   num-executors = nodes*10-1
executors=$((nodes*10-1))
echo "Number of executors = " $executors

SPARK_OPTIONS="--driver-memory $SPARK_DRIVER_MEM --num-executors $executors --executor-cores $SPARK_EXECUTOR_N_CORES --executor-memory $SPARK_EXECUTOR_MEM --driver-java-options '-Xss512m' --supervise"

export HADOOP_CONF_DIR=$HOME/expansecluster

##########################
# Load required modules
##########################
module purge
module load $SPARK_MODULES

# location of data storage and scratch space on every worker (on local SSD)
scratch=/scratch/$USER/job_$SLURM_JOB_ID

myhadoop-configure.sh -s $scratch

# spark configuration generated by myhadoop
SPARK_ENV=$HADOOP_CONF_DIR/spark/spark-env.sh
echo "export TMP=$scratch/tmp_sac" >> $SPARK_ENV
echo "export TMPDIR=$scratch/tmp_sac" >> $SPARK_ENV
echo "export SPARK_LOCAL_DIRS=$scratch" >> $SPARK_ENV
source $SPARK_ENV

export SPARK_MASTER_HOST=$SPARK_MASTER_IP

# start HDFS
start-dfs.sh
# start Spark
myspark start

JARS=.
for I in `ls $SPARK_HOME/jars/*.jar -I *unsafe*`; do
    JARS=$JARS:$I
done

export EXP_HOME="$(pwd -P)"
rm -rf classes
mkdir -p classes

f="factorization_sac.scala"
echo compiling $f ...
scalac -d classes -cp classes:${JARS}:${TP_HOME}/lib/diablo.jar ${EXP_HOME}/src/$f >/dev/null

jar cf sac.jar -C classes .
n=$1
m=$2
d=$3
iterations=$4
echo "n: $n, m: $m, d: $d, iterations: $iterations"
spark-submit --jars ${TP_HOME}/lib/diablo.jar --class Factorization --master $MASTER $SPARK_OPTIONS sac.jar 4 $n $m $d $iterations

stop-dfs.sh
myhadoop-cleanup.sh
