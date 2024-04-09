#!/bin/bash
#SBATCH --job-name="sac_pagerank"
#SBATCH --output="sac_pagerank_%j.out"

echo "SAC Pagerank Job"
nodes=$SLURM_NNODES
echo "Number of nodes = " $nodes

# Expanse node: 128 cores (126 available), 256 GB RAM
#   executor-cores = 12   (10 executors/node)
#   executor-memory = 24GB
#   num-executors = nodes*10-1
executors=$((nodes*10-1))
echo "Number of executors = " $executors

SPARK_OPTIONS="--driver-memory 24G --num-executors $executors --executor-cores 12 --executor-memory 24G --driver-java-options '-Xss512m' --supervise"

export HADOOP_CONF_DIR=$HOME/expansecluster
module load cpu/0.15.4 gcc/7.5.0 openjdk hadoop/3.2.2 spark

SW=/expanse/lustre/projects/uot166/fegaras

export EXP_HOME="$(pwd -P)"

# location of data storage and scratch space on every worker (on local SSD)
scratch=/scratch/$USER/job_$SLURM_JOB_ID

myhadoop-configure.sh -s $scratch

# spark configuration generated by myhadoop
SPARK_ENV=$HADOOP_CONF_DIR/spark/spark-env.sh
echo "export TMP=$scratch/tmp" >> $SPARK_ENV
echo "export TMPDIR=$scratch/tmp" >> $SPARK_ENV
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

rm -rf classes
mkdir -p classes

f="pagerank_sac.scala"
echo compiling $f ...
scalac -d classes -cp classes:${JARS}:${DIABLO_HOME}/lib/diablo.jar ${EXP_HOME}/src/$f >/dev/null

jar cf sac.jar -C classes .
n=$1
iterations=$2
echo "n: $n, iterations: $iterations"
spark-submit --jars ${DIABLO_HOME}/lib/diablo.jar --class PageRank --master $MASTER $SPARK_OPTIONS sac.jar 4 $n $iterations

stop-dfs.sh
myhadoop-cleanup.sh
