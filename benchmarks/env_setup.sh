#!/bin/bash

SW=/expanse/lustre/projects/uot166/fegaras
##########################
# Set TensorPlanner root directory as TP_HOME
##########################
export TP_HOME="$HOME/TensorPlanner"

##########################
# Point to Scala and Spark installations
##########################
# Install Scala 2.12: https://www.scala-lang.org/download/all.html 
export SCALA_HOME=$SW/scala-2.12.3
# Install Spark 3: https://spark.apache.org/downloads.html
export SPARK_HOME=$SW/spark-3.1.2-bin-hadoop3.2

JARS=${TP_HOME}/lib/diablo.jar
for I in ${SPARK_HOME}/jars/*.jar; do
    JARS=${JARS}:$I
done
export JARS=${JARS}

##########################
# Required modules (modify based on available modules on cluster)
##########################
# Load GCC and OpenMPI 4.1
export TP_OPENMPI_MODULES="slurm cpu/0.17.3b gcc/10.2.0/npcyll4 openmpi/4.1.3"
# Load GCC and MVAPICH2 2.3
export TP_MVAPICH2_MODULES="slurm cpu/0.17.3b gcc/10.2.0/npcyll4 mvapich2/2.3.7/iyjtn3x"
# Load JDK, Spark and hadoop
export SPARK_MODULES="slurm cpu/0.15.4 gcc/7.5.0 openjdk hadoop/3.2.2 spark"
# Load ScaLAPACK modules
export ScaLAPACK_MODULES="slurm cpu/0.17.3b  gcc/10.2.0 openmpi/4.1.3 intel-mkl/2020.4.304"

export PYTHON_ENV="$HOME/venv"
source "$PYTHON_ENV/bin/activate" # Activate the virtual environment

# Add libraries to Path
export PATH="$PYTHON_ENV/bin:$SCALA_HOME/bin:$TP_HOME/bin:$PATH"

##########################
# If uisng MVAPICH2 set use_mvapich="y", otherwise, set use_mvapich="n"
##########################
export use_mvapich="y"

##########################
# Set up SLURM job configuration
##########################
export ACCOUNT_NAME=uot166
export N_NODES=5
export N_TASKS_PER_NODE=2
export N_CPUS_PER_TASK=64
export TIME_LIMIT=60 # in minutes
export MEM_LIMIT=249208M
