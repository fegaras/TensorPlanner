#!/bin/bash

SW=/expanse/lustre/projects/uot166/fegaras
export DIABLO_HOME="$(cd `dirname $0`/../..; pwd -P)"
export SCALA_HOME=$SW/scala-2.12.3
export SPARK_HOME=$SW/spark-3.1.2-bin-hadoop3.2

JARS=${DIABLO_HOME}/lib/diablo.jar
for I in ${SPARK_HOME}/jars/*.jar; do
    JARS=${JARS}:$I
done
export JARS=${JARS}

# Create python virtual environment
#python3 -m venv venv
python_env="$HOME/venv"
source "$python_env/bin/activate"
export PATH="$python_env/bin:$SCALA_HOME/bin:$DIABLO_HOME/bin:$PATH"

export ACCOUNT_NAME=uot166
export N_NODES=5
export N_TASKS_PER_NODE=2
export N_CPUS_PER_TASK=64
export TIME_LIMIT=60
export MEM_LIMIT=249208M
