#/bin/bash

export DIABLO_HOME=${HOME}/TensorPlanner

export JAVA_HOME=/usr/lib/jvm/java-1.11.0-openjdk-amd64

export SCALA_HOME=${HOME}/system/scala-2.12.15

export SPARK_HOME=${HOME}/spark-3.2.1-bin-hadoop3.2

if [ -v mvapich ]; then
    export mvapich
    # install MVAPICH2 2.3.7 from https://mvapich.cse.ohio-state.edu/downloads/
    export MPI_HOME=${HOME}/mvapich
else
    unset mvapich
    # install open-mpi from https://www.open-mpi.org/software/
    export MPI_HOME=${HOME}/openmpi
fi

JARS=${DIABLO_HOME}/lib/diablo.jar
for I in core sql; do
    JARS=${JARS}:`ls ${SPARK_HOME}/jars/spark-${I}*.jar`
done
export JARS=${JARS}

export PATH="$SCALA_HOME/bin:$MPI_HOME/bin:$DIABLO_HOME/bin:$SPARK_HOME/bin:$PATH"

export LD_LIBRARY_PATH="$MPI_HOME/lib:$LD_LIBRARY_PATH"
