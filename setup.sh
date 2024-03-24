#/bin/bash

export DIABLO_HOME=${HOME}/TensorPlanner

export JAVA_HOME=/usr/lib/jvm/java-1.11.0-openjdk-amd64

export SCALA_HOME=${HOME}/system/scala-2.12.15

export SPARK_HOME=${HOME}/spark-3.2.1-bin-hadoop3.2

if [ -v mvapich ]; then
    # install MVAPICH2 2.3.7 from https://mvapich.cse.ohio-state.edu/downloads/
    export MPI_HOME=${HOME}/mvapich 
else
    # install open-mpi from https://www.open-mpi.org/software/
    export MPI_HOME=${HOME}/openmpi
fi

JARS=.:${DIABLO_HOME}/lib/diablo.jar:${DIABLO_HOME}/lib/mpi.jar
for I in ${SPARK_HOME}/jars/*.jar; do
    JARS=${JARS}:$I
done
for I in ${SPARK_HOME}/lib/*.jar; do
    JARS=${JARS}:$I
done
export JARS=${JARS}

export PATH="$SCALA_HOME/bin:$MPI_HOME/bin:$DIABLO_HOME/bin:$SPARK_HOME/bin:$PATH"
export LD_LIBRARY_PATH="$MPI_HOME/lib:/usr/local/lib:$LD_LIBRARY_PATH"
