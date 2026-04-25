#/bin/bash

export DIABLO_HOME=${HOME}/TensorPlanner
export JAVA_HOME=${HOME}/java/jdk-11.0.2
export SCALA_HOME=${HOME}/scala-2.12.12
export SPARK_HOME=${HOME}/spark-3.1.2-bin-hadoop3.2
export CUDA_HOME=${HOME}/cuda_home
export NVHPC_HOME=${HOME}/nvhpc_home/Linux_x86_64/25.3

if [ "$mvapich" == "y" ]; then
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

export NVHPC_COMPILER=${NVHPC_HOME}/compilers
export PATH="$SCALA_HOME/bin:$MPI_HOME/bin:$NVHPC_COMPILER/bin:$DIABLO_HOME/bin:$SPARK_HOME/bin:$JAVA_HOME/bin:$CUDA_HOME/bin:$PATH"

export LD_LIBRARY_PATH="$MPI_HOME/lib:$NVHPC_COMPILER/lib:$CUDA_HOME/lib64:$LD_LIBRARY_PATH"

source $HOME/.venv/bin/activate

export use_GPU=true
export block_size=4096
export trace=false
