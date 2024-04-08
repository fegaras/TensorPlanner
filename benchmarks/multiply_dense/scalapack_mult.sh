#!/bin/bash
#SBATCH --job-name="scalapack_mult"
#SBATCH --output="scalapack_multiply_%j.out"

echo "Scalapack Multiply Job"
nodes=$SLURM_NNODES
echo "Number of nodes = " $nodes

module purge
module load slurm cpu/0.17.3b  gcc/10.2.0 openmpi/4.1.3 intel-mkl/2020.4.304
export OMP_NUM_THREADS=64
export EXP_HOME="$(pwd -P)"

mpicxx -fopenmp -o $EXP_HOME/sc_mult $EXP_HOME/src/mult_scalapack.cpp  \
    -I$INTEL_MKLHOME/mkl/include \
    -m64  ${MKLROOT}/lib/intel64/libmkl_scalapack_lp64.a -Wl,--start-group ${MKLROOT}/lib/intel64/libmkl_intel_lp64.a \
    ${MKLROOT}/lib/intel64/libmkl_intel_thread.a ${MKLROOT}/lib/intel64/libmkl_core.a \
    ${MKLROOT}/lib/intel64/libmkl_blacs_openmpi_lp64.a -Wl,--end-group -liomp5 -lpthread -lm -ldl

echo "Compilation done..."
n=$1
srun -n $SLURM_NTASKS $EXP_HOME/sc_mult $n
