# STAG-MLIR (TensorPlanner extension)

### Installation

STAG-MLIR depends on MPI 5, JDK 11, Spark 3, Scala 2.12, sbt 1.6.2, CUDA 12, NVIDA HPC SDK, and MLIR.

* Install either
open-mpi 5.0 with ucx from [https://www.open-mpi.org/software/](https://www.open-mpi.org/software/) or
MVAPICH2 2.3 from [https://mvapich.cse.ohio-state.edu/downloads/](https://mvapich.cse.ohio-state.edu/downloads/).
* Install Scala 2.12 from [https://www.scala-lang.org/download/2.12.19.html](https://www.scala-lang.org/download/2.12.19.html).
* Install sbt from [https://www.scala-sbt.org/download/](https://www.scala-sbt.org/download/).
* Install Apache Spark 3 from [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html).
* Install CUDA Toolkit 12.8 from [https://developer.nvidia.com/cuda-12-8-2-download-archive](https://developer.nvidia.com/cuda-12-8-2-download-archive).
* Install NVIDIA HPC SDK 25.3 from [https://developer.nvidia.com/nvidia-hpc-sdk-253-downloads](https://developer.nvidia.com/nvidia-hpc-sdk-253-downloads)
* Install MLIR from [https://mlir.llvm.org/getting_started/](https://mlir.llvm.org/getting_started/)

Edit the file `setup.sh` to point to your installation directories.
For open-mpi, do:
```bash
source setup.sh
```
for MVAPICH2, do:
```bash
mvapich=y source setup.sh
```
Compile STAG-MLIR using:
```bash
sbt package
make ptx-hpc
```

To test it on UTA (or on any SLURM-based cluster), build the system:
```bash
sbatch hpc_build.run
```
Edit `bin/tp` and `utils/extract_gpu_kernel.py` with correct chip_type (sm_80/sm_90 and cc80/cc90).
Go to `tests`, edit `hpc_experiment.run`, and do:
```bash
sbatch hpc_experiment.run
```
