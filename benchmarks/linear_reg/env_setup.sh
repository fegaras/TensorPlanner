#!/bin/bash

SW=/expanse/lustre/projects/uot166/fegaras
export DIABLO_HOME="$(cd `dirname $0`/../..; pwd -P)"
export SCALA_HOME=$SW/scala-2.12.3

export JARS=${DIABLO_HOME}/lib/diablo.jar

# Create python virtual environment
#python3 -m venv venv
python_env="$HOME/venv"
source "$python_env/bin/activate"
# install PyTorch
#pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu
# install pandas
#pip install pandas
export PATH="$python_env/bin:$SCALA_HOME/bin:$DIABLO_HOME/bin:$PATH"

export ACCOUNT_NAME=uot166
export N_NODES=5
export N_TASKS_PER_NODE=2
export N_CPUS_PER_TASK=64
export TIME_LIMIT=60
export MEM_LIMIT=249208M
