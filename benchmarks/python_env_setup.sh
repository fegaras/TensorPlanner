##########################
# Create python virtual environment
##########################
export PYTHON_ENV="$HOME/venv"
python3 -m venv $PYTHON_ENV

source "$PYTHON_ENV/bin/activate" # Activate the virtual environment
# upgrade pip
$PYTHON_ENV/bin/python3 -m pip install --upgrade pip

##########################
# Install dependencies
##########################
pip install -r requirements.txt
# install PyTorch CPU: https://pytorch.org/
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu
# Install Ray: https://docs.ray.io/en/latest/ray-overview/installation.html
pip install -U "ray[default]"
# Install Dask: https://docs.dask.org/en/stable/install.html
pip install "dask[complete]"
