##########################
# Create python virtual environment
##########################
export PYTHON_ENV="$HOME/venv"
python3 -m venv $PYTHON_ENV

source "$PYTHON_ENV/bin/activate" # Activate the virtual environment

##########################
# Install dependencies
##########################
pip install -r requirements.txt
