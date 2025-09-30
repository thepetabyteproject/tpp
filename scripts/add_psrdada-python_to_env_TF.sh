#!/bin/sh

# This script will add the PSRDADA-Python package to an existing Conda environement. 
# It must be run while the desired environment is activated. 
# It will download and compile the source code.

# Loads prerequisites assuming Thorny Flat modulefiles (use alternate methods to get CUDA and PSRDADA on other systems)
module load parallel/cuda/12.3 astronomy/psrdada/2024-08-19

# Clone the repo for PSRDADA-Python; switch to branch with latest fixes to prevent compiler error
git clone https://github.com/TRASAL/psrdada-python.git
cd psrdada-python; git checkout issue7

# Install requirements
echo "pip install"
pip install --user -r requirements.txt

# Compile the package--double check that the tests pass
echo "make"
make && make test && make install

cd ..
