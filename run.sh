#!/bin/bash

# Activate pyincore environment
#. /tmp/virtualenv/pyincore/bin/activate
eval "$(conda shell.bash hook)"
conda activate pyincore

# Run script with all parameters passed from the command line
./dw_pyincore.py "$@"
