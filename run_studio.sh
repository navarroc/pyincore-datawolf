#!/bin/sh

# Activate pyincore environment
source /root/miniconda3/etc/profile.d/conda.sh
conda activate pyincore

SCRIPT_DIR=$(dirname $0)

# Run script with all parameters passed from the command line
$SCRIPT_DIR/dw_pyincore_studio.py "$@"
