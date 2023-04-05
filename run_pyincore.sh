#!/bin/sh

# Activate pyincore environment
source /root/miniconda3/etc/profile.d/conda.sh
conda activate pyincore

echo "Hello World"

# Assumes a volume will get mounted in /tmp so results can be persisted
cp dw_pyincore.py input_definition.json /data

# Working directory
cd /data/

# Run script with all parameters passed from the command line
./dw_pyincore.py "$@"
