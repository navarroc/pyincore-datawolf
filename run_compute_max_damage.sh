#!/bin/bash

# Activate pyincore environment
. /tmp/virtualenv/pyincore/bin/activate

# Run script with all parameters passed from the command line
./compute_max_damage.py "$@"
