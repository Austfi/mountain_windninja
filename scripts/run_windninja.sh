#!/bin/bash
# Wrapper script to run WindNinja with OpenFOAM environment
# This sources OpenFOAM before running daily_run.py

# Source OpenFOAM 9 environment
source /opt/openfoam9/etc/bashrc

# Run the Python script with all arguments passed through
/usr/bin/python3 /home/austin_finnell/keystone_automation/scripts/daily_run.py "$@"
