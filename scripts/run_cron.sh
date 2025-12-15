#!/bin/bash
# Wrapper script for Keystone Automation Cron Jobs
# Ensures full environment (OpenFOAM, Python, PATH) is set up correctly.

source /opt/openfoam9/etc/bashrc

export PATH=$PATH:/home/austin_finnell/.local/bin
export PYTHONPATH=$PYTHONPATH:/home/austin_finnell/keystone_automation

cd /home/austin_finnell/keystone_automation

# Log start
echo "Starting Cron Run: $(date)" >> logs/cron_combined.log

# Run the hourly automation script
# usage: python3 scripts/hourly_run.py
/usr/bin/python3 scripts/hourly_run.py >> logs/cron_combined.log 2>&1

echo "Finished Cron Run: $(date)" >> logs/cron_combined.log
echo "---------------------------------------------------" >> logs/cron_combined.log
