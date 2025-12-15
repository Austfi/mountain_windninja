#!/bin/bash
# Manual trigger to update both Forecast and Reanalysis "latest" files.

# Ensure we are in the correct directory
cd /home/austin_finnell/keystone_automation

# 1. Full Forecast (Updates HRRR_latest.kmz)
echo "=========================================="
echo "Starting Manual Full Forecast Update (HRRR)"
echo "Target: gs://.../latest/HRRR_latest.kmz"
echo "=========================================="
python3 scripts/daily_run.py --mode full_forecast --hours 18 --model HRRR

# 2. Reanalysis (Updates HRRR_reanalysis_latest.kmz)
echo "=========================================="
echo "Starting Manual Reanalysis Update (HRRR)"
echo "Target: gs://.../latest/HRRR_reanalysis_latest.kmz"
echo "=========================================="
# Reanalysis typically covers the last 12-24 hours. Defaulting to 12 as per daily run.
python3 scripts/daily_run.py --mode reanalysis --hours 12 --model HRRR

echo "=========================================="
echo "All Manual Updates Completed."
echo "=========================================="
