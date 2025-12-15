#!/bin/bash
# Installs crontab entries for Keystone Automation
# Assumes system time is UTC. Adjust times if necessary for Mountain Time (UTC-7/6).

BASE_DIR="/home/austin_finnell/keystone_automation"
LOG_DIR="$BASE_DIR/logs"
mkdir -p "$LOG_DIR"

# Backup existing cron
crontab -l > "$BASE_DIR/crontab.bak" 2>/dev/null

# Define Cron Jobs
# Define common script paths
RUN_SCRIPT="cd $BASE_DIR && python3 scripts/daily_run.py"
CLEANUP_SCRIPT="$BASE_DIR/scripts/maintenance_cleanup.sh"

# Create new crontab file
echo "# Keystone Automation Cron Jobs" > "$BASE_DIR/new_crontab"

# 1. Hourly Forecast (Runs every hour at minute 15)
# Starts +15 min past hour to allow for some model data availability (or use prev cycle)
echo "15 * * * * $RUN_SCRIPT --mode forecast --hours 18 --model HRRR >> $LOG_DIR/cron_forecast.log 2>&1" >> "$BASE_DIR/new_crontab"

# 2. Hourly Reanalysis (Runs every hour at minute 45)
# Covers past 12 hours.
echo "45 * * * * $RUN_SCRIPT --mode reanalysis --hours 12 --model HRRR >> $LOG_DIR/cron_reanalysis.log 2>&1" >> "$BASE_DIR/new_crontab"

# 3. Daily Maintenance / Cleanup (04:00 MST -> 11:00 UTC)
echo "0 11 * * * $CLEANUP_SCRIPT >> $LOG_DIR/cron_cleanup.log 2>&1" >> "$BASE_DIR/new_crontab"

# Install
crontab "$BASE_DIR/new_crontab"

echo "Crontab updated successfully. Previous crontab backed up to crontab.bak"
crontab -l
