#!/bin/bash
set -euo pipefail
umask 002

# ---- prevent overlapping runs ----
LOCKFILE=/tmp/windninja.lock
exec 9>"$LOCKFILE" || exit 1
flock -n 9 || exit 0

# ---- explicit cron-safe environment ----
export HOME=/home/austin_finnell
export PATH=/home/austin_finnell/bin:/usr/local/bin:/usr/bin:/bin:/home/austin_finnell/.local/bin
export PYTHONPATH=/home/austin_finnell/keystone_automation

cd /home/austin_finnell/keystone_automation

# ---- OpenFOAM environment (guard for strict shell) ----
export ZSH_NAME=""
set +u
source /opt/openfoam9/etc/bashrc
set -u

# HARD disable renumberMesh (WindNinja/OpenFOAM instability)
export PATH=$(echo "$PATH" | sed 's#[^:]*renumberMesh[^:]*:##g')

# ---- logging ----
{
  echo "Starting Cron Run: $(date -u)"
  /usr/bin/python3 scripts/hourly_run.py
  echo "Finished Cron Run: $(date -u)"
  echo "---------------------------------------------------"
} >> logs/cron_combined.log 2>&1
