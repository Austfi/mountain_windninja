#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="/opt/mountain_windninja"
LOG_DIR="$REPO_DIR/runtime/logs"

mkdir -p "$LOG_DIR"
touch "$LOG_DIR/cron_daemon.log"

crontab "$REPO_DIR/docker/forecast.crontab"

echo "Installed crontab:"
crontab -l

exec cron -f
