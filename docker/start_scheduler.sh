#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="/opt/mountain_windninja"
LOG_DIR="$REPO_DIR/runtime/logs"
SCHEDULE_MINUTE="${MWN_SCHEDULE_MINUTE:-15}"

mkdir -p "$LOG_DIR"
touch "$LOG_DIR/cron_daemon.log"

if ! [[ "$SCHEDULE_MINUTE" =~ ^[0-9]+$ ]] || [ "$SCHEDULE_MINUTE" -gt 59 ]; then
  echo "MWN_SCHEDULE_MINUTE must be an integer from 0 to 59." >&2
  exit 1
fi

CRONTAB_FILE="/tmp/mountain_windninja.crontab"
cat > "$CRONTAB_FILE" <<EOF
SHELL=/bin/bash
PATH=/opt/venv/bin:/usr/local/bin:/usr/bin:/bin

# Mountain WindNinja container cron jobs
${SCHEDULE_MINUTE} * * * * cd /opt/mountain_windninja && ./scripts/run_cron.sh >> /opt/mountain_windninja/runtime/logs/cron_combined.log 2>&1
0 11 * * * cd /opt/mountain_windninja && ./scripts/maintenance_cleanup.sh >> /opt/mountain_windninja/runtime/logs/cron_cleanup.log 2>&1
EOF

crontab "$CRONTAB_FILE"

echo "Installed crontab:"
crontab -l

exec cron -f
