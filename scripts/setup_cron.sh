#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ -f "$BASE_DIR/config/runtime.env" ]; then
  set -a
  . "$BASE_DIR/config/runtime.env"
  set +a
fi

resolve_path() {
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    *) printf '%s\n' "$BASE_DIR/$1" ;;
  esac
}

RUNTIME_ROOT="$(resolve_path "${MWN_RUNTIME_ROOT:-runtime}")"
LOG_DIR="$RUNTIME_ROOT/logs"
RUN_SCRIPT="$BASE_DIR/scripts/run_cron.sh"
CLEANUP_SCRIPT="$BASE_DIR/scripts/maintenance_cleanup.sh"

mkdir -p "$LOG_DIR"

crontab -l > "$BASE_DIR/crontab.bak" 2>/dev/null || true

cat > "$BASE_DIR/new_crontab" <<EOF
# Mountain WindNinja cron jobs
15 * * * * $RUN_SCRIPT
0 11 * * * $CLEANUP_SCRIPT >> $LOG_DIR/cron_cleanup.log 2>&1
EOF

crontab "$BASE_DIR/new_crontab"

echo "Crontab updated successfully. Previous crontab backed up to $BASE_DIR/crontab.bak"
crontab -l
