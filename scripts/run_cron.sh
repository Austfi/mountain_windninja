#!/usr/bin/env bash
set -euo pipefail
umask 002

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
LOCKFILE="${MWN_LOCKFILE:-/tmp/mountain_windninja.lock}"
PYTHON_BIN="${MWN_PYTHON_BIN:-$BASE_DIR/.venv/bin/python}"
OPENFOAM_BASHRC="${MWN_OPENFOAM_BASHRC:-/opt/openfoam9/etc/bashrc}"

mkdir -p "$LOG_DIR"

exec 9>"$LOCKFILE" || exit 1
flock -n 9 || exit 0

export PATH="/usr/local/bin:/usr/bin:/bin:${PATH:-}"
cd "$BASE_DIR"

if [ -f "$OPENFOAM_BASHRC" ]; then
  export ZSH_NAME=""
  set +u
  # shellcheck disable=SC1090
  source "$OPENFOAM_BASHRC"
  set -u
fi

{
  echo "Starting cron forecast run: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  "$PYTHON_BIN" scripts/hourly_run.py
  echo "Finished cron forecast run: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "---------------------------------------------------"
} >> "$LOG_DIR/cron_combined.log" 2>&1
