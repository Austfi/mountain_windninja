#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ -f "$BASE_DIR/config/runtime.env" ]; then
  set -a
  . "$BASE_DIR/config/runtime.env"
  set +a
fi

OPENFOAM_BASHRC="${MWN_OPENFOAM_BASHRC:-/opt/openfoam9/etc/bashrc}"
PYTHON_BIN="${MWN_PYTHON_BIN:-$BASE_DIR/.venv/bin/python}"

if [ -f "$OPENFOAM_BASHRC" ]; then
  set +u
  # shellcheck disable=SC1090
  source "$OPENFOAM_BASHRC"
  set -u
  export FOAM_USER_LIBBIN=/usr/local/lib/
fi

exec "$PYTHON_BIN" "$BASE_DIR/scripts/daily_run.py" "$@"
