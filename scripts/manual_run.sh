#!/usr/bin/env bash
# Run a full forecast + reanalysis manually.
# Useful for initial testing or manual catch-up runs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ -f "$BASE_DIR/config/runtime.env" ]; then
  set -a
  . "$BASE_DIR/config/runtime.env"
  set +a
fi

PYTHON_BIN="${MWN_PYTHON_BIN:-$BASE_DIR/.venv/bin/python}"
cd "$BASE_DIR"

echo "=== 18-hour HRRR Forecast ==="
"$PYTHON_BIN" scripts/daily_run.py --mode forecast --hours 18 --model HRRR

echo "=== 12-hour HRRR Reanalysis ==="
"$PYTHON_BIN" scripts/daily_run.py --mode reanalysis --hours 12 --model HRRR

echo "=== Done ==="
