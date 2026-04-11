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
ARCHIVE_DIR="$RUNTIME_ROOT/archives"
TEMP_DIR="$RUNTIME_ROOT/temp"
GRIB_CACHE="$TEMP_DIR/grib_cache"

mkdir -p "$ARCHIVE_DIR" "$TEMP_DIR" "$GRIB_CACHE"

echo "=== Starting Cleanup: $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="

echo "Cleaning archive files older than 7 days..."
find "$ARCHIVE_DIR" -name "*.zip" -type f -mtime +7 -print -delete

echo "Cleaning temp run directories older than 1 day..."
find "$TEMP_DIR" -mindepth 1 -maxdepth 1 -type d -not -name "grib_cache" -mtime +1 -print -exec rm -rf {} +

echo "Pruning GRIB cache older than 2 days..."
find "$GRIB_CACHE" -name "*.grib2" -type f -mtime +2 -print -delete

echo "Current Disk Usage:"
du -sh "$RUNTIME_ROOT" "$TEMP_DIR" "$ARCHIVE_DIR"
echo "=== Cleanup Completed ==="
