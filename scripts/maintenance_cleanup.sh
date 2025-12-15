#!/bin/bash
# Maintenance script to clean up old artifacts and temporary files.

BASE_DIR="/home/austin_finnell/keystone_automation"
ARCHIVE_DIR="$BASE_DIR/archives"
TEMP_DIR="$BASE_DIR/temp"
GRIB_CACHE="$TEMP_DIR/grib_cache"

echo "=== Starting Cleanup: $(date) ==="

# 1. Archive Retention:
# - Forecasts: Keep 3 days
echo "Cleaning Forecast Archives (older than 3 days)..."
find "$ARCHIVE_DIR" -name "*forecast*.zip" -type f -mtime +3 -print -delete

# - Reanalysis: Keep 7 days
echo "Cleaning Reanalysis Archives (older than 7 days)..."
find "$ARCHIVE_DIR" -name "*reanalysis*.zip" -type f -mtime +7 -print -delete

# 2. Temp Directory: Remove run directories older than 1 day
# Be careful not to delete grib_cache or the temp dir itself
echo "Cleaning Temp Run Directories (older than 1 day)..."
find "$TEMP_DIR" -mindepth 1 -maxdepth 1 -type d -not -name "grib_cache" -mtime +1 -print -exec rm -rf {} +

# 3. GRIB Cache: Remove GRIB files older than 2 days
echo "Pruning GRIB Cache (older than 2 days)..."
find "$GRIB_CACHE" -name "*.grib2" -type f -mtime +2 -print -delete

# 4. Check Disk Usage
echo "Current Disk Usage:"
du -sh "$BASE_DIR"
du -sh "$TEMP_DIR"
du -sh "$ARCHIVE_DIR"

echo "=== Cleanup Completed ==="
