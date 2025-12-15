#!/usr/bin/env python3
"""Hourly automation script for WindNinja HRRR forecasts and reanalysis.

This script is intended to be run every hour (e.g., via cron). It:
1. Determines the most recent HRRR cycle based on the current UTC time.
2. Executes the existing `daily_run.py` script for a single hour window.
3. Packages the generated KMZ files into zip archives.
4. Uploads the KMZ files and zip archives to Google Cloud Storage using `gcs_manager`.
5. Triggers an update of the bucket index (which now includes a KML network link).
"""

import subprocess
import json
import datetime
import pathlib
import shutil
import sys

import utils
import config_loader
from gcs_manager import manager as gcs_manager

logger = utils.setup_logging(__name__)

def get_current_cycle() -> datetime.datetime:
    """Return the most recent HRRR cycle (UTC hour) for which data is expected.
    HRRR cycles are hourly. We use the current UTC hour as the cycle.
    """
    now = datetime.datetime.utcnow()
    # Round down to the start of the hour
    cycle = now.replace(minute=0, second=0, microsecond=0)
    return cycle

def run_daily_for_cycle(mode: str, model: str, dry_run: bool = False) -> bool:
    """Invoke `daily_run.py` with standard 18h window.

    Parameters
    ----------
    mode: str
        Either "forecast" or "reanalysis".
    model: str
        Model name, e.g. "HRRR".
    dry_run: bool
        If True, pass ``--dry-run`` to the script.
    """
    cmd = [
        "python3",
        "scripts/daily_run.py",
        "--mode",
        mode,
        "--model",
        model,
        "--hours",
        "18"
    ]
    if dry_run:
        cmd.append("--dry-run")
        
    logger.info(f"Running daily_run for {mode} {model} (18h)")
    result = subprocess.run(cmd, capture_output=True, text=True)
    
    # Log output regardless of success for debugging
    if result.stdout:
        logger.info(f"daily_run stdout: {result.stdout}")
    if result.stderr:
        logger.warning(f"daily_run stderr: {result.stderr}")
        
    if result.returncode != 0:
        logger.error(f"daily_run failed with code {result.returncode}")
        return False
        
    logger.info("daily_run completed successfully")
    return True

def main():
    dry_run = "--dry-run" in sys.argv
    
    # Run Forecast only (Reanalysis disabled due to NOMADS data retention limits)
    logger.info("Starting scheduled Forecast run...")
    run_daily_for_cycle("forecast", "HRRR", dry_run=dry_run)
    
    # Ensure Index is up to date
    if not dry_run:
        gcs_manager.update_index()

if __name__ == "__main__":
    main()
