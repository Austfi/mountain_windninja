#!/usr/bin/env python3
"""Hourly cron entry point.

Runs an 18-hour HRRR forecast via daily_run.py, then updates the
GCS bucket index if uploads are enabled.  Called by run_cron.sh.
"""
import subprocess
import sys
import os

import config_loader
import utils
from gcs_manager import manager as gcs_manager

logger = utils.setup_logging(__name__)


def run_daily_for_cycle(mode: str, model: str, hours: int, dry_run: bool = False) -> bool:
    run_script = str(config_loader.SCRIPTS_DIR / "run_windninja.sh")
    cmd = [
        run_script,
        "--mode", mode,
        "--model", model,
        "--hours", str(hours),
    ]
    if dry_run:
        cmd.append("--dry-run")

    logger.info(f"Running daily_run: {mode} {model} ({hours}h)")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        logger.error(f"daily_run failed:\n{result.stderr}")
        return False
    if result.stdout:
        logger.info(result.stdout.rstrip())
    return True


def main():
    dry_run = "--dry-run" in sys.argv
    mode = os.getenv("MWN_SCHEDULE_MODE", "forecast").strip() or "forecast"
    model = os.getenv("MWN_SCHEDULE_MODEL", "HRRR").strip() or "HRRR"
    hours_raw = os.getenv("MWN_SCHEDULE_HOURS", "18").strip() or "18"

    try:
        hours = int(hours_raw)
    except ValueError:
        logger.error(f"MWN_SCHEDULE_HOURS must be an integer, got: {hours_raw!r}")
        return 1
    if hours < 1:
        logger.error(f"MWN_SCHEDULE_HOURS must be >= 1, got: {hours}")
        return 1

    logger.info("Starting scheduled WindNinja run...")
    if not run_daily_for_cycle(mode, model, hours, dry_run=dry_run):
        return 1

    if not dry_run and config_loader.GCS_UPLOAD_ENABLED:
        gcs_manager.update_index()
    return 0


if __name__ == "__main__":
    sys.exit(main())
