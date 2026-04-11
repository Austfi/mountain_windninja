#!/usr/bin/env python3
"""Hourly cron entry point.

Runs an 18-hour HRRR forecast via daily_run.py, then updates the
GCS bucket index if uploads are enabled.  Called by run_cron.sh.
"""
import subprocess
import sys

import config_loader
import utils
from gcs_manager import manager as gcs_manager

logger = utils.setup_logging(__name__)


def run_daily_for_cycle(mode: str, model: str, dry_run: bool = False) -> bool:
    run_script = str(config_loader.SCRIPTS_DIR / "run_windninja.sh")
    cmd = [
        run_script,
        "--mode", mode,
        "--model", model,
        "--hours", "18",
    ]
    if dry_run:
        cmd.append("--dry-run")

    logger.info(f"Running daily_run: {mode} {model} (18h)")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        logger.error(f"daily_run failed:\n{result.stderr}")
        return False
    if result.stdout:
        logger.info(result.stdout.rstrip())
    return True


def main():
    dry_run = "--dry-run" in sys.argv

    logger.info("Starting scheduled forecast run...")
    run_daily_for_cycle("forecast", "HRRR", dry_run=dry_run)

    if not dry_run and config_loader.GCS_UPLOAD_ENABLED:
        gcs_manager.update_index()


if __name__ == "__main__":
    main()
