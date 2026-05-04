"""WindNinja process execution helpers."""
from __future__ import annotations

import os
import shutil
import subprocess

try:
    from . import config_loader, utils
except ImportError:
    import config_loader
    import utils

logger = utils.setup_logging(__name__)


def build_windninja_env(run_type="forecast"):
    env = os.environ.copy()
    if run_type != "reanalysis":
        return env

    gcs_auth_vars = (
        "GS_SECRET_ACCESS_KEY",
        "GS_ACCESS_KEY_ID",
        "GS_OAUTH2_PRIVATE_KEY_FILE",
        "GS_OAUTH2_CLIENT_EMAIL",
        "GS_OAUTH2_REFRESH_TOKEN",
        "GOOGLE_APPLICATION_CREDENTIALS",
    )
    has_gcs_auth = any(env.get(name) for name in gcs_auth_vars)
    if not has_gcs_auth and not env.get("GS_NO_SIGN_REQUEST"):
        env["GS_NO_SIGN_REQUEST"] = "YES"
    return env


def run_windninja(config_path, run_type="forecast"):
    """Invoke WindNinja_cli, cleaning up any stale NINJAFOAM case first."""
    config_basename = os.path.splitext(os.path.basename(config_path))[0]
    case_dir = config_loader.STATIC_DATA_DIR / f"NINJAFOAM_{config_basename}"
    if case_dir.exists():
        logger.warning(f"Removing stale case directory: {case_dir}")
        shutil.rmtree(case_dir)

    cmd = [config_loader.WINDNINJA_CLI, config_path]
    env = build_windninja_env(run_type)
    if run_type == "reanalysis" and env.get("GS_NO_SIGN_REQUEST") == "YES":
        logger.info("Using unsigned GCS access for public HRRR pastcast data.")
    logger.info(f"Running: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True, env=env)
    except subprocess.CalledProcessError as exc:
        logger.error(f"WindNinja failed: {exc}")
        raise
