"""Output naming, archiving, and retention helpers."""
from __future__ import annotations

import datetime
import glob
import os
import re
import shutil
import zipfile

try:
    from . import config_loader, utils
except ImportError:
    import config_loader
    import utils

logger = utils.setup_logging(__name__)
WEATHER_MODEL_OUTPUT_PREFIXES = ("NOMADS-", "PASTCAST-")


def format_start_label(start_time):
    """Return the UTC run identity label used in local output names."""
    return start_time.strftime("%Y%m%d_%H%M")


def build_output_dir_name(domain_key, start_time, run_label, model):
    """Build a collision-resistant runtime/temp directory name."""
    return f"{domain_key}_{format_start_label(start_time)}_{run_label}_{model}"


def build_archive_name_base(domain_key, start_time, run_label, model):
    """Build a collision-resistant archive/playable KMZ base name."""
    return f"{domain_key}_{run_label}_{model}_{format_start_label(start_time)}"


def build_domain_average_output_dir_name(domain_key: str, start_label: str) -> str:
    """Build a collision-resistant domain-average runtime/temp directory name."""
    return f"{domain_key}_domavg_{start_label}"


def build_domain_average_archive_name(
    domain_key: str,
    start_label: str,
    speed: float,
    speed_units: str,
    direction: float,
) -> str:
    """Build a collision-resistant domain-average archive name."""
    return (
        f"{domain_key}_domavg_{start_label}_"
        f"{int(speed)}{speed_units}_{int(direction)}deg"
    )


def sanitize_label(value: str | None, fallback: str = "external") -> str:
    """Return a filesystem-safe label used in generated run names."""
    raw = (value or fallback).strip()
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", raw).strip("._-")
    return safe or fallback


def build_grid_output_dir_name(domain_key: str, start_time, label: str | None) -> str:
    """Build the runtime/temp directory name for one gridded-forcing run."""
    return f"{domain_key}_{format_start_label(start_time)}_grid_{sanitize_label(label)}"


def build_grid_archive_name(domain_key: str, label: str | None, start_time) -> str:
    """Build the archive/playable KMZ base name for one gridded-forcing run."""
    return f"{domain_key}_grid_{sanitize_label(label)}_{format_start_label(start_time)}"


def _is_weather_model_output(filename: str) -> bool:
    return filename.startswith(WEATHER_MODEL_OUTPUT_PREFIXES)


def _rename_without_collision(old: str, new: str) -> None:
    if os.path.abspath(old) == os.path.abspath(new):
        return
    if os.path.exists(new):
        raise FileExistsError(f"Refusing to overwrite existing output: {new}")
    os.rename(old, new)


def rename_reanalysis_outputs(output_dir, domain_key):
    """Normalize WindNinja reanalysis outputs to <domain>_YYYYMMDD_HHMM_*.ext."""
    vel_files = glob.glob(os.path.join(output_dir, "*_vel.asc"))
    for fpath in vel_files:
        fname = os.path.basename(fpath)
        if _is_weather_model_output(fname):
            continue
        match = re.search(r"(\d{4})(\d{2})(\d{2})[-_]?(\d{2})(\d{2})", fname)
        if not match:
            continue
        ymd_hm = (f"{match.group(1)}{match.group(2)}{match.group(3)}"
                  f"_{match.group(4)}{match.group(5)}")
        base_old = fpath.replace("_vel.asc", "")
        base_new = os.path.join(output_dir, f"{domain_key}_{ymd_hm}")
        for ext in ("_vel.asc", "_ang.asc", "_vel.prj", "_ang.prj",
                    "_vel.asc.aux.xml", "_80m.kmz"):
            old = base_old + ext
            if os.path.exists(old):
                _rename_without_collision(old, base_new + ext)

    for kpath in glob.glob(os.path.join(output_dir, "*.kmz")):
        kname = os.path.basename(kpath)
        if _is_weather_model_output(kname):
            continue
        match = re.search(r"(\d{4})(\d{2})(\d{2})[-_]?(\d{2})(\d{2})", kname)
        if match:
            ts = (f"{match.group(1)}{match.group(2)}{match.group(3)}"
                  f"_{match.group(4)}{match.group(5)}")
            new_kmz = f"{domain_key}_{ts}_80m.kmz"
            _rename_without_collision(kpath, os.path.join(output_dir, new_kmz))


def archive_results(run_output_dir, archive_name_base):
    """Zip retained run outputs into runtime/archives/ and clean up."""
    utils.ensure_dir(config_loader.ARCHIVE_DIR)
    archive_path = os.path.join(config_loader.ARCHIVE_DIR, f"{archive_name_base}.zip")

    grids_dir = os.path.join(run_output_dir, "grids")
    if os.path.exists(grids_dir):
        shutil.rmtree(grids_dir)

    logger.info(f"Archiving to {archive_path}")
    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _dirs, files in os.walk(run_output_dir):
            for fname in files:
                full = os.path.join(root, fname)
                zf.write(full, os.path.relpath(full, run_output_dir))

    shutil.rmtree(run_output_dir)
    return archive_path


def enforce_retention(days=7):
    """Delete local archives older than *days*."""
    now = datetime.datetime.now()
    cutoff = datetime.timedelta(days=days)
    if not os.path.exists(config_loader.ARCHIVE_DIR):
        return
    for fname in os.listdir(config_loader.ARCHIVE_DIR):
        fpath = os.path.join(config_loader.ARCHIVE_DIR, fname)
        if os.path.isfile(fpath):
            age = now - datetime.datetime.fromtimestamp(os.path.getmtime(fpath))
            if age > cutoff:
                try:
                    os.remove(fpath)
                    logger.info(f"Deleted old archive: {fname}")
                except OSError as exc:
                    logger.error(f"Could not delete {fname}: {exc}")
