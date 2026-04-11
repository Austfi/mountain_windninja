#!/usr/bin/env python3
"""Run a WindNinja simulation and produce output files.

This is the main script that:
  1. Reads a domain template (.cfg) and fills in date/time/paths.
  2. Runs WindNinja_cli to produce KMZ and ASCII output.
  3. Bundles hourly KMZs into a single playable KMZ.
  4. Archives results as a zip.
  5. Optionally uploads to Google Cloud Storage.

Usage:
  python daily_run.py --mode forecast --model HRRR --hours 18
  python daily_run.py --mode reanalysis --model HRRR --hours 12
  python daily_run.py --mode domain-average --speed 15 --direction 225
  python daily_run.py --mode forecast --hours 6 --keep-temp
  python daily_run.py --mode forecast --hours 18 --no-upload --dry-run
"""
import argparse
import datetime
import glob
import os
import re
import shutil
import subprocess
import sys
import zipfile

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import config_loader
import create_time_series
import utils
from gcs_manager import manager as gcs

logger = utils.setup_logging("daily_run")

# ---------------------------------------------------------------------------
# Weather model name mapping
# ---------------------------------------------------------------------------
FORECAST_MODEL_MAP = {
    "HRRR": "NOMADS-HRRR-CONUS-3-KM",
    "NBM": "NOMADS-NBM-CONUS-2.5-KM",
    "NAM": "NOMADS-NAM-NEST-CONUS-3-KM",
    "NAM-CONUS": "NOMADS-NAM-CONUS-12-KM",
    "NAM-ALASKA": "NOMADS-NAM-ALASKA-11.25-KM",
    "RAP": "NOMADS-RAP-CONUS-13-KM",
    "GFS": "NOMADS-GFS-GLOBAL-0.25-DEG",
}

PASTCAST_MODEL_MAP = {
    "HRRR": "PASTCAST-GCP-HRRR-CONUS-3-KM",
}

ALL_MODEL_NAMES = sorted(set(list(FORECAST_MODEL_MAP) + list(PASTCAST_MODEL_MAP)))


def resolve_weather_model(model: str, run_type: str) -> str:
    if run_type == "forecast":
        return FORECAST_MODEL_MAP[model]
    if run_type == "reanalysis":
        if model not in PASTCAST_MODEL_MAP:
            raise ValueError(
                f"Reanalysis only supports: {', '.join(sorted(PASTCAST_MODEL_MAP))}."
            )
        return PASTCAST_MODEL_MAP[model]
    raise ValueError(f"Unsupported run type: {run_type}")


# ---------------------------------------------------------------------------
# Config generation
# ---------------------------------------------------------------------------
def generate_config(date_str, start_time, stop_time, domain_config,
                    wx_model_type_override=None, surface_vegetation=None,
                    sub_dir=None, output_wind_height=10.0):
    """Read a template .cfg, fill placeholders, write a ready-to-run config."""
    run_output_dir = sub_dir or os.path.join(config_loader.TEMP_DIR, date_str)
    utils.ensure_dir(run_output_dir)

    with open(str(domain_config.template_path), "r") as f:
        template = f.read()

    duration = max(1, int((stop_time - start_time).total_seconds() / 3600))

    filled = template.format(
        start_year=start_time.year, start_month=start_time.month,
        start_day=start_time.day, start_hour=start_time.hour,
        start_minute=start_time.minute,
        stop_year=stop_time.year, stop_month=stop_time.month,
        stop_day=stop_time.day, stop_hour=stop_time.hour,
        stop_minute=stop_time.minute,
        forecast_duration=duration,
        elevation_file=domain_config.elevation_file.as_posix(),
        output_wind_height=output_wind_height,
    )

    lines = filled.split("\n")
    out_lines = []
    found_vegetation = False

    for line in lines:
        stripped = line.strip()

        if stripped.startswith("output_path"):
            continue

        if wx_model_type_override and stripped.startswith("wx_model_type"):
            out_lines.append(f"wx_model_type = {wx_model_type_override}")
            continue

        if surface_vegetation and stripped.startswith("vegetation"):
            out_lines.append(f"vegetation = {surface_vegetation}")
            found_vegetation = True
            continue

        out_lines.append(line)

    if (surface_vegetation and surface_vegetation != "none"
            and domain_config.elevation_file.suffix.lower() != ".lcp"
            and not found_vegetation):
        out_lines.append(f"vegetation = {surface_vegetation}")

    out_lines.append(f"output_path = {run_output_dir}")

    config_path = os.path.join(
        run_output_dir,
        f"{domain_config.key}_{start_time.strftime('%Y%m%d_%H%M')}.cfg",
    )
    with open(config_path, "w") as f:
        f.write("\n".join(out_lines))

    return config_path, run_output_dir


def generate_domain_average_config(domain_config, wind_speed, wind_direction,
                                   speed_units="mph", surface_vegetation=None,
                                   sub_dir=None, output_wind_height=10.0):
    """Generate a domain-average config (single uniform wind, no wx download).

    This mirrors the desktop app's "Domain Average" initialization: specify one
    wind speed and direction, and WindNinja distributes it across the terrain.
    """
    now_utc = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    date_str = now_utc.strftime("%Y%m%d_%H%M")
    run_output_dir = sub_dir or os.path.join(config_loader.TEMP_DIR,
                                             f"domavg_{date_str}")
    utils.ensure_dir(run_output_dir)

    vegetation = surface_vegetation or config_loader.SURFACE_VEGETATION
    use_lcp = domain_config.elevation_file.suffix.lower() == ".lcp"

    lines = [
        f"num_threads = {os.cpu_count() or 4}",
        f"elevation_file = {domain_config.elevation_file.as_posix()}",
        "",
        "initialization_method = domainAverageInitialization",
        f"input_speed = {wind_speed}",
        f"input_speed_units = {speed_units}",
        f"input_direction = {wind_direction}",
        "input_wind_height = 10.0",
        "units_input_wind_height = m",
        "",
    ]

    if not use_lcp and vegetation and vegetation != "none":
        lines.append(f"vegetation = {vegetation}")

    lines += [
        "diurnal_winds = false",
        "",
        f"year  = {now_utc.year}",
        f"month = {now_utc.month}",
        f"day   = {now_utc.day}",
        f"hour  = {now_utc.hour}",
        f"minute = {now_utc.minute}",
        "time_zone = UTC",
        "",
        "mesh_resolution = 80.0",
        "units_mesh_resolution = m",
        "",
        "momentum_flag = true",
        "number_of_iterations = 300",
        "",
        f"output_wind_height = {output_wind_height}",
        "units_output_wind_height = m",
        f"output_speed_units = {speed_units}",
        "",
        "write_goog_output = true",
        "goog_out_use_consistent_color_scale = false",
        "units_goog_out_resolution = m",
        "",
        "write_ascii_output = true",
        "ascii_out_resolution = -1",
        "units_ascii_out_resolution = m",
        "",
        f"output_path = {run_output_dir}",
    ]

    config_path = os.path.join(
        run_output_dir, f"{domain_config.key}_domavg_{date_str}.cfg",
    )
    with open(config_path, "w") as f:
        f.write("\n".join(lines))

    return config_path, run_output_dir


# ---------------------------------------------------------------------------
# Run WindNinja
# ---------------------------------------------------------------------------
def run_windninja(config_path):
    """Invoke WindNinja_cli, cleaning up any stale NINJAFOAM case first."""
    config_basename = os.path.splitext(os.path.basename(config_path))[0]
    case_dir = config_loader.STATIC_DATA_DIR / f"NINJAFOAM_{config_basename}"
    if case_dir.exists():
        logger.warning(f"Removing stale case directory: {case_dir}")
        shutil.rmtree(case_dir)

    cmd = [config_loader.WINDNINJA_CLI, config_path]
    logger.info(f"Running: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"WindNinja failed: {e}")
        raise


# ---------------------------------------------------------------------------
# Post-processing
# ---------------------------------------------------------------------------
def rename_reanalysis_outputs(output_dir, domain_key):
    """Normalize reanalysis file names to <domain>_YYYYMMDD_HHMM_*.ext."""
    vel_files = glob.glob(os.path.join(output_dir, "*_vel.asc"))
    for fpath in vel_files:
        fname = os.path.basename(fpath)
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
                try:
                    os.rename(old, base_new + ext)
                except OSError:
                    pass

    for kpath in glob.glob(os.path.join(output_dir, "*.kmz")):
        kname = os.path.basename(kpath)
        m = re.search(r"(\d{4})(\d{2})(\d{2})[-_]?(\d{2})(\d{2})", kname)
        if m:
            ts = f"{m.group(2)}-{m.group(3)}-{m.group(1)}_{m.group(4)}{m.group(5)}"
            new_kmz = f"{domain_key}_{ts}_80m.kmz"
            try:
                os.rename(kpath, os.path.join(output_dir, new_kmz))
            except OSError:
                pass


def archive_results(run_output_dir, archive_name_base):
    """Zip KMZ files from the run into runtime/archives/ and clean up."""
    utils.ensure_dir(config_loader.ARCHIVE_DIR)
    archive_path = os.path.join(config_loader.ARCHIVE_DIR, f"{archive_name_base}.zip")

    grids_dir = os.path.join(run_output_dir, "grids")
    if os.path.exists(grids_dir):
        shutil.rmtree(grids_dir)

    logger.info(f"Archiving to {archive_path}")
    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _dirs, files in os.walk(run_output_dir):
            for fname in files:
                if fname.endswith(".kmz"):
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
                except OSError as e:
                    logger.error(f"Could not delete {fname}: {e}")


# ---------------------------------------------------------------------------
# Run parameters
# ---------------------------------------------------------------------------
def get_run_parameters(mode, hours):
    now_utc = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    start = now_utc.replace(minute=0, second=0, microsecond=0)

    if mode == "forecast":
        return {
            "start": start,
            "stop": start + datetime.timedelta(hours=hours),
            "label": f"forecast_{hours}h",
            "type": "forecast",
        }
    elif mode == "reanalysis":
        stop = start
        return {
            "start": stop - datetime.timedelta(hours=hours),
            "stop": stop,
            "label": f"reanalysis_{hours}h",
            "type": "reanalysis",
        }
    else:
        raise ValueError(f"Unknown mode: {mode}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    config_loader.init_directories()
    available = config_loader.list_domains()

    parser = argparse.ArgumentParser(
        description="Run a WindNinja simulation."
    )
    parser.add_argument("--mode", choices=["forecast", "reanalysis", "domain-average"],
                        default="forecast")
    parser.add_argument("--hours", type=int, default=18,
                        help="Forecast/reanalysis window in hours (default: 18)")
    parser.add_argument("--domain", choices=available,
                        default=config_loader.DEFAULT_DOMAIN)
    parser.add_argument("--model", choices=ALL_MODEL_NAMES,
                        default="HRRR")
    parser.add_argument("--speed", type=float, default=None,
                        help="Wind speed for domain-average mode (in --speed-units)")
    parser.add_argument("--direction", type=float, default=None,
                        help="Wind direction in degrees for domain-average mode (0=N, 90=E, 180=S, 270=W)")
    parser.add_argument("--speed-units", default="mph",
                        choices=["mph", "mps", "kph", "kts"],
                        help="Units for --speed (default: mph)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Generate config but skip WindNinja execution")
    parser.add_argument("--keep-temp", action="store_true",
                        help="Keep output files instead of archiving")
    parser.add_argument("--no-upload", action="store_true",
                        help="Skip GCS upload even if enabled")
    parser.add_argument("--height", type=float, default=10.0,
                        help="Output wind height in meters above ground (default: 10)")
    args = parser.parse_args()

    domain_config = config_loader.get_domain_config(args.domain)
    logger.info(f"Domain: {domain_config.label}")

    do_upload = (config_loader.GCS_UPLOAD_ENABLED
                 and not args.dry_run and not args.no_upload)

    # --- Domain-average mode (no weather download, single wind) ---
    if args.mode == "domain-average":
        if args.speed is None or args.direction is None:
            parser.error("--speed and --direction are required for domain-average mode")

        logger.info(f"Mode: DOMAIN-AVERAGE | "
                     f"Speed: {args.speed} {args.speed_units} | "
                     f"Direction: {args.direction}°")

        try:
            config_path, output_dir = generate_domain_average_config(
                domain_config, args.speed, args.direction,
                speed_units=args.speed_units,
                surface_vegetation=config_loader.SURFACE_VEGETATION,
                output_wind_height=args.height,
            )

            if not args.dry_run:
                run_windninja(config_path)

                output_name = (f"{domain_config.key}_domavg_"
                               f"{int(args.speed)}{args.speed_units}_"
                               f"{int(args.direction)}deg")

                if not args.keep_temp:
                    archive_results(output_dir, output_name)

            enforce_retention()
            logger.info("Done.")

        except Exception as e:
            logger.error(f"FAILED: {e}")
            raise
        return

    # --- Forecast / Reanalysis mode ---
    run_params = get_run_parameters(args.mode, args.hours)

    try:
        wx_model = resolve_weather_model(args.model, run_params["type"])
    except ValueError as exc:
        parser.error(str(exc))

    date_str = run_params["start"].strftime("%Y%m%d")
    output_dir = os.path.join(
        config_loader.TEMP_DIR,
        f"{date_str}_{run_params['label']}_{args.model}",
    )

    logger.info(f"Mode: {args.mode.upper()} | Model: {args.model} ({wx_model})")
    logger.info(f"Window: {run_params['start']} -> {run_params['stop']} UTC")

    if do_upload:
        gcs.upload_status(run_params["label"], args.model, "running")

    try:
        utils.ensure_dir(output_dir)

        config_path, _ = generate_config(
            date_str, run_params["start"], run_params["stop"],
            domain_config,
            wx_model_type_override=wx_model,
            surface_vegetation=config_loader.SURFACE_VEGETATION,
            sub_dir=output_dir,
            output_wind_height=args.height,
        )

        if not args.dry_run:
            run_windninja(config_path)

        if run_params["type"] == "reanalysis":
            rename_reanalysis_outputs(output_dir, domain_config.key)

        if not args.dry_run:
            output_name = (f"{domain_config.key}_{run_params['label']}"
                           f"_{args.model}_{date_str}")

            playable_kmz = None
            try:
                playable_kmz = create_time_series.create_playable_kmz(
                    output_dir, output_name,
                    domain_label=domain_config.label)
                if playable_kmz:
                    logger.info(f"Playable KMZ: {playable_kmz}")
            except Exception as e:
                logger.error(f"Playable KMZ failed: {e}")

            if do_upload and playable_kmz and os.path.exists(playable_kmz):
                latest_name = ("latest_reanalysis.kmz"
                               if "reanalysis" in run_params["label"]
                               else "latest_forecast.kmz")
                gcs.upload_file(playable_kmz, latest_name,
                                cache_control="public, max-age=60")

            if not args.keep_temp:
                archive_path = archive_results(output_dir, output_name)
                if do_upload and os.path.exists(archive_path):
                    dest = f"archives/{date_str}/{os.path.basename(archive_path)}"
                    gcs.upload_file(archive_path, dest)
                    gcs.cleanup_old_forecasts()

            if do_upload:
                gcs.upload_status(run_params["label"], args.model, "success")
                gcs.update_index()

            enforce_retention()

        logger.info("Done.")

    except Exception as e:
        logger.error(f"FAILED: {e}")
        if do_upload:
            gcs.upload_status(run_params["label"], args.model, "failure",
                              error=str(e))
        raise


if __name__ == "__main__":
    main()
