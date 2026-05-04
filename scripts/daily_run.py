#!/usr/bin/env python3
"""Run a WindNinja simulation and produce output files."""
from __future__ import annotations

import argparse
import datetime
import os
from pathlib import Path

try:
    from . import config_loader, create_time_series, utils
    from .archive_manager import (
        archive_results,
        build_archive_name_base,
        build_domain_average_archive_name,
        build_domain_average_output_dir_name,
        build_grid_archive_name,
        build_grid_output_dir_name,
        build_output_dir_name,
        enforce_retention,
        format_start_label,
        rename_reanalysis_outputs,
    )
    from .gcs_manager import manager as gcs
    from .weather_models import (
        ALL_MODEL_NAMES,
        FORECAST_MODEL_MAP,
        PASTCAST_MODEL_MAP,
        resolve_weather_model,
    )
    from .windninja_config import (
        generate_config,
        generate_domain_average_config,
        generate_gridded_config,
        template_momentum_enabled,
    )
    from .windninja_runner import build_windninja_env, run_windninja
except ImportError:
    import config_loader
    import create_time_series
    import utils
    from archive_manager import (
        archive_results,
        build_archive_name_base,
        build_domain_average_archive_name,
        build_domain_average_output_dir_name,
        build_grid_archive_name,
        build_grid_output_dir_name,
        build_output_dir_name,
        enforce_retention,
        format_start_label,
        rename_reanalysis_outputs,
    )
    from gcs_manager import manager as gcs
    from weather_models import (
        ALL_MODEL_NAMES,
        FORECAST_MODEL_MAP,
        PASTCAST_MODEL_MAP,
        resolve_weather_model,
    )
    from windninja_config import (
        generate_config,
        generate_domain_average_config,
        generate_gridded_config,
        template_momentum_enabled,
    )
    from windninja_runner import build_windninja_env, run_windninja

__all__ = [
    "ALL_MODEL_NAMES",
    "FORECAST_MODEL_MAP",
    "PASTCAST_MODEL_MAP",
    "archive_results",
    "build_archive_name_base",
    "build_domain_average_archive_name",
    "build_domain_average_output_dir_name",
    "build_grid_archive_name",
    "build_grid_output_dir_name",
    "build_output_dir_name",
    "build_run_parameters",
    "build_windninja_env",
    "enforce_retention",
    "format_start_label",
    "generate_config",
    "generate_domain_average_config",
    "generate_gridded_config",
    "get_run_parameters",
    "parse_utc_timestamp",
    "rename_reanalysis_outputs",
    "resolve_weather_model",
    "run_windninja",
    "template_momentum_enabled",
]

logger = utils.setup_logging("daily_run")


def get_run_parameters(mode, hours):
    if hours < 1:
        raise ValueError("--hours must be >= 1.")

    now_utc = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    start = now_utc.replace(minute=0, second=0, microsecond=0)

    if mode == "forecast":
        return {
            "start": start,
            "stop": start + datetime.timedelta(hours=hours),
            "label": f"forecast_{hours}h",
            "type": "forecast",
        }
    if mode == "reanalysis":
        stop = start
        return {
            "start": stop - datetime.timedelta(hours=hours),
            "stop": stop,
            "label": f"reanalysis_{hours}h",
            "type": "reanalysis",
        }
    raise ValueError(f"Unknown mode: {mode}")


def parse_utc_timestamp(raw_value):
    formats = (
        "%Y%m%d%H%M",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M",
    )
    for fmt in formats:
        try:
            return datetime.datetime.strptime(raw_value, fmt)
        except ValueError:
            continue
    raise ValueError(
        "Expected UTC time in YYYYMMDDHHMM, YYYY-MM-DDTHH:MM, "
        "YYYY-MM-DDTHH:MM:SSZ, or YYYY-MM-DD HH:MM format."
    )


def build_run_parameters(mode, hours, start_time=None, end_time=None):
    if start_time is None and end_time is None:
        return get_run_parameters(mode, hours)

    if mode != "reanalysis":
        raise ValueError("--start/--end are only supported for reanalysis mode.")
    if start_time is None or end_time is None:
        raise ValueError("--start and --end must be provided together.")
    if start_time >= end_time:
        raise ValueError("--end must be later than --start.")
    if start_time.minute or end_time.minute:
        raise ValueError("--start and --end must be on hour boundaries (minute 00).")

    duration = end_time - start_time
    hours = int(duration.total_seconds() / 3600)
    if duration != datetime.timedelta(hours=hours):
        raise ValueError("--start and --end must be an exact whole number of hours apart.")

    return {
        "start": start_time,
        "stop": end_time,
        "label": f"reanalysis_{hours}h",
        "type": "reanalysis",
    }


def resolve_cli_path(raw_path):
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return (Path(os.fspath(config_loader.BASE_DIR)) / path).resolve()


def _extract_domain_average_start_label(domain_key: str, output_dir: str) -> str:
    dirname = Path(output_dir).name
    prefix = f"{domain_key}_domavg_"
    if dirname.startswith(prefix):
        return dirname.removeprefix(prefix)
    if dirname.startswith("domavg_"):
        return dirname.removeprefix("domavg_")
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M")


def validate_point_sampling_supported(parser, domain_config, points_input_path) -> None:
    if not points_input_path:
        return
    if template_momentum_enabled(domain_config.template_path):
        parser.error(
            "--points-file is not supported when momentum_flag = true. "
            "Run without --points-file and use mwn.sh validate-rasters afterward."
        )


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
    parser.add_argument("--start",
                        help="UTC start time for reanalysis window "
                             "(YYYYMMDDHHMM or YYYY-MM-DDTHH:MM)")
    parser.add_argument("--end",
                        help="UTC end time for reanalysis window "
                             "(YYYYMMDDHHMM or YYYY-MM-DDTHH:MM)")
    parser.add_argument("--domain", choices=available,
                        default=config_loader.DEFAULT_DOMAIN)
    parser.add_argument("--model", choices=ALL_MODEL_NAMES,
                        default="HRRR")
    parser.add_argument("--speed", type=float, default=None,
                        help="Wind speed for domain-average mode (in --speed-units)")
    parser.add_argument("--direction", type=float, default=None,
                        help="Wind direction in degrees for domain-average mode")
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
    parser.add_argument("--points-file",
                        help="WindNinja point-sampling CSV in WGS84 format.")
    parser.add_argument("--points-output",
                        help="Optional output CSV for sampled model/wx vectors.")
    args = parser.parse_args()

    domain_config = config_loader.get_domain_config(args.domain)
    logger.info(f"Domain: {domain_config.label}")

    do_upload = (config_loader.GCS_UPLOAD_ENABLED
                 and not args.dry_run and not args.no_upload)

    points_input_path = None
    points_output_path = None
    if args.points_file:
        points_input_path = resolve_cli_path(args.points_file)
        if not points_input_path.exists():
            parser.error(f"--points-file does not exist: {points_input_path}")
    if args.points_output:
        points_output_path = resolve_cli_path(args.points_output)
    validate_point_sampling_supported(parser, domain_config, points_input_path)

    if args.mode == "domain-average":
        if args.speed is None or args.direction is None:
            parser.error("--speed and --direction are required for domain-average mode")

        logger.info(
            f"Mode: DOMAIN-AVERAGE | Speed: {args.speed} {args.speed_units} | "
            f"Direction: {args.direction} deg"
        )

        try:
            config_path, output_dir = generate_domain_average_config(
                domain_config, args.speed, args.direction,
                speed_units=args.speed_units,
                surface_vegetation=config_loader.SURFACE_VEGETATION,
                output_wind_height=args.height,
                input_points_file=str(points_input_path) if points_input_path else None,
                output_points_file=str(points_output_path) if points_output_path else None,
            )

            if args.dry_run:
                logger.info(f"Generated config: {config_path}")
            else:
                run_windninja(config_path)

                start_label = _extract_domain_average_start_label(
                    domain_config.key, output_dir,
                )
                output_name = build_domain_average_archive_name(
                    domain_config.key,
                    start_label,
                    args.speed,
                    args.speed_units,
                    args.direction,
                )

                if not args.keep_temp:
                    archive_path = archive_results(output_dir, output_name)
                    logger.info(f"Archive: {archive_path}")
                else:
                    logger.info(f"Output kept in: {output_dir}")

            enforce_retention()
            logger.info("Done.")

        except Exception as exc:
            logger.error(f"FAILED: {exc}")
            raise
        return

    try:
        explicit_start = parse_utc_timestamp(args.start) if args.start else None
        explicit_end = parse_utc_timestamp(args.end) if args.end else None
        run_params = build_run_parameters(
            args.mode, args.hours, start_time=explicit_start, end_time=explicit_end,
        )
    except ValueError as exc:
        parser.error(str(exc))

    try:
        wx_model = resolve_weather_model(args.model, run_params["type"])
    except ValueError as exc:
        parser.error(str(exc))

    date_str = run_params["start"].strftime("%Y%m%d")
    output_dir = os.path.join(
        config_loader.TEMP_DIR,
        build_output_dir_name(
            domain_config.key, run_params["start"], run_params["label"], args.model,
        ),
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
            input_points_file=str(points_input_path) if points_input_path else None,
            output_points_file=str(points_output_path) if points_output_path else None,
            run_type=run_params["type"],
        )

        if not args.dry_run:
            run_windninja(config_path, run_type=run_params["type"])

        if run_params["type"] == "reanalysis":
            rename_reanalysis_outputs(output_dir, domain_config.key)

        if not args.dry_run:
            output_name = build_archive_name_base(
                domain_config.key, run_params["start"], run_params["label"], args.model,
            )

            playable_kmz = None
            try:
                playable_kmz = create_time_series.create_playable_kmz(
                    output_dir, output_name,
                    domain_label=domain_config.label)
                if playable_kmz:
                    logger.info(f"Playable KMZ: {playable_kmz}")
            except Exception as exc:
                logger.error(f"Playable KMZ failed: {exc}")

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

    except Exception as exc:
        logger.error(f"FAILED: {exc}")
        if do_upload:
            gcs.upload_status(run_params["label"], args.model, "failure",
                              error=str(exc))
        raise


if __name__ == "__main__":
    main()
