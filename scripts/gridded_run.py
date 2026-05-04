#!/usr/bin/env python3
"""Run one WindNinja timestep from prepared gridded speed/direction inputs."""
from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
import subprocess
import tempfile

try:
    from . import config_loader, create_time_series, utils
    from .archive_manager import (
        archive_results,
        build_grid_archive_name,
        build_grid_output_dir_name,
        enforce_retention,
        sanitize_label,
    )
    from .daily_run import parse_utc_timestamp
    from .gcs_manager import manager as gcs
    from .wind_math import asc_has_nodata
    from .windninja_config import generate_gridded_config
    from .windninja_runner import run_windninja
except ImportError:
    import config_loader
    import create_time_series
    import utils
    from archive_manager import (
        archive_results,
        build_grid_archive_name,
        build_grid_output_dir_name,
        enforce_retention,
        sanitize_label,
    )
    from daily_run import parse_utc_timestamp
    from gcs_manager import manager as gcs
    from wind_math import asc_has_nodata
    from windninja_config import generate_gridded_config
    from windninja_runner import run_windninja


logger = utils.setup_logging("gridded_run")


class GridValidationError(ValueError):
    """Raised for operator-fixable gridded input problems."""


@dataclass(frozen=True)
class RasterInfo:
    path: Path
    size: tuple[int, int]
    geo_transform: tuple[float, float, float, float, float, float]
    wkt: str
    nodata: float | None


def resolve_cli_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return (Path(os.fspath(config_loader.BASE_DIR)) / path).resolve()


def _run_json(command: list[str]) -> dict:
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise GridValidationError(f"{command[0]} failed for {command[-1]}: {detail}")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise GridValidationError(f"Could not parse {command[0]} JSON for {command[-1]}") from exc


def _raster_info(path: Path) -> RasterInfo:
    payload = _run_json(["gdalinfo", "-json", str(path)])
    size = payload.get("size") or []
    if len(size) != 2:
        raise GridValidationError(f"Could not determine raster dimensions: {path}")

    raw_transform = payload.get("geoTransform") or []
    if len(raw_transform) != 6:
        raise GridValidationError(f"Missing geotransform: {path}")
    transform = tuple(float(value) for value in raw_transform)
    if transform[2] != 0 or transform[4] != 0:
        raise GridValidationError(f"Raster must be north-up: {path}")

    wkt = ((payload.get("coordinateSystem") or {}).get("wkt")) or ""
    if not wkt.strip():
        raise GridValidationError(f"Missing CRS/projection: {path}")

    nodata = None
    bands = payload.get("bands") or []
    if bands and bands[0].get("noDataValue") is not None:
        nodata = float(bands[0]["noDataValue"])

    return RasterInfo(
        path=path,
        size=(int(size[0]), int(size[1])),
        geo_transform=transform,
        wkt=wkt,
        nodata=nodata,
    )


def _extent(info: RasterInfo) -> tuple[float, float, float, float]:
    gt = info.geo_transform
    width, height = info.size
    xmin = gt[0]
    ymax = gt[3]
    xmax = gt[0] + gt[1] * width
    ymin = gt[3] + gt[5] * height
    return min(xmin, xmax), min(ymin, ymax), max(xmin, xmax), max(ymin, ymax)


def _compact_wkt(value: str) -> str:
    return "".join(value.lower().split())


def _epsg_token(path: Path) -> str | None:
    result = subprocess.run(
        ["gdalsrsinfo", "-o", "epsg", str(path)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    for line in result.stdout.splitlines():
        token = line.strip()
        if token.upper().startswith("EPSG:"):
            return token.upper()
    return None


def _crs_matches(left: RasterInfo, right: RasterInfo) -> bool:
    left_epsg = _epsg_token(left.path)
    right_epsg = _epsg_token(right.path)
    if left_epsg and right_epsg:
        return left_epsg == right_epsg
    return _compact_wkt(left.wkt) == _compact_wkt(right.wkt)


def _covers(grid: RasterInfo, terrain: RasterInfo) -> bool:
    gxmin, gymin, gxmax, gymax = _extent(grid)
    txmin, tymin, txmax, tymax = _extent(terrain)
    x_tol = max(abs(grid.geo_transform[1]), 1.0) * 0.5
    y_tol = max(abs(grid.geo_transform[5]), 1.0) * 0.5
    return (
        gxmin <= txmin + x_tol
        and gymin <= tymin + y_tol
        and gxmax >= txmax - x_tol
        and gymax >= tymax - y_tol
    )


def _same_grid(left: RasterInfo, right: RasterInfo) -> bool:
    if left.size != right.size:
        return False
    return all(
        abs(a - b) <= max(abs(a), abs(b), 1.0) * 1e-9
        for a, b in zip(left.geo_transform, right.geo_transform)
    )


def _warp_to_terrain(source: RasterInfo, terrain: RasterInfo, output_path: Path) -> None:
    xmin, ymin, xmax, ymax = _extent(terrain)
    command = [
        "gdalwarp",
        "-overwrite",
        "-of",
        "AAIGrid",
        "-r",
        "near",
        "-dstnodata",
        str(source.nodata if source.nodata is not None else -9999),
        "-t_srs",
        terrain.wkt,
        "-te",
        str(xmin),
        str(ymin),
        str(xmax),
        str(ymax),
        "-ts",
        str(terrain.size[0]),
        str(terrain.size[1]),
        str(source.path),
        str(output_path),
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise GridValidationError(f"Could not inspect no-data overlap for {source.path}: {detail}")


def _has_nodata_overlap(grid: RasterInfo, terrain: RasterInfo) -> bool:
    if grid.nodata is None:
        return False
    if _same_grid(grid, terrain):
        return asc_has_nodata(grid.path, grid.nodata)

    with tempfile.TemporaryDirectory(prefix="mwn-grid-check-") as tmp_dir:
        aligned = Path(tmp_dir) / "aligned.asc"
        _warp_to_terrain(grid, terrain, aligned)
        return asc_has_nodata(aligned, grid.nodata)


def validate_grid_inputs(
    speed_grid: Path,
    direction_grid: Path,
    domain_config,
) -> tuple[RasterInfo, RasterInfo, RasterInfo]:
    """Validate griddedInitialization inputs against the selected domain."""
    if domain_config.elevation_file.suffix.lower() == ".lcp":
        raise GridValidationError(
            "run-grid does not support .lcp domains in v1. Register and use the "
            "matching DEM .tif terrain for this domain instead."
        )

    for label, path in (("speed grid", speed_grid), ("direction grid", direction_grid)):
        if not path.exists():
            raise GridValidationError(f"Missing {label}: {path}")

    if not domain_config.elevation_file.exists():
        raise GridValidationError(f"Missing domain terrain: {domain_config.elevation_file}")

    speed_info = _raster_info(speed_grid)
    direction_info = _raster_info(direction_grid)
    terrain_info = _raster_info(domain_config.elevation_file)

    if speed_info.size != direction_info.size:
        raise GridValidationError(
            f"Grid dimensions do not match: speed={speed_info.size}, "
            f"direction={direction_info.size}"
        )
    if not _crs_matches(speed_info, direction_info):
        raise GridValidationError("Speed and direction grid CRS/projection do not match.")
    if not _crs_matches(speed_info, terrain_info):
        raise GridValidationError("Grid CRS/projection does not match the domain terrain.")
    if not _covers(speed_info, terrain_info):
        raise GridValidationError("Speed grid does not fully cover the domain terrain.")
    if not _covers(direction_info, terrain_info):
        raise GridValidationError("Direction grid does not fully cover the domain terrain.")

    if _has_nodata_overlap(speed_info, terrain_info):
        raise GridValidationError("Speed grid has no-data cells overlapping the domain.")
    if _has_nodata_overlap(direction_info, terrain_info):
        raise GridValidationError("Direction grid has no-data cells overlapping the domain.")

    return speed_info, direction_info, terrain_info


def main() -> int:
    config_loader.init_directories()
    available_domains = config_loader.list_domains()

    parser = argparse.ArgumentParser(
        description="Run one WindNinja timestep from speed/direction AAIGrid files."
    )
    parser.add_argument("--speed-grid", required=True, help="AAIGrid wind speed grid (.asc).")
    parser.add_argument(
        "--direction-grid",
        required=True,
        help="AAIGrid wind direction grid (.asc).",
    )
    parser.add_argument("--time", required=True, help="UTC time for this timestep.")
    parser.add_argument("--domain", required=True, choices=available_domains)
    parser.add_argument("--height", type=float, default=10.0)
    parser.add_argument("--label", default="external")
    parser.add_argument("--keep-temp", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-upload", action="store_true")
    args = parser.parse_args()

    try:
        run_time = parse_utc_timestamp(args.time)
    except ValueError as exc:
        parser.error(str(exc))

    domain_config = config_loader.get_gridded_domain_config(args.domain)
    speed_grid = resolve_cli_path(args.speed_grid)
    direction_grid = resolve_cli_path(args.direction_grid)
    label = sanitize_label(args.label)
    do_upload = (
        config_loader.GCS_UPLOAD_ENABLED
        and not args.dry_run
        and not args.no_upload
    )

    logger.info(f"Domain: {domain_config.label}")
    logger.info(f"Grid time: {run_time} UTC")

    try:
        validate_grid_inputs(speed_grid, direction_grid, domain_config)

        output_dir = os.path.join(
            config_loader.TEMP_DIR,
            build_grid_output_dir_name(domain_config.key, run_time, label),
        )
        config_path, _ = generate_gridded_config(
            domain_config,
            speed_grid,
            direction_grid,
            run_time,
            label=label,
            surface_vegetation=config_loader.SURFACE_VEGETATION,
            sub_dir=output_dir,
            output_wind_height=args.height,
        )
        logger.info(f"Generated config: {config_path}")

        run_label = f"grid_{label}"
        output_name = build_grid_archive_name(domain_config.key, label, run_time)

        if do_upload:
            gcs.upload_status(run_label, "GRID", "running")

        if args.dry_run:
            logger.info("Dry run complete; WindNinja execution skipped.")
            return 0

        run_windninja(config_path)

        playable_kmz = None
        try:
            playable_kmz = create_time_series.create_playable_kmz(
                output_dir, output_name, domain_label=domain_config.label,
            )
            if playable_kmz:
                logger.info(f"Playable KMZ: {playable_kmz}")
        except Exception as exc:
            logger.error(f"Playable KMZ failed: {exc}")

        if do_upload and playable_kmz and os.path.exists(playable_kmz):
            gcs.upload_file(playable_kmz, "latest_grid.kmz",
                            cache_control="public, max-age=60")

        if not args.keep_temp:
            archive_path = archive_results(output_dir, output_name)
            logger.info(f"Archive: {archive_path}")
            if do_upload and os.path.exists(archive_path):
                date_dir = run_time.strftime("%Y%m%d")
                dest = f"archives/{date_dir}/{os.path.basename(archive_path)}"
                gcs.upload_file(archive_path, dest)
                gcs.cleanup_old_forecasts()
        else:
            logger.info(f"Output kept in: {output_dir}")

        if do_upload:
            gcs.upload_status(run_label, "GRID", "success")
            gcs.update_index()

        enforce_retention()
        logger.info("Done.")
        return 0
    except Exception as exc:
        logger.error(f"FAILED: {exc}")
        if do_upload:
            gcs.upload_status(f"grid_{label}", "GRID", "failure", error=str(exc))
        raise


if __name__ == "__main__":
    raise SystemExit(main())
