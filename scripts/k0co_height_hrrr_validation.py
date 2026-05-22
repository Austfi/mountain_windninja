#!/usr/bin/env python3
"""K0CO-only HRRR height adjustment test using GMTED 500 m elevation."""
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import datetime as dt
import html
import json
import math
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, replace
from pathlib import Path

try:
    from . import config_loader
    from . import forcing_from_grib as forcing
    from . import raster_validation
    from . import synoptic_validation as sv
    from . import utils
    from . import validation_study as vs
    from .archive_manager import build_grid_output_dir_name
    from .wind_math import (
        asc_nodata_value,
        convert_speed,
        is_nodata,
        speed_direction_from_uv,
    )
except ImportError:
    import config_loader
    import forcing_from_grib as forcing
    import raster_validation
    import synoptic_validation as sv
    import utils
    import validation_study as vs
    from archive_manager import build_grid_output_dir_name
    from wind_math import (
        asc_nodata_value,
        convert_speed,
        is_nodata,
        speed_direction_from_uv,
    )


UTC = dt.timezone.utc
OUTPUT_NODATA = -9999.0
STUDY_KEY = "berthoud_pass_k0co"
HEIGHT_STUDY_KEY = "berthoud_pass_k0co_height_hrrr"
ADJUSTED_LABEL = "k0co_height_hrrr"
DOCUMENTED_START = "202601010000"
DOCUMENTED_END = "202604010000"
DEFAULT_ARCHIVE_BASE_URL = "https://noaa-hrrr-bdp-pds.s3.amazonaws.com"
GMTED_RESOLUTION_M = 500
ADJUSTMENT_GRID_VERSION = "gmted_500m_v2"
MOMENTUM_SOLVER = "momentum"
MASS_SOLVER = "mass"
SOLVER_CHOICES = (MOMENTUM_SOLVER, MASS_SOLVER)

HRRR_FIELDS = {
    "u10": ("UGRD", "10 m above ground"),
    "v10": ("VGRD", "10 m above ground"),
    "u80": ("UGRD", "80 m above ground"),
    "v80": ("VGRD", "80 m above ground"),
    "hgt": ("HGT", "surface"),
}

logger = utils.setup_logging("k0co_height_hrrr_validation")
ASC_HEADER_KEYS = {
    "ncols",
    "nrows",
    "xllcorner",
    "yllcorner",
    "xllcenter",
    "yllcenter",
    "cellsize",
    "dx",
    "dy",
    "nodata_value",
}


class HeightHrrrError(ValueError):
    """Raised for operator-fixable height-HRRR validation problems."""


@dataclass(frozen=True)
class HrrrIndexRecord:
    number: int
    offset: int
    parameter: str
    level: str
    forecast: str
    raw_line: str


@dataclass(frozen=True)
class RasterInfo:
    size: tuple[int, int]
    geo_transform: tuple[float, float, float, float, float, float]
    wkt: str


@dataclass(frozen=True)
class HourForcingPaths:
    hour: dt.datetime
    speed_mps: Path
    direction: Path
    speed_mph: Path
    metadata: Path


@dataclass(frozen=True)
class AdjustmentSetting:
    key: str
    output_suffix: str
    windninja_label: str
    blend_scale_m: float
    cap_mode: str
    low_cap: float | None = None
    high_cap: float | None = None
    exposure_radius_m: float | None = None
    exposure_inner_skip_m: float = 500.0
    full_exposure_tpi_m: float = 250.0


ADJUSTMENT_SETTINGS = {
    "v1-current": AdjustmentSetting(
        key="v1-current",
        output_suffix="",
        windninja_label=ADJUSTED_LABEL,
        blend_scale_m=300.0,
        cap_mode="raw_10m",
        low_cap=0.75,
        high_cap=1.35,
    ),
    "balanced-300m-10-80-cap": AdjustmentSetting(
        key="balanced-300m-10-80-cap",
        output_suffix="_balanced_300m_10_80_cap",
        windninja_label=f"{ADJUSTED_LABEL}_balanced_300m_10_80_cap",
        blend_scale_m=300.0,
        cap_mode="levels_10_80",
        low_cap=0.75,
        high_cap=1.10,
    ),
    "exposure-gate-300m-10-80-cap": AdjustmentSetting(
        key="exposure-gate-300m-10-80-cap",
        output_suffix="_exposure_gate_300m_10_80_cap",
        windninja_label=f"{ADJUSTED_LABEL}_exposure_gate_300m_10_80_cap",
        blend_scale_m=300.0,
        cap_mode="levels_10_80",
        low_cap=0.75,
        high_cap=1.10,
        exposure_radius_m=3000.0,
        exposure_inner_skip_m=500.0,
        full_exposure_tpi_m=250.0,
    ),
    "exposure-gate-400m-10-80-cap": AdjustmentSetting(
        key="exposure-gate-400m-10-80-cap",
        output_suffix="_exposure_gate_400m_10_80_cap",
        windninja_label=f"{ADJUSTED_LABEL}_exposure_gate_400m_10_80_cap",
        blend_scale_m=300.0,
        cap_mode="levels_10_80",
        low_cap=0.75,
        high_cap=1.10,
        exposure_radius_m=3000.0,
        exposure_inner_skip_m=500.0,
        full_exposure_tpi_m=400.0,
    ),
}


def default_validation_root(adjustment_setting: AdjustmentSetting) -> str:
    return f"runtime/validation/{HEIGHT_STUDY_KEY}{adjustment_setting.output_suffix}"


def windninja_domain_for_solver(study_domain: str, solver: str) -> str:
    if solver == MOMENTUM_SOLVER:
        return study_domain
    if solver == MASS_SOLVER:
        mass_domain = f"{study_domain}_mass"
        if mass_domain not in config_loader.list_domains():
            raise HeightHrrrError(f"Mass-solver domain not configured: {mass_domain}")
        return mass_domain
    raise HeightHrrrError(f"Unsupported WindNinja solver: {solver}")


def solver_label_suffix(solver: str) -> str:
    return "" if solver == MOMENTUM_SOLVER else f"_{solver}"


def windninja_label_for_solver(adjustment_setting: AdjustmentSetting, solver: str) -> str:
    return f"{adjustment_setting.windninja_label}{solver_label_suffix(solver)}"


def ymdhm(value: dt.datetime) -> str:
    return value.astimezone(UTC).strftime("%Y%m%d%H%M")


def run_command(command: list[str], *, input_text: str | None = None) -> str:
    result = subprocess.run(
        command,
        input=input_text,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise HeightHrrrError(f"{command[0]} failed: {detail}")
    return result.stdout


def gdalinfo_json(path: Path | str) -> dict:
    return json.loads(run_command(["gdalinfo", "-json", str(path)]))


def raster_info(path: Path) -> RasterInfo:
    payload = gdalinfo_json(path)
    size = payload.get("size") or []
    transform = payload.get("geoTransform") or []
    wkt = ((payload.get("coordinateSystem") or {}).get("wkt")) or sidecar_wkt(path)
    if len(size) != 2 or len(transform) != 6 or not wkt.strip():
        raise HeightHrrrError(f"Could not inspect raster grid: {path}")
    return RasterInfo(
        size=(int(size[0]), int(size[1])),
        geo_transform=tuple(float(value) for value in transform),
        wkt=wkt,
    )


def sidecar_wkt(path: Path) -> str:
    prj_path = path.with_suffix(".prj")
    if not prj_path.exists():
        return ""
    return prj_path.read_text(encoding="utf-8", errors="ignore").strip()


def raster_extent(info: RasterInfo) -> tuple[float, float, float, float]:
    gt = info.geo_transform
    width, height = info.size
    xmin = gt[0]
    ymax = gt[3]
    xmax = gt[0] + gt[1] * width
    ymin = gt[3] + gt[5] * height
    return min(xmin, xmax), min(ymin, ymax), max(xmin, xmax), max(ymin, ymax)


def expand_extent_to_resolution(
    xmin: float,
    ymin: float,
    xmax: float,
    ymax: float,
    resolution_m: int,
) -> tuple[float, float, float, float]:
    width = math.ceil((xmax - xmin) / resolution_m) * resolution_m
    height = math.ceil((ymax - ymin) / resolution_m) * resolution_m
    return xmin, ymax - height, xmin + width, ymax


def extent_covers(
    grid_extent: tuple[float, float, float, float],
    target_extent: tuple[float, float, float, float],
) -> bool:
    gxmin, gymin, gxmax, gymax = grid_extent
    txmin, tymin, txmax, tymax = target_extent
    tolerance_m = 0.01
    return (
        gxmin <= txmin + tolerance_m
        and gymin <= tymin + tolerance_m
        and gxmax >= txmax - tolerance_m
        and gymax >= tymax - tolerance_m
    )


def raster_covers_extent(
    grid: RasterInfo,
    target_extent: tuple[float, float, float, float],
) -> bool:
    return extent_covers(raster_extent(grid), target_extent)


def windninja_adjustment_extent(target: RasterInfo) -> tuple[float, float, float, float]:
    xmin, ymin, xmax, ymax = raster_extent(target)
    return expand_extent_to_resolution(
        xmin - GMTED_RESOLUTION_M,
        ymin - GMTED_RESOLUTION_M,
        xmax + GMTED_RESOLUTION_M,
        ymax + GMTED_RESOLUTION_M,
        GMTED_RESOLUTION_M,
    )


def dataset_srs_arg(path: Path, fallback_wkt: str) -> str:
    result = subprocess.run(
        ["gdalsrsinfo", "-o", "epsg", str(path)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        for line in result.stdout.splitlines():
            token = line.strip()
            if token.upper().startswith("EPSG:"):
                return token.upper()
    return fallback_wkt


def terrain_bbox_wgs84(terrain: forcing.TerrainGrid, *, pad_degrees: float = 0.1) -> dict:
    xmin, ymin, xmax, ymax = raster_extent(
        RasterInfo(terrain.size, terrain.geo_transform, terrain.wkt)
    )
    points = [(xmin, ymin), (xmin, ymax), (xmax, ymin), (xmax, ymax)]
    input_text = "\n".join(f"{x} {y}" for x, y in points) + "\n"
    output = run_command(
        [
            "gdaltransform",
            "-s_srs",
            dataset_srs_arg(terrain.path, terrain.wkt),
            "-t_srs",
            "EPSG:4326",
        ],
        input_text=input_text,
    )
    lons = []
    lats = []
    for line in output.splitlines():
        parts = line.split()
        if len(parts) >= 2:
            lons.append(float(parts[0]))
            lats.append(float(parts[1]))
    if not lons or not lats:
        raise HeightHrrrError(f"Could not transform terrain bbox for {terrain.path}")
    return {
        "west": min(lons) - pad_degrees,
        "south": min(lats) - pad_degrees,
        "east": max(lons) + pad_degrees,
        "north": max(lats) + pad_degrees,
    }


def parse_hrrr_idx(text: str) -> list[HrrrIndexRecord]:
    records: list[HrrrIndexRecord] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        parts = line.rstrip(":").split(":")
        if len(parts) < 6:
            continue
        try:
            number = int(parts[0])
            offset = int(parts[1])
        except ValueError:
            continue
        records.append(
            HrrrIndexRecord(
                number=number,
                offset=offset,
                parameter=parts[3],
                level=parts[4],
                forecast=parts[5],
                raw_line=line,
            )
        )
    return records


def select_hrrr_messages(records: list[HrrrIndexRecord]) -> dict[str, HrrrIndexRecord]:
    selected = {}
    for key, (parameter, level) in HRRR_FIELDS.items():
        match = next(
            (
                record
                for record in records
                if record.parameter == parameter
                and record.level == level
                and record.forecast == "anl"
            ),
            None,
        )
        if match is None:
            raise HeightHrrrError(f"HRRR index is missing {parameter}:{level}:anl")
        selected[key] = match
    return selected


def hrrr_url(run_time: dt.datetime, archive_base_url: str) -> str:
    stamp = run_time.astimezone(UTC)
    return (
        f"{archive_base_url.rstrip('/')}/hrrr.{stamp:%Y%m%d}/conus/"
        f"hrrr.t{stamp:%H}z.wrfsfcf00.grib2"
    )


def download_bytes(url: str, *, byte_range: tuple[int, int | None] | None = None) -> bytes:
    headers = {"User-Agent": "mountain-windninja/height-hrrr-validation"}
    if byte_range is not None:
        start, end = byte_range
        headers["Range"] = f"bytes={start}-" if end is None else f"bytes={start}-{end}"
    request = urllib.request.Request(url, headers=headers)
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.read()
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
            if attempt == 2:
                break
            time.sleep(2 ** attempt)
    raise HeightHrrrError(f"Could not download {url}: {last_error}")


def download_hrrr_subset(run_time: dt.datetime, grib_dir: Path, archive_base_url: str) -> Path:
    grib_dir.mkdir(parents=True, exist_ok=True)
    grib_path = grib_dir / f"hrrr_{ymdhm(run_time)}_height_subset.grib2"
    idx_path = grib_path.with_suffix(".idx")
    if grib_path.exists() and idx_path.exists():
        return grib_path

    url = hrrr_url(run_time, archive_base_url)
    idx_text = download_bytes(f"{url}.idx").decode("utf-8")
    records = parse_hrrr_idx(idx_text)
    selected = select_hrrr_messages(records)
    positions = {record.number: index for index, record in enumerate(records)}

    with grib_path.open("wb") as handle:
        for key in HRRR_FIELDS:
            record = selected[key]
            position = positions[record.number]
            next_offset = records[position + 1].offset if position + 1 < len(records) else None
            byte_end = None if next_offset is None else next_offset - 1
            handle.write(download_bytes(url, byte_range=(record.offset, byte_end)))
    idx_path.write_text(idx_text, encoding="utf-8")
    return grib_path


def write_prj(path: Path, wkt: str) -> None:
    path.with_suffix(".prj").write_text(wkt.strip() + "\n", encoding="utf-8")


def esri_wkt(source: Path) -> str:
    output = run_command(["gdalsrsinfo", "-o", "wkt_esri", str(source)])
    return output.strip()


def copy_grid_projection(source: Path, target: Path) -> None:
    source_prj = source.with_suffix(".prj")
    if source_prj.exists():
        shutil.copy2(source_prj, target.with_suffix(".prj"))
        return
    try:
        write_prj(target, esri_wkt(source))
    except HeightHrrrError:
        return


def translate_hrrr_band(grib_path: Path, band: int, bbox: dict, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    command = [
        "gdal_translate",
        "-q",
        "-of",
        "AAIGrid",
        "-b",
        str(band),
        "-projwin_srs",
        "EPSG:4326",
        "-projwin",
        str(bbox["west"]),
        str(bbox["north"]),
        str(bbox["east"]),
        str(bbox["south"]),
        str(grib_path),
        str(output_path),
    ]
    run_command(command)
    write_prj(output_path, esri_wkt(grib_path))


def warp_grid_to_reference_grid(
    source_grid: Path,
    reference_grid: Path,
    output_path: Path,
    *,
    resample: str = "bilinear",
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    info = raster_info(reference_grid)
    xmin, ymin, xmax, ymax = raster_extent(info)
    command = [
        "gdalwarp",
        "-overwrite",
        "-q",
        "-of",
        "AAIGrid",
        "-r",
        resample,
        "-dstnodata",
        str(OUTPUT_NODATA),
        "-t_srs",
        info.wkt,
        "-te",
        str(xmin),
        str(ymin),
        str(xmax),
        str(ymax),
        "-ts",
        str(info.size[0]),
        str(info.size[1]),
        str(source_grid),
        str(output_path),
    ]
    run_command(command)
    write_prj(output_path, esri_wkt(reference_grid))


def gmted_source_path(domain_key: str) -> Path:
    return Path(config_loader.STATIC_DATA_DIR) / f"{domain_key}_gmted_500m.tif"


def ensure_gmted_source_grid(
    domain_key: str,
    terrain: forcing.TerrainGrid,
    *,
    force: bool,
) -> Path:
    path = gmted_source_path(domain_key)
    _ = force
    if path.exists():
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    bbox = terrain_bbox_wgs84(terrain, pad_degrees=0.05)
    logger.info(f"Fetching GMTED 500 m elevation source: {path}")
    run_command([
        "fetch_dem",
        "--bbox",
        str(bbox["north"]),
        str(bbox["east"]),
        str(bbox["south"]),
        str(bbox["west"]),
        "--src",
        "gmted",
        "--out_res",
        str(GMTED_RESOLUTION_M),
        str(path),
    ])
    return path


def ensure_gmted_adjustment_grid(
    domain_key: str,
    terrain: forcing.TerrainGrid,
    validation_root: Path,
    *,
    force: bool,
) -> Path:
    output_path = validation_root / "gmted_500m" / "elevation.asc"
    terrain_info = RasterInfo(terrain.size, terrain.geo_transform, terrain.wkt)
    target_extent = windninja_adjustment_extent(terrain_info)
    if output_path.exists() and not force:
        try:
            if raster_covers_extent(raster_info(output_path), target_extent):
                return output_path
        except HeightHrrrError:
            pass
        logger.info(f"Rebuilding GMTED 500 m grid with full domain coverage: {output_path}")
    source_grid = ensure_gmted_source_grid(domain_key, terrain, force=force)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    xmin, ymin, xmax, ymax = target_extent
    command = [
        "gdalwarp",
        "-overwrite",
        "-q",
        "-of",
        "AAIGrid",
        "-r",
        "bilinear",
        "-dstnodata",
        str(OUTPUT_NODATA),
        "-t_srs",
        terrain.wkt,
        "-te",
        str(xmin),
        str(ymin),
        str(xmax),
        str(ymax),
        "-tr",
        str(GMTED_RESOLUTION_M),
        str(GMTED_RESOLUTION_M),
        str(source_grid),
        str(output_path),
    ]
    run_command(command)
    write_prj(output_path, esri_wkt(terrain.path))
    return output_path


def header_with_nodata(header_lines: list[str]) -> list[str]:
    output = []
    replaced = False
    for line in header_lines:
        if line.strip().lower().startswith("nodata_value"):
            output.append(f"NODATA_value {OUTPUT_NODATA:g}")
            replaced = True
        else:
            output.append(line)
    if not replaced:
        output.append(f"NODATA_value {OUTPUT_NODATA:g}")
    return output


def grid_value(value: float) -> str:
    return f"{value:.6f}" if math.isfinite(value) else f"{OUTPUT_NODATA:g}"


def read_grid_header(path: Path) -> tuple[list[str], dict[str, str]]:
    lines = []
    header = {}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            parts = stripped.split(maxsplit=1)
            key = parts[0].lower()
            if key not in ASC_HEADER_KEYS:
                break
            lines.append(line.rstrip("\n"))
            header[key] = parts[1].strip() if len(parts) > 1 else ""
    return lines, header


def iter_grid_rows(path: Path):
    header_lines, _header = read_grid_header(path)
    with path.open("r", encoding="utf-8") as handle:
        for _ in header_lines:
            next(handle)
        for line in handle:
            stripped = line.strip()
            if stripped:
                yield [float(value) for value in stripped.split()]


def grid_cell_size_m(header: dict[str, str]) -> float:
    value = header.get("cellsize")
    if value is None:
        raise HeightHrrrError("ASC grid is missing cellsize")
    return abs(float(value))


def exposure_weight_grid(
    dem_grid: Path,
    nodata: float,
    setting: AdjustmentSetting,
) -> tuple[list[list[float]], dict[str, float | int | None]]:
    if setting.exposure_radius_m is None:
        return [], {}

    _header_lines, header = read_grid_header(dem_grid)
    cell_size = grid_cell_size_m(header)
    dem_rows = list(iter_grid_rows(dem_grid))
    if not dem_rows:
        return [], {
            "exposure_cell_count": 0,
            "exposure_weight_mean": None,
            "tpi_min_m": None,
            "tpi_max_m": None,
        }

    radius_cells = max(1, math.ceil(setting.exposure_radius_m / cell_size))
    inner_skip_m = max(0.0, setting.exposure_inner_skip_m)
    weights: list[list[float]] = []
    stats: dict[str, float | int | None] = {
        "exposure_cell_count": 0,
        "exposure_weight_sum": 0.0,
        "tpi_min_m": None,
        "tpi_max_m": None,
    }

    for row_index, row in enumerate(dem_rows):
        weight_row = []
        for col_index, center in enumerate(row):
            if is_nodata(center, nodata):
                weight_row.append(0.0)
                continue
            surrounding = []
            row_min = max(0, row_index - radius_cells)
            row_max = min(len(dem_rows), row_index + radius_cells + 1)
            for other_row_index in range(row_min, row_max):
                other_row = dem_rows[other_row_index]
                col_min = max(0, col_index - radius_cells)
                col_max = min(len(other_row), col_index + radius_cells + 1)
                for other_col_index in range(col_min, col_max):
                    dy = (other_row_index - row_index) * cell_size
                    dx = (other_col_index - col_index) * cell_size
                    distance = math.hypot(dx, dy)
                    if distance <= inner_skip_m or distance > setting.exposure_radius_m:
                        continue
                    value = other_row[other_col_index]
                    if not is_nodata(value, nodata):
                        surrounding.append(value)
            if not surrounding:
                weight_row.append(0.0)
                continue
            tpi = center - (sum(surrounding) / len(surrounding))
            exposure_weight = min(max(tpi / setting.full_exposure_tpi_m, 0.0), 1.0)
            stats["exposure_cell_count"] = int(stats["exposure_cell_count"] or 0) + 1
            stats["exposure_weight_sum"] = float(stats["exposure_weight_sum"] or 0.0) + exposure_weight
            for key, value in (("tpi_min_m", tpi), ("tpi_max_m", tpi)):
                current = stats[key]
                if current is None:
                    stats[key] = value
                elif key.endswith("min_m"):
                    stats[key] = min(float(current), value)
                else:
                    stats[key] = max(float(current), value)
            weight_row.append(exposure_weight)
        weights.append(weight_row)

    count = int(stats["exposure_cell_count"] or 0)
    stats["exposure_weight_mean"] = (
        float(stats["exposure_weight_sum"] or 0.0) / count if count else None
    )
    stats.pop("exposure_weight_sum")
    return weights, stats


def adjustment_cap_limits(
    setting: AdjustmentSetting,
    raw_speed: float,
    speed_80m: float,
) -> tuple[float | None, float | None]:
    if setting.cap_mode == "none":
        return None, None

    assert setting.low_cap is not None
    assert setting.high_cap is not None

    if setting.cap_mode == "raw_10m":
        return raw_speed * setting.low_cap, raw_speed * setting.high_cap

    if setting.cap_mode == "levels_10_80":
        return (
            min(raw_speed, speed_80m) * setting.low_cap,
            max(raw_speed, speed_80m) * setting.high_cap,
        )

    raise ValueError(f"Unknown adjustment cap mode {setting.cap_mode}")


def write_speed_grid_in_units(source_speed: Path, output_speed: Path, to_units: str) -> None:
    header_lines, header = read_grid_header(source_speed)
    nodata = asc_nodata_value(header, OUTPUT_NODATA)
    with output_speed.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(header_with_nodata(header_lines)) + "\n")
        for row in iter_grid_rows(source_speed):
            values = []
            for value in row:
                if is_nodata(value, nodata):
                    values.append(f"{OUTPUT_NODATA:g}")
                else:
                    values.append(grid_value(convert_speed(value, "mps", to_units)))
            handle.write(" ".join(values) + "\n")
    copy_grid_projection(source_speed, output_speed)


def write_adjusted_forcing_grids(
    u10_grid: Path,
    v10_grid: Path,
    u80_grid: Path,
    v80_grid: Path,
    hgt_grid: Path,
    dem_grid: Path,
    speed_grid: Path,
    direction_grid: Path,
    setting: AdjustmentSetting = ADJUSTMENT_SETTINGS["v1-current"],
) -> dict:
    header_lines, _ = read_grid_header(u10_grid)
    grids = [u10_grid, v10_grid, u80_grid, v80_grid, hgt_grid, dem_grid]
    nodata_values = [asc_nodata_value(read_grid_header(path)[1], OUTPUT_NODATA) for path in grids]
    exposure_weights, exposure_stats = exposure_weight_grid(dem_grid, nodata_values[5], setting)
    stats = {
        "valid_cell_count": 0,
        "adjusted_cell_count": 0,
        "raw_fallback_count": 0,
        "cap_low_count": 0,
        "cap_high_count": 0,
        "weight_sum": 0.0,
        "base_weight_sum": 0.0,
        "elevation_delta_min_m": None,
        "elevation_delta_max_m": None,
        **exposure_stats,
    }
    speed_grid.parent.mkdir(parents=True, exist_ok=True)
    with speed_grid.open("w", encoding="utf-8") as speed_out, direction_grid.open(
        "w", encoding="utf-8"
    ) as direction_out:
        header = "\n".join(header_with_nodata(header_lines)) + "\n"
        speed_out.write(header)
        direction_out.write(header)
        for row_index, rows in enumerate(zip(*(iter_grid_rows(path) for path in grids))):
            speed_values = []
            direction_values = []
            for col_index, values in enumerate(zip(*rows)):
                u10, v10, u80, v80, hgt, dem = values
                if is_nodata(u10, nodata_values[0]) or is_nodata(v10, nodata_values[1]):
                    speed_values.append(f"{OUTPUT_NODATA:g}")
                    direction_values.append(f"{OUTPUT_NODATA:g}")
                    continue

                can_adjust = not any(
                    is_nodata(value, nodata)
                    for value, nodata in (
                        (u80, nodata_values[2]),
                        (v80, nodata_values[3]),
                        (hgt, nodata_values[4]),
                        (dem, nodata_values[5]),
                    )
                )
                raw_speed = math.hypot(u10, v10)
                if can_adjust:
                    elevation_delta = dem - hgt
                    base_weight = min(max(elevation_delta / setting.blend_scale_m, 0.0), 1.0)
                    exposure_weight = 1.0
                    if exposure_weights:
                        exposure_weight = exposure_weights[row_index][col_index]
                    weight = base_weight * exposure_weight
                    adjusted_u = (1.0 - weight) * u10 + weight * u80
                    adjusted_v = (1.0 - weight) * v10 + weight * v80
                    adjusted_speed = math.hypot(adjusted_u, adjusted_v)
                    low_cap, high_cap = adjustment_cap_limits(
                        setting,
                        raw_speed,
                        math.hypot(u80, v80),
                    )
                    capped_speed = adjusted_speed
                    if low_cap is not None:
                        capped_speed = max(capped_speed, low_cap)
                    if high_cap is not None:
                        capped_speed = min(capped_speed, high_cap)
                    if adjusted_speed > 0.0 and capped_speed != adjusted_speed:
                        scale = capped_speed / adjusted_speed
                        adjusted_u *= scale
                        adjusted_v *= scale
                    if low_cap is not None and adjusted_speed < low_cap:
                        stats["cap_low_count"] += 1
                    elif high_cap is not None and adjusted_speed > high_cap:
                        stats["cap_high_count"] += 1
                    stats["adjusted_cell_count"] += 1
                    stats["weight_sum"] += weight
                    stats["base_weight_sum"] += base_weight
                    for key, value in (
                        ("elevation_delta_min_m", elevation_delta),
                        ("elevation_delta_max_m", elevation_delta),
                    ):
                        current = stats[key]
                        if current is None:
                            stats[key] = value
                        elif key.endswith("min_m"):
                            stats[key] = min(current, value)
                        else:
                            stats[key] = max(current, value)
                else:
                    adjusted_u = u10
                    adjusted_v = v10
                    stats["raw_fallback_count"] += 1

                speed, direction = speed_direction_from_uv(adjusted_u, adjusted_v)
                speed_values.append(grid_value(speed if speed is not None else OUTPUT_NODATA))
                direction_values.append(
                    grid_value(direction if direction is not None else OUTPUT_NODATA)
                )
                stats["valid_cell_count"] += 1
            speed_out.write(" ".join(speed_values) + "\n")
            direction_out.write(" ".join(direction_values) + "\n")

    if stats["adjusted_cell_count"]:
        stats["weight_mean"] = stats["weight_sum"] / stats["adjusted_cell_count"]
        stats["base_weight_mean"] = stats["base_weight_sum"] / stats["adjusted_cell_count"]
        stats["cap_low_cell_fraction"] = stats["cap_low_count"] / stats["adjusted_cell_count"]
        stats["cap_high_cell_fraction"] = stats["cap_high_count"] / stats["adjusted_cell_count"]
    else:
        stats["weight_mean"] = None
        stats["base_weight_mean"] = None
        stats["cap_low_cell_fraction"] = None
        stats["cap_high_cell_fraction"] = None
    stats.pop("weight_sum")
    stats.pop("base_weight_sum")
    copy_grid_projection(u10_grid, speed_grid)
    copy_grid_projection(u10_grid, direction_grid)
    return stats


def sample_dataset_value(path: Path, lon: float, lat: float) -> float | None:
    result = subprocess.run(
        ["gdallocationinfo", "-wgs84", "-valonly", str(path), str(lon), str(lat)],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise HeightHrrrError(f"gdallocationinfo failed for {path}: {detail}")
    for line in reversed(result.stdout.splitlines()):
        try:
            value = float(line.strip())
        except ValueError:
            continue
        if value <= -9990:
            return None
        return value
    return None


def cleanup_intermediates(hour_dir: Path) -> None:
    for name in ("grib", "fields", "fields_native"):
        path = hour_dir / name
        if path.exists():
            shutil.rmtree(path)


def cleanup_ninjafoam_caches(domain_key: str) -> int:
    static_dir = Path(config_loader.STATIC_DATA_DIR)
    if not static_dir.exists():
        return 0
    prefix = f"NINJAFOAM_{domain_key}_"
    removed = 0
    for path in static_dir.iterdir():
        if path.is_dir() and path.name.startswith(prefix):
            shutil.rmtree(path)
            removed += 1
    if removed:
        logger.info(f"Removed {removed} generated NINJAFOAM cache directories for {domain_key}")
    return removed


def metadata_matches_setting(metadata_path: Path, adjustment_setting: AdjustmentSetting) -> bool:
    try:
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    setting_key = payload.get("adjustment_setting")
    return payload.get("adjustment_grid_version") == ADJUSTMENT_GRID_VERSION and (
        setting_key == adjustment_setting.key
        or (adjustment_setting.key == "v1-current" and setting_key is None)
    )


def metadata_is_gmted_500m(metadata_path: Path) -> bool:
    return metadata_matches_setting(metadata_path, ADJUSTMENT_SETTINGS["v1-current"])


def prepare_adjusted_hrrr_hour(
    run_time: dt.datetime,
    study: vs.StudyConfig,
    validation_root: Path,
    *,
    archive_base_url: str,
    force: bool,
    adjustment_setting: AdjustmentSetting = ADJUSTMENT_SETTINGS["v1-current"],
) -> HourForcingPaths:
    domain = config_loader.get_gridded_domain_config(study.domain)
    terrain = forcing._terrain_grid(domain.elevation_file)
    adjustment_grid = ensure_gmted_adjustment_grid(
        study.domain,
        terrain,
        validation_root,
        force=force,
    )
    hour_dir = validation_root / "forcing" / ymdhm(run_time)
    adjusted_dir = hour_dir / f"adjusted_hrrr_gmted_500m{adjustment_setting.output_suffix}"
    validation_dir = hour_dir / f"validation_hrrr_gmted_500m{adjustment_setting.output_suffix}"
    paths = HourForcingPaths(
        hour=run_time,
        speed_mps=adjusted_dir / "speed.asc",
        direction=adjusted_dir / "direction.asc",
        speed_mph=validation_dir / "speed_mph.asc",
        metadata=hour_dir / "adjustment_metadata.json",
    )
    native_ready = (
        paths.speed_mps.exists()
        and paths.direction.exists()
        and paths.speed_mph.exists()
        and paths.metadata.exists()
        and metadata_matches_setting(paths.metadata, adjustment_setting)
    )
    if native_ready and not force:
        cleanup_intermediates(hour_dir)
        return paths

    if force and hour_dir.exists():
        shutil.rmtree(hour_dir)
    adjusted_dir.mkdir(parents=True, exist_ok=True)
    validation_dir.mkdir(parents=True, exist_ok=True)

    grib_path = download_hrrr_subset(run_time, hour_dir / "grib", archive_base_url)
    bbox = terrain_bbox_wgs84(terrain)
    field_paths = {}
    for band, key in enumerate(HRRR_FIELDS, start=1):
        native_field_path = hour_dir / "fields_native" / f"{key}.asc"
        field_path = hour_dir / "fields" / f"{key}.asc"
        translate_hrrr_band(grib_path, band, bbox, native_field_path)
        warp_grid_to_reference_grid(native_field_path, adjustment_grid, field_path)
        field_paths[key] = field_path

    stats = write_adjusted_forcing_grids(
        field_paths["u10"],
        field_paths["v10"],
        field_paths["u80"],
        field_paths["v80"],
        field_paths["hgt"],
        adjustment_grid,
        paths.speed_mps,
        paths.direction,
        setting=adjustment_setting,
    )
    write_speed_grid_in_units(paths.speed_mps, paths.speed_mph, study.speed_units)
    validation_direction = validation_dir / "direction.asc"
    shutil.copy2(paths.direction, validation_direction)
    copy_grid_projection(paths.speed_mps, validation_direction)

    paths.metadata.write_text(
        json.dumps(
            {
                "time_utc": sv.isoformat_utc(run_time),
                "domain": study.domain,
                "adjustment_setting": adjustment_setting.key,
                "adjustment_grid_version": ADJUSTMENT_GRID_VERSION,
                "grid": "GMTED 500 m grid in the WindNinja domain projection",
                "terrain_sample": "GMTED2010 elevation at 500 m",
                "elevation_grid": str(adjustment_grid),
                "windninja_input_grid": "same 500 m adjusted HRRR grid",
                "windninja_terrain": str(domain.elevation_file),
                "formula": {
                    "elevation_delta": "GMTED 500 m elevation - HRRR surface HGT",
                    "weight": f"clamp(elevation_delta / {adjustment_setting.blend_scale_m:g} m, 0, 1)",
                    "exposure_gate": {
                        "radius_m": adjustment_setting.exposure_radius_m,
                        "inner_skip_m": adjustment_setting.exposure_inner_skip_m
                        if adjustment_setting.exposure_radius_m is not None
                        else None,
                        "full_exposure_tpi_m": adjustment_setting.full_exposure_tpi_m
                        if adjustment_setting.exposure_radius_m is not None
                        else None,
                    },
                    "u_adjusted": "(1 - weight) * u10 + weight * u80",
                    "v_adjusted": "(1 - weight) * v10 + weight * v80",
                    "speed_cap": {
                        "mode": adjustment_setting.cap_mode,
                        "low_cap": adjustment_setting.low_cap,
                        "high_cap": adjustment_setting.high_cap,
                    },
                },
                "stats": stats,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    cleanup_intermediates(hour_dir)
    return paths


def sample_time(row: dict) -> dt.datetime:
    return sv.parse_iso_time(row["sample_time_utc"])


def filter_rows_for_window(rows: list[dict], start: dt.datetime, end: dt.datetime) -> list[dict]:
    return [
        row
        for row in rows
        if row.get("station_id") == "K0CO" and start <= sample_time(row) < end
    ]


def dedupe_sample_rows(rows: list[dict]) -> list[dict]:
    seen = set()
    output = []
    for row in rows:
        key = (row.get("station_id"), row.get("sample_time_utc"))
        if key in seen:
            continue
        seen.add(key)
        output.append(row)
    return output


def native_sample_paths_for_study(study: vs.StudyConfig, chunks: list[vs.Chunk]) -> list[Path]:
    aggregate = study.validation_root / "samples.csv"
    if aggregate.exists():
        return [aggregate]
    return [
        paths["samples"]
        for chunk in chunks
        if (paths := vs.chunk_output_paths(study, chunk))["samples"].exists()
    ]


def sample_adjusted_hrrr(row: dict, station: dict, forcing_paths: HourForcingPaths) -> dict:
    lon = station["longitude"]
    lat = station["latitude"]
    adjusted_speed = sample_dataset_value(forcing_paths.speed_mph, lon, lat)
    adjusted_dir = sample_dataset_value(
        forcing_paths.speed_mph.parent / "direction.asc",
        lon,
        lat,
    )
    if adjusted_speed is None or adjusted_dir is None:
        raise HeightHrrrError(f"Could not sample adjusted HRRR at K0CO for {ymdhm(forcing_paths.hour)}")
    adjusted_u, adjusted_v = sv.obs_to_uv(adjusted_speed, adjusted_dir)
    vector_error = math.hypot(adjusted_u - row["u_obs"], adjusted_v - row["v_obs"])
    return {
        "station_id": row["station_id"],
        "sample_time_utc": row["sample_time_utc"],
        "obs_time_utc": row["obs_time_utc"],
        "observed_speed": row["speed_obs"],
        "observed_dir_deg": row["dir_obs_deg"],
        "hrrr_speed": row["wx_speed"],
        "hrrr_dir_deg": row["wx_dir_deg"],
        "hrrr_speed_error": row["wx_speed_error"],
        "hrrr_dir_abs_error_deg": row["wx_dir_abs_error_deg"],
        "hrrr_vector_error": row["wx_vector_error"],
        "adjusted_hrrr_speed": round(adjusted_speed, 6),
        "adjusted_hrrr_dir_deg": round(adjusted_dir, 6),
        "adjusted_hrrr_speed_error": round(adjusted_speed - row["speed_obs"], 6),
        "adjusted_hrrr_dir_abs_error_deg": round(
            sv.circular_abs_error_deg(adjusted_dir, row["dir_obs_deg"]),
            6,
        ),
        "adjusted_hrrr_vector_error": round(vector_error, 6),
    }


def metric_row(name: str, rows: list[dict], prefix: str) -> dict:
    speed_errors = [row[f"{prefix}_speed_error"] for row in rows]
    direction_errors = [row[f"{prefix}_dir_abs_error_deg"] for row in rows]
    vector_errors = [row[f"{prefix}_vector_error"] for row in rows]
    return {
        "result": name,
        "sample_count": len(rows),
        "speed_bias": sv.mean(speed_errors),
        "speed_mae": sv.mean([abs(value) for value in speed_errors]),
        "speed_rmse": sv.rmse(speed_errors),
        "dir_mae_deg": sv.mean(direction_errors),
        "vector_rmse": sv.rmse(vector_errors),
    }


def metric_value(value) -> str:
    if isinstance(value, int):
        return str(value)
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{numeric:.2f}"


def daily_speed_means(rows: list[dict]) -> list[dict]:
    by_day: dict[dt.date, dict[str, list[float]]] = {}
    for row in rows:
        day = sample_time(row).date()
        bucket = by_day.setdefault(
            day,
            {"observed": [], "hrrr": [], "adjusted_hrrr": []},
        )
        bucket["observed"].append(float(row["observed_speed"]))
        bucket["hrrr"].append(float(row["hrrr_speed"]))
        bucket["adjusted_hrrr"].append(float(row["adjusted_hrrr_speed"]))
    output = []
    for day in sorted(by_day):
        output.append({
            "date": day,
            "observed": sv.mean(by_day[day]["observed"]),
            "hrrr": sv.mean(by_day[day]["hrrr"]),
            "adjusted_hrrr": sv.mean(by_day[day]["adjusted_hrrr"]),
        })
    return output


def comparison_svg(rows: list[dict]) -> str:
    daily = daily_speed_means(rows)
    if not daily:
        return "<p>No samples available.</p>"
    width = 1120
    height = 430
    left = 70
    right = 24
    top = 48
    bottom = 64
    plot_width = width - left - right
    plot_height = height - top - bottom
    values = [
        value
        for day in daily
        for value in (day["observed"], day["hrrr"], day["adjusted_hrrr"])
        if value is not None
    ]
    ymax = max(10.0, math.ceil(max(values) / 4.0) * 4.0)
    ymin = 0.0

    def x_for(index: int) -> float:
        if len(daily) == 1:
            return left + plot_width / 2.0
        return left + plot_width * index / (len(daily) - 1)

    def y_for(value: float) -> float:
        return top + (ymax - value) / (ymax - ymin) * plot_height

    def polyline(key: str, color: str) -> str:
        points = " ".join(
            f"{x_for(index):.1f},{y_for(float(day[key])):.1f}"
            for index, day in enumerate(daily)
            if day[key] is not None
        )
        return f'<polyline points="{points}" class="series" stroke="{color}"/>'

    ticks = [ymax * index / 5.0 for index in range(5, -1, -1)]
    parts = [
        f'<svg viewBox="0 0 {width} {height}" width="{width}" height="{height}" '
        'role="img" aria-label="K0CO daily mean wind speed comparison">',
        "<style>text{font-family:Arial,sans-serif;fill:#1f2937}.axis{font-size:12px;"
        "fill:#4b5563}.title{font-size:18px;font-weight:700}.legend{font-size:13px}"
        ".grid{stroke:#e5e7eb;stroke-width:1}.axisline{stroke:#9ca3af;stroke-width:1}"
        ".series{fill:none;stroke-width:2.4}</style>",
        '<text x="560" y="28" text-anchor="middle" class="title">'
        "K0CO Daily Mean Wind Speed: Observed vs HRRR vs Adjusted HRRR</text>",
    ]
    for tick in ticks:
        y = y_for(tick)
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{width - right}" y2="{y:.1f}" class="grid"/>')
        parts.append(f'<text x="{left - 10}" y="{y + 4:.1f}" text-anchor="end" class="axis">{tick:.0f}</text>')
    parts.append(f'<line x1="{left}" y1="{height - bottom}" x2="{width - right}" y2="{height - bottom}" class="axisline"/>')
    parts.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{height - bottom}" class="axisline"/>')
    tick_step = max(1, len(daily) // 6)
    for index in range(0, len(daily), tick_step):
        day = daily[index]["date"]
        parts.append(
            f'<text x="{x_for(index):.1f}" y="{height - 40}" text-anchor="middle" '
            f'class="axis">{html.escape(day.strftime("%m-%d"))}</text>'
        )
    parts.extend([
        polyline("observed", "#111827"),
        '<line x1="70" y1="406" x2="98" y2="406" stroke="#111827" stroke-width="3"/>',
        '<text x="106" y="410" class="legend">Observed K0CO</text>',
        polyline("hrrr", "#2563eb"),
        '<line x1="260" y1="406" x2="288" y2="406" stroke="#2563eb" stroke-width="3"/>',
        '<text x="296" y="410" class="legend">HRRR</text>',
        polyline("adjusted_hrrr", "#dc2626"),
        '<line x1="450" y1="406" x2="478" y2="406" stroke="#dc2626" stroke-width="3"/>',
        '<text x="486" y="410" class="legend">Adjusted HRRR</text>',
        '<text transform="translate(18 215) rotate(-90)" text-anchor="middle" class="axis">mph</text>',
        "</svg>",
    ])
    return "\n".join(parts)


def adjustment_description(adjustment_setting: AdjustmentSetting) -> str:
    if adjustment_setting.cap_mode == "raw_10m":
        cap = (
            f"{adjustment_setting.low_cap:g}x-{adjustment_setting.high_cap:g}x "
            "raw HRRR 10 m speed"
        )
    elif adjustment_setting.cap_mode == "levels_10_80":
        cap = (
            f"{adjustment_setting.low_cap:g}x min(10 m, 80 m) through "
            f"{adjustment_setting.high_cap:g}x max(10 m, 80 m)"
        )
    else:
        cap = "no cap"
    return (
        f"{adjustment_setting.key}: GMTED2010 500 m elevation, "
        f"{adjustment_setting.blend_scale_m:g} m blend scale, {cap}"
    )


def write_hrrr_comparison_html(
    output_dir: Path,
    rows: list[dict],
    metrics: list[dict],
    start: dt.datetime,
    end: dt.datetime,
    adjustment_setting: AdjustmentSetting = ADJUSTMENT_SETTINGS["v1-current"],
) -> None:
    hrrr = metrics[0]
    adjusted = metrics[1]
    deltas = {
        "speed_mae": adjusted["speed_mae"] - hrrr["speed_mae"],
        "speed_bias": adjusted["speed_bias"] - hrrr["speed_bias"],
        "dir_mae_deg": adjusted["dir_mae_deg"] - hrrr["dir_mae_deg"],
        "vector_rmse": adjusted["vector_rmse"] - hrrr["vector_rmse"],
    }
    cards = [
        ("Samples", str(len(rows))),
        ("Speed MAE Change", f'{deltas["speed_mae"]:.2f} mph'),
        ("Vector RMSE Change", f'{deltas["vector_rmse"]:.2f} mph'),
        ("Direction MAE Change", f'{deltas["dir_mae_deg"]:.2f} deg'),
    ]
    metric_fields = [
        "result",
        "sample_count",
        "speed_bias",
        "speed_mae",
        "speed_rmse",
        "dir_mae_deg",
        "vector_rmse",
    ]
    metric_rows = "\n".join(
        "<tr>"
        + "".join(f"<td>{html.escape(metric_value(row[field]))}</td>" for field in metric_fields)
        + "</tr>"
        for row in metrics
    )
    sample_rows = "\n".join(
        "<tr>"
        f"<td>{html.escape(str(row['sample_time_utc']))}</td>"
        f"<td>{metric_value(row['observed_speed'])}</td>"
        f"<td>{metric_value(row['hrrr_speed'])}</td>"
        f"<td>{metric_value(row['adjusted_hrrr_speed'])}</td>"
        f"<td>{metric_value(row['observed_dir_deg'])}</td>"
        f"<td>{metric_value(row['hrrr_dir_deg'])}</td>"
        f"<td>{metric_value(row['adjusted_hrrr_dir_deg'])}</td>"
        "</tr>"
        for row in rows[:24]
    )
    cards_html = "\n".join(
        f'<div class="card"><div class="label">{html.escape(label)}</div>'
        f'<div class="value">{html.escape(value)}</div></div>'
        for label, value in cards
    )
    content = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>K0CO HRRR Comparison</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #111827; }}
    .wrap {{ max-width: 1180px; }}
    .note {{ color: #4b5563; line-height: 1.45; }}
    .cards {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin: 18px 0; }}
    .card {{ border: 1px solid #d1d5db; border-radius: 6px; padding: 12px; background: #f9fafb; }}
    .label {{ color: #4b5563; font-size: 12px; text-transform: uppercase; }}
    .value {{ font-size: 22px; font-weight: 700; margin-top: 6px; }}
    table {{ border-collapse: collapse; margin: 16px 0 28px; width: 100%; font-size: 13px; }}
    th, td {{ border: 1px solid #d1d5db; padding: 6px 8px; text-align: right; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ background: #f3f4f6; }}
  </style>
</head>
<body>
<div class="wrap">
  <h1>K0CO HRRR Comparison</h1>
  <p class="note">
    Full matched period: {html.escape(sv.isoformat_utc(start))} through {html.escape(sv.isoformat_utc(end))}.
    Observed is K0CO. HRRR is raw 10 m HRRR sampled at K0CO. Adjusted HRRR uses GMTED2010
    500 m elevation to blend HRRR 10 m and 80 m vectors.
    Setting: {html.escape(adjustment_description(adjustment_setting))}.
  </p>
  <p class="note">
    This HTML is HRRR-only. WindNinja still uses the high-resolution Berthoud DEM when adjusted
    HRRR is passed into gridded initialization.
  </p>
  <div class="cards">{cards_html}</div>
  {comparison_svg(rows)}
  <h2>Metrics</h2>
  <table>
    <thead><tr>{''.join(f'<th>{html.escape(field)}</th>' for field in metric_fields)}</tr></thead>
    <tbody>{metric_rows}</tbody>
  </table>
  <h2>First 24 Hourly Samples</h2>
  <table>
    <thead>
      <tr><th>time UTC</th><th>obs speed</th><th>HRRR speed</th><th>adjusted speed</th>
      <th>obs dir</th><th>HRRR dir</th><th>adjusted dir</th></tr>
    </thead>
    <tbody>{sample_rows}</tbody>
  </table>
  <p class="note">Source CSV: hrrr_comparison_samples.csv</p>
</div>
</body>
</html>
"""
    (output_dir / "hrrr_comparison.html").write_text(content, encoding="utf-8")
    if output_dir.name == HEIGHT_STUDY_KEY:
        (output_dir / "hrrr_observed_adjusted_view.html").write_text(content, encoding="utf-8")


def is_documented_period(start: dt.datetime, end: dt.datetime) -> bool:
    return ymdhm(start) == DOCUMENTED_START and ymdhm(end) == DOCUMENTED_END


def hrrr_comparison_output_dirs(
    validation_root: Path,
    start: dt.datetime,
    end: dt.datetime,
) -> list[Path]:
    period_dir = validation_root / "hrrr_comparisons" / f"{ymdhm(start)}_{ymdhm(end)}"
    output_dirs = [period_dir]
    if is_documented_period(start, end):
        output_dirs.append(validation_root)
    return output_dirs


def write_hrrr_comparison_files(
    output_dir: Path,
    rows: list[dict],
    start: dt.datetime,
    end: dt.datetime,
    adjustment_setting: AdjustmentSetting = ADJUSTMENT_SETTINGS["v1-current"],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    samples_csv = output_dir / "hrrr_comparison_samples.csv"
    metrics_csv = output_dir / "hrrr_comparison_metrics.csv"
    summary_json = output_dir / "hrrr_comparison_summary.json"
    sv.rows_to_csv(samples_csv, rows)
    metrics = [
        metric_row("hrrr", rows, "hrrr"),
        metric_row("adjusted_hrrr", rows, "adjusted_hrrr"),
    ]
    with metrics_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(metrics[0].keys()))
        writer.writeheader()
        writer.writerows(metrics)
    write_hrrr_comparison_html(output_dir, rows, metrics, start, end, adjustment_setting)
    hrrr, adjusted = metrics
    sv.write_json(
        summary_json,
        {
            "generated_at_utc": sv.isoformat_utc(dt.datetime.now(UTC)),
            "station": "K0CO",
            "adjustment_setting": adjustment_setting.key,
            "adjustment_description": adjustment_description(adjustment_setting),
            "start_utc": sv.isoformat_utc(start),
            "end_utc": sv.isoformat_utc(end),
            "sample_count": len(rows),
            "metrics": {"hrrr": hrrr, "adjusted_hrrr": adjusted},
            "adjusted_hrrr_vs_hrrr_delta": {
                "speed_mae": adjusted["speed_mae"] - hrrr["speed_mae"],
                "speed_bias": adjusted["speed_bias"] - hrrr["speed_bias"],
                "dir_mae_deg": adjusted["dir_mae_deg"] - hrrr["dir_mae_deg"],
                "vector_rmse": adjusted["vector_rmse"] - hrrr["vector_rmse"],
                "note": "negative means adjusted HRRR is lower/better",
            },
            "outputs": {
                "samples_csv": str(samples_csv),
                "metrics_csv": str(metrics_csv),
            },
        },
    )


def write_hrrr_comparison(
    validation_root: Path,
    rows: list[dict],
    start: dt.datetime,
    end: dt.datetime,
    adjustment_setting: AdjustmentSetting = ADJUSTMENT_SETTINGS["v1-current"],
) -> None:
    for output_dir in hrrr_comparison_output_dirs(validation_root, start, end):
        write_hrrr_comparison_files(output_dir, rows, start, end, adjustment_setting)


def copy_prj(source: Path, target: Path) -> None:
    prj = source.with_suffix(".prj")
    if prj.exists():
        shutil.copy2(prj, target.with_suffix(".prj"))


def install_validation_rasters(run_dir: Path, run_time: dt.datetime, forcing_paths: HourForcingPaths) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    label = run_time.strftime("%Y%m%d_%H%M")
    speed_target = run_dir / f"HEIGHT-HRRR-{label}_vel.asc"
    direction_target = run_dir / f"HEIGHT-HRRR-{label}_ang.asc"
    shutil.copy2(forcing_paths.speed_mph, speed_target)
    shutil.copy2(forcing_paths.speed_mph.parent / "direction.asc", direction_target)
    copy_prj(forcing_paths.speed_mph, speed_target)
    copy_prj(forcing_paths.speed_mph.parent / "direction.asc", direction_target)


def install_windninja_validation_names(run_dir: Path, run_time: dt.datetime, domain: str) -> None:
    label = run_time.strftime("%Y%m%d_%H%M")
    for kind in ("vel", "ang"):
        candidates = [
            run_dir / f"{domain}_100m_{kind}.asc",
            run_dir / f"{domain}_{kind}.asc",
        ]
        candidates.extend(
            path
            for path in sorted(run_dir.glob(f"*_100m_{kind}.asc"))
            if not path.name.startswith("HEIGHT-HRRR-")
        )
        candidates.extend(
            path
            for path in sorted(run_dir.glob(f"*_{kind}.asc"))
            if not path.name.startswith("HEIGHT-HRRR-")
        )
        source = next((path for path in candidates if path.exists()), None)
        if source is not None:
            target = run_dir / f"{domain}_{label}_100m_{kind}.asc"
            if source.resolve() != target.resolve():
                shutil.copy2(source, target)
                copy_prj(source, target)


def run_grid_hour(
    run_time: dt.datetime,
    study: vs.StudyConfig,
    forcing_paths: HourForcingPaths,
    *,
    force: bool,
    skip_runs: bool,
    adjustment_setting: AdjustmentSetting = ADJUSTMENT_SETTINGS["v1-current"],
    solver: str = MOMENTUM_SOLVER,
) -> Path:
    windninja_domain = windninja_domain_for_solver(study.domain, solver)
    windninja_label = windninja_label_for_solver(adjustment_setting, solver)
    run_dir = Path(config_loader.TEMP_DIR) / build_grid_output_dir_name(
        windninja_domain,
        run_time.replace(tzinfo=None),
        windninja_label,
    )
    if run_dir.exists() and not force:
        install_validation_rasters(run_dir, run_time, forcing_paths)
        install_windninja_validation_names(run_dir, run_time, windninja_domain)
        if _has_complete_validation_set(run_dir, run_time):
            logger.info(f"Using existing adjusted WindNinja run: {run_dir}")
            cleanup_ninjafoam_caches(windninja_domain)
            return run_dir
    if skip_runs:
        cleanup_ninjafoam_caches(windninja_domain)
        return run_dir
    if run_dir.exists():
        shutil.rmtree(run_dir)
    cleanup_ninjafoam_caches(windninja_domain)
    command = [
        sys.executable,
        str(config_loader.SCRIPTS_DIR / "gridded_run.py"),
        "--speed-grid",
        str(forcing_paths.speed_mps),
        "--direction-grid",
        str(forcing_paths.direction),
        "--time",
        ymdhm(run_time),
        "--domain",
        windninja_domain,
        "--height",
        "10.0",
        "--label",
        windninja_label,
        "--keep-temp",
        "--no-upload",
    ]
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError:
        cleanup_ninjafoam_caches(windninja_domain)
        raise
    install_validation_rasters(run_dir, run_time, forcing_paths)
    install_windninja_validation_names(run_dir, run_time, windninja_domain)
    cleanup_ninjafoam_caches(windninja_domain)
    return run_dir


def _has_complete_validation_set(run_dir: Path, run_time: dt.datetime) -> bool:
    if not run_dir.exists():
        return False
    return run_time.astimezone(UTC) in raster_validation.collect_raster_sets(run_dir)


def iter_chunk_hours(chunk: vs.Chunk) -> list[dt.datetime]:
    hours = []
    cursor = chunk.start
    while cursor < chunk.end:
        hours.append(cursor)
        cursor += dt.timedelta(hours=1)
    return hours


def mode_chunk_paths(validation_root: Path, chunk: vs.Chunk, mode: str) -> dict[str, Path]:
    chunk_dir = validation_root / "chunks" / chunk.label / mode
    return {
        "dir": chunk_dir,
        "rasters": chunk_dir / "rasters",
        "samples": chunk_dir / "samples.csv",
        "station_summary": chunk_dir / "station_summary.csv",
        "group_summary": chunk_dir / "group_summary.csv",
        "summary": chunk_dir / "summary.json",
    }


def adjusted_mode_name(
    adjustment_setting: AdjustmentSetting,
    solver: str = MOMENTUM_SOLVER,
) -> str:
    return f"height_adjusted_hrrr{adjustment_setting.output_suffix}{solver_label_suffix(solver)}"


def mode_validation_is_complete(validation_root: Path, chunk: vs.Chunk, mode: str) -> bool:
    paths = mode_chunk_paths(validation_root, chunk, mode)
    return paths["summary"].exists() and paths["samples"].exists()


def stage_validation_rasters(run_dirs: list[Path], stage_dir: Path, *, force: bool) -> None:
    if stage_dir.exists() and force:
        shutil.rmtree(stage_dir)
    stage_dir.mkdir(parents=True, exist_ok=True)
    for run_dir in run_dirs:
        if not run_dir.exists():
            raise HeightHrrrError(f"Missing adjusted WindNinja run directory: {run_dir}")
        for source in sorted(run_dir.glob("*.asc")) + sorted(run_dir.glob("*.prj")):
            target = stage_dir / source.name
            if target.exists() and not force:
                continue
            shutil.copy2(source, target)


def validate_grid_chunk(
    study: vs.StudyConfig,
    chunk: vs.Chunk,
    validation_root: Path,
    run_dirs: list[Path],
    *,
    force: bool,
    adjustment_setting: AdjustmentSetting = ADJUSTMENT_SETTINGS["v1-current"],
    solver: str = MOMENTUM_SOLVER,
) -> Path:
    paths = mode_chunk_paths(validation_root, chunk, adjusted_mode_name(adjustment_setting, solver))
    if paths["summary"].exists() and paths["samples"].exists() and not force:
        return paths["samples"]
    stage_validation_rasters(run_dirs, paths["rasters"], force=force)
    command = [
        sys.executable,
        str(config_loader.SCRIPTS_DIR / "raster_validation.py"),
        "--run-dir",
        str(paths["rasters"]),
        "--metadata-file",
        str(study.metadata_file),
        "--start",
        ymdhm(chunk.start),
        "--end",
        ymdhm(chunk.end),
        "--samples-csv",
        str(paths["samples"]),
        "--station-summary-csv",
        str(paths["station_summary"]),
        "--group-summary-csv",
        str(paths["group_summary"]),
        "--summary-json",
        str(paths["summary"]),
        "--tolerance-minutes",
        str(study.tolerance_minutes),
        "--speed-units",
        study.speed_units,
        "--allow-empty",
    ]
    subprocess.run(command, check=True)
    return paths["samples"]


def summarize_windninja_outputs(
    validation_root: Path,
    native_paths: list[Path],
    adjusted_paths: list[Path],
    start: dt.datetime,
    end: dt.datetime,
    solver: str = MOMENTUM_SOLVER,
) -> None:
    native_rows = dedupe_sample_rows(filter_rows_for_window(vs.load_sample_rows(native_paths), start, end))
    adjusted_rows = dedupe_sample_rows(filter_rows_for_window(vs.load_sample_rows(adjusted_paths), start, end))
    native_summary = sv.summarize_samples(native_rows)
    adjusted_summary = sv.summarize_samples(adjusted_rows)
    metrics = [
        {
            "result": "hrrr",
            **metric_row("hrrr", [
                {
                    "hrrr_speed_error": row["wx_speed_error"],
                    "hrrr_dir_abs_error_deg": row["wx_dir_abs_error_deg"],
                    "hrrr_vector_error": row["wx_vector_error"],
                }
                for row in native_rows
            ], "hrrr"),
        },
        {
            "result": "adjusted_hrrr",
            **metric_row("adjusted_hrrr", [
                {
                    "adjusted_hrrr_speed_error": row["wx_speed_error"],
                    "adjusted_hrrr_dir_abs_error_deg": row["wx_dir_abs_error_deg"],
                    "adjusted_hrrr_vector_error": row["wx_vector_error"],
                }
                for row in adjusted_rows
            ], "adjusted_hrrr"),
        },
    ]
    output_suffix = solver_label_suffix(solver)
    comparison_csv = validation_root / f"comparison_metrics{output_suffix}.csv"
    adjusted_windninja_result = (
        "windninja_from_adjusted_hrrr"
        if solver == MOMENTUM_SOLVER
        else f"windninja_{solver}_from_adjusted_hrrr"
    )
    with comparison_csv.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = [
            "result",
            "sample_count",
            "speed_bias",
            "speed_mae",
            "speed_rmse",
            "dir_mae_deg",
            "vector_mae",
            "vector_rmse",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow({"result": "hrrr", **native_summary["hrrr"], "sample_count": native_summary["sample_count"]})
        writer.writerow({"result": "adjusted_hrrr", **adjusted_summary["hrrr"], "sample_count": adjusted_summary["sample_count"]})
        writer.writerow({"result": "windninja_from_hrrr", **native_summary["windninja"], "sample_count": native_summary["sample_count"]})
        writer.writerow({"result": adjusted_windninja_result, **adjusted_summary["windninja"], "sample_count": adjusted_summary["sample_count"]})
    sv.write_json(
        validation_root / f"comparison_summary{output_suffix}.json",
        {
            "generated_at_utc": sv.isoformat_utc(dt.datetime.now(UTC)),
            "windninja_solver": solver,
            "metrics": {
                "native_hrrr_run": native_summary,
                "adjusted_hrrr_run": adjusted_summary,
            },
            "outputs": {"comparison_metrics_csv": str(comparison_csv)},
        },
    )
    _ = metrics


def print_plan(
    height_study: vs.StudyConfig,
    chunks: list[vs.Chunk],
    adjustment_setting: AdjustmentSetting,
    solver: str,
) -> None:
    hour_count = sum(len(iter_chunk_hours(chunk)) for chunk in chunks)
    windninja_domain = windninja_domain_for_solver(height_study.domain, solver)
    print(json.dumps({
        "study": HEIGHT_STUDY_KEY,
        "adjustment_setting": adjustment_setting.key,
        "windninja_solver": solver,
        "windninja_domain": windninja_domain,
        "windninja_label": windninja_label_for_solver(adjustment_setting, solver),
        "windninja_validation_mode": adjusted_mode_name(adjustment_setting, solver),
        "validation_root": str(height_study.validation_root),
        "chunk_count": len(chunks),
        "hour_count": hour_count,
        "external_grid_hour_count": hour_count,
        "height_adjusted_grid_included": True,
        "windninja_grid_run_count": hour_count,
        "grid": "GMTED 500 m adjusted HRRR grid",
        "elevation_source": "GMTED2010",
        "adjustment_resolution_m": GMTED_RESOLUTION_M,
        "blend_scale_m": adjustment_setting.blend_scale_m,
        "cap_mode": adjustment_setting.cap_mode,
        "low_cap": adjustment_setting.low_cap,
        "high_cap": adjustment_setting.high_cap,
        "exposure_radius_m": adjustment_setting.exposure_radius_m,
        "exposure_inner_skip_m": (
            adjustment_setting.exposure_inner_skip_m
            if adjustment_setting.exposure_radius_m is not None
            else None
        ),
        "full_exposure_tpi_m": (
            adjustment_setting.full_exposure_tpi_m
            if adjustment_setting.exposure_radius_m is not None
            else None
        ),
        "windninja_adjusted_run_count": hour_count,
    }, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="K0CO height-aware HRRR validation.")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--chunk-hours", type=int, default=24)
    parser.add_argument("--validation-root")
    parser.add_argument(
        "--adjustment-setting",
        choices=sorted(ADJUSTMENT_SETTINGS),
        default="v1-current",
        help=(
            "Adjustment recipe. The balanced option is the next K0CO candidate "
            "from HRRR-only tuning."
        ),
    )
    parser.add_argument("--archive-base-url", default=DEFAULT_ARCHIVE_BASE_URL)
    parser.add_argument("--token")
    parser.add_argument("--plan", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--force-native", action="store_true")
    parser.add_argument("--skip-native", action="store_true")
    parser.add_argument("--skip-runs", action="store_true")
    parser.add_argument("--hrrr-only", action="store_true")
    parser.add_argument(
        "--windninja-solver",
        choices=SOLVER_CHOICES,
        default=MOMENTUM_SOLVER,
        help=(
            "WindNinja solver for adjusted gridded runs. 'mass' uses the "
            "matching *_mass domain/template and writes separate mass-labeled outputs."
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Parallel workers for independent hourly adjusted HRRR field builds.",
    )
    parser.add_argument("--no-preflight", action="store_true")
    return parser


def load_native_rows(source_study: vs.StudyConfig, chunks: list[vs.Chunk], args) -> tuple[list[Path], list[dict]]:
    paths = native_sample_paths_for_study(source_study, chunks) if not args.force_native else []
    if not paths and not args.skip_native:
        for chunk in chunks:
            run_dir = vs.run_reanalysis_chunk(source_study, chunk, force=args.force_native, dry_run=False)
            paths.append(vs.validate_chunk(source_study, chunk, run_dir, force=args.force_native, dry_run=False))
    rows = dedupe_sample_rows(
        filter_rows_for_window(vs.load_sample_rows(paths), vs.parse_utc(args.start), vs.parse_utc(args.end))
    )
    return paths, rows


def reuse_station_inputs_if_available(
    height_study: vs.StudyConfig,
    source_study: vs.StudyConfig,
) -> bool:
    if height_study.metadata_file.exists() and height_study.bbox_file.exists():
        return True
    candidates = [
        (source_study.metadata_file, source_study.bbox_file),
        (
            vs.resolve_repo_path(default_validation_root(ADJUSTMENT_SETTINGS["v1-current"]))
            / "station_metadata.json",
            vs.resolve_repo_path(default_validation_root(ADJUSTMENT_SETTINGS["v1-current"]))
            / "station_bbox.json",
        ),
    ]
    for metadata_source, bbox_source in candidates:
        if metadata_source.exists() and bbox_source.exists():
            height_study.metadata_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(metadata_source, height_study.metadata_file)
            shutil.copy2(bbox_source, height_study.bbox_file)
            logger.info(f"Reused station metadata from {metadata_source.parent}")
            return True
    return False


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    adjustment_setting = ADJUSTMENT_SETTINGS[args.adjustment_setting]
    source_study = vs.load_study_config(STUDY_KEY)
    validation_root_arg = args.validation_root or default_validation_root(adjustment_setting)
    validation_root = vs.resolve_repo_path(validation_root_arg)
    height_study = replace(
        source_study,
        key=HEIGHT_STUDY_KEY,
        label=f"Berthoud Pass K0CO height-aware HRRR ({adjustment_setting.key})",
        validation_root=validation_root,
        metadata_file=validation_root / "station_metadata.json",
        bbox_file=validation_root / "station_bbox.json",
    )
    start = vs.parse_utc(args.start)
    end = vs.parse_utc(args.end)
    chunks = vs.plan_chunks(start, end, args.chunk_hours)
    windninja_domain_for_solver(height_study.domain, args.windninja_solver)
    if args.plan:
        print_plan(height_study, chunks, adjustment_setting, args.windninja_solver)
        return 0
    if not args.no_preflight:
        vs.run_preflight(source_study)
    if args.force or not reuse_station_inputs_if_available(height_study, source_study):
        vs.ensure_station_inputs(height_study, start, end, args.token)
    if not source_study.metadata_file.exists() and not args.skip_native:
        vs.ensure_station_inputs(source_study, start, end, args.token)

    native_paths, native_rows = load_native_rows(source_study, chunks, args)
    if not native_rows:
        logger.error("No K0CO native HRRR samples found for this window.")
        return 1
    station = json.loads(height_study.metadata_file.read_text(encoding="utf-8"))["stations"][0]
    domain = config_loader.get_gridded_domain_config(height_study.domain)
    terrain = forcing._terrain_grid(domain.elevation_file)
    ensure_gmted_adjustment_grid(height_study.domain, terrain, validation_root, force=args.force)
    logger.info(
        f"Building adjusted HRRR comparison for {len(native_rows)} matched K0CO hours. "
        "ETA pending first 24 hours."
    )
    start_wall = time.monotonic()
    comparison_rows: list[dict | None] = [None] * len(native_rows)

    def build_one(index: int, row: dict) -> tuple[int, dict]:
        run_time = sample_time(row)
        forcing_paths = prepare_adjusted_hrrr_hour(
            run_time,
            height_study,
            validation_root,
            archive_base_url=args.archive_base_url,
            force=args.force,
            adjustment_setting=adjustment_setting,
        )
        return index, sample_adjusted_hrrr(row, station, forcing_paths)

    workers = max(args.workers, 1)
    try:
        if workers == 1:
            for index, row in enumerate(native_rows):
                result_index, sample = build_one(index, row)
                comparison_rows[result_index] = sample
                completed = index + 1
                if completed == 1 or completed % 24 == 0 or completed == len(native_rows):
                    elapsed = time.monotonic() - start_wall
                    eta = ""
                    if completed >= 24:
                        remaining = elapsed / completed * (len(native_rows) - completed)
                        eta = f"; ETA remaining {remaining / 3600.0:.1f} hours"
                    logger.info(f"Built adjusted HRRR comparison {completed}/{len(native_rows)}{eta}")
        else:
            logger.info(f"Using {workers} parallel HRRR field workers.")
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(build_one, index, row): index
                    for index, row in enumerate(native_rows)
                }
                for completed, future in enumerate(
                    concurrent.futures.as_completed(futures),
                    start=1,
                ):
                    result_index, sample = future.result()
                    comparison_rows[result_index] = sample
                    if completed == 1 or completed % 24 == 0 or completed == len(native_rows):
                        elapsed = time.monotonic() - start_wall
                        eta = ""
                        if completed >= 24:
                            remaining = elapsed / completed * (len(native_rows) - completed)
                            eta = f"; ETA remaining {remaining / 3600.0:.1f} hours"
                        logger.info(
                            f"Built adjusted HRRR comparison {completed}/{len(native_rows)}{eta}"
                        )
    except HeightHrrrError as exc:
        logger.error(f"Adjusted HRRR comparison failed: {exc}")
        return 1
    write_hrrr_comparison(
        validation_root,
        [row for row in comparison_rows if row is not None],
        start,
        end,
        adjustment_setting,
    )
    if args.hrrr_only:
        return 0

    sample_times = {sample_time(row) for row in native_rows}
    adjusted_sample_paths: list[Path] = []
    for chunk in chunks:
        run_dirs = []
        for hour in [value for value in iter_chunk_hours(chunk) if value in sample_times]:
            forcing_paths = prepare_adjusted_hrrr_hour(
                hour,
                height_study,
                validation_root,
                archive_base_url=args.archive_base_url,
                force=False,
                adjustment_setting=adjustment_setting,
            )
            try:
                run_dirs.append(
                    run_grid_hour(
                        hour,
                        height_study,
                        forcing_paths,
                        force=args.force,
                        skip_runs=args.skip_runs,
                        adjustment_setting=adjustment_setting,
                        solver=args.windninja_solver,
                    )
                )
            except subprocess.CalledProcessError as exc:
                logger.error(f"Adjusted WindNinja hour failed for {ymdhm(hour)}: {exc}")
                return exc.returncode
        if run_dirs:
            adjusted_sample_paths.append(
                validate_grid_chunk(
                    height_study,
                    chunk,
                    validation_root,
                    run_dirs,
                    force=args.force,
                    adjustment_setting=adjustment_setting,
                    solver=args.windninja_solver,
                )
            )
    summarize_windninja_outputs(
        validation_root,
        native_paths,
        adjusted_sample_paths,
        start,
        end,
        solver=args.windninja_solver,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
