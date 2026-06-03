"""Validate Breck/Tenmile residual U-Net candidates against Synoptic stations.

This runner is intentionally outside the operational ``mwn.sh`` path. It uses
existing GCS mass/momentum HRRR pairs, applies selected residual U-Net
checkpoints, samples point stations, and writes compact validation products.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Callable

from scripts import raster_validation as rv
from scripts import synoptic_validation as sv

from . import infer as ml_infer
from .pairing import parse_run_label


UTC = dt.timezone.utc

DEFAULT_BUCKET = "mwn-ml-general-9p6-spring-nova-475120-r0"
DEFAULT_MOMENTUM_DOMAIN = "breck_tenmile_9p6"
DEFAULT_MASS_DOMAIN = "breck_tenmile_9p6_mass"
DEFAULT_MODEL = "HRRR"
DEFAULT_SPEED_UNITS = "mph"
DEFAULT_STATION_MANIFEST = Path("config/stations/breck_tenmile_ml_validation_manifest.csv")
DEFAULT_OUTPUT_ROOT = Path("runtime/ml/residual_unet/validation/breck_synoptic")
DEFAULT_GCS_OUTPUT_PREFIX = "validation/breck_synoptic"
DEFAULT_CROP_SIZE = 96
DEFAULT_TOLERANCE_MINUTES = 30

DEFAULT_MODEL_NAMES = (
    "breck_tenmile_9p6_specific_lcp_canopy_v2_hrrr_resunet32",
    "breck_tenmile_9p6_specific_lcp_canopy_v2_hrrr_unet64",
    "breck_tenmile_9p6_specific_lcp_canopy_v2",
)
DEFAULT_MODEL_CHECKPOINTS = {
    name: Path("ml/residual_unet/colab/results") / name / "best.pt"
    for name in DEFAULT_MODEL_NAMES
}

STATION_AVAILABILITY = {
    "CABP6": (
        dt.datetime(2021, 11, 10, 18, 0, tzinfo=UTC),
        dt.datetime(2026, 4, 5, 17, 0, tzinfo=UTC),
    ),
    "CABP8": (
        dt.datetime(2014, 12, 8, 21, 34, tzinfo=UTC),
        dt.datetime(2026, 4, 13, 16, 0, tzinfo=UTC),
    ),
    "CAHSB": (
        dt.datetime(2021, 4, 20, 22, 6, tzinfo=UTC),
        dt.datetime(2026, 4, 20, 16, 0, tzinfo=UTC),
    ),
}

PARENT_PREFIXES = ("PASTCAST-", "NOMADS-", "HEIGHT-HRRR")
IGNORED_PREFIXES = (*PARENT_PREFIXES, "GENERIC-")
GCS_RUN_RE = re.compile(
    r"^(?P<domain>.+)_(?P<start>\d{8}_\d{4})_reanalysis_"
    r"(?P<hours>\d+)h_(?P<model>[A-Za-z0-9_-]+)$"
)
WIND_RASTER_RE = re.compile(r"^(?P<base>.+)_(?P<kind>vel|ang)\.asc$")


@dataclass(frozen=True)
class GcsRun:
    uri: str
    run_name: str
    domain: str
    start: dt.datetime
    hours: int
    model: str

    @property
    def key(self) -> tuple[dt.datetime, int, str]:
        return (self.start, self.hours, self.model)

    @property
    def end(self) -> dt.datetime:
        return self.start + dt.timedelta(hours=self.hours)


@dataclass(frozen=True)
class GcsPair:
    mass: GcsRun
    momentum: GcsRun

    @property
    def key(self) -> tuple[dt.datetime, int, str]:
        return self.mass.key


@dataclass(frozen=True)
class RasterInventory:
    solver_timestamps: frozenset[dt.datetime]
    parent_timestamps: frozenset[dt.datetime]


@dataclass(frozen=True)
class Coverage:
    pair: GcsPair
    mass_solver_count: int
    momentum_solver_count: int
    parent_hrrr_count: int
    paired_timestamp_count: int
    status: str
    reason: str


@dataclass(frozen=True)
class RasterSource:
    speed_path: Path
    direction_path: Path


def parse_utc(value: str) -> dt.datetime:
    raw = value.strip()
    for fmt in ("%Y%m%d%H%M", "%Y%m%d_%H%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M"):
        try:
            return dt.datetime.strptime(raw, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    raise ValueError(f"Expected UTC timestamp, got {value!r}")


def ymdhm(value: dt.datetime) -> str:
    return value.astimezone(UTC).strftime("%Y%m%d%H%M")


def iso(value: dt.datetime) -> str:
    return sv.isoformat_utc(value)


def parse_iso(value: str) -> dt.datetime:
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    parsed = dt.datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def safe_source_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_").lower()


def parse_gcs_run_uri(uri: str) -> GcsRun:
    normalized = uri.strip().rstrip("/")
    parts = normalized.split("/")
    match = None
    run_name = ""
    run_index = -1
    for index in range(len(parts) - 1, -1, -1):
        candidate = parts[index]
        match = GCS_RUN_RE.match(candidate)
        if match:
            run_name = candidate
            run_index = index
            break
    if match is None:
        raise ValueError(f"Not a WindNinja reanalysis run URI: {uri}")
    run_uri = "/".join(parts[:run_index + 1])
    start = dt.datetime.strptime(match.group("start"), "%Y%m%d_%H%M").replace(tzinfo=UTC)
    return GcsRun(
        uri=run_uri,
        run_name=run_name,
        domain=match.group("domain"),
        start=start,
        hours=int(match.group("hours")),
        model=match.group("model"),
    )


def station_window_overlaps(start: dt.datetime, end: dt.datetime) -> bool:
    for available_start, available_end in STATION_AVAILABILITY.values():
        if start <= available_end and end >= available_start:
            return True
    return False


def pair_gcs_runs(
    mass_uris: list[str],
    momentum_uris: list[str],
    *,
    mass_domain: str = DEFAULT_MASS_DOMAIN,
    momentum_domain: str = DEFAULT_MOMENTUM_DOMAIN,
    model: str = DEFAULT_MODEL,
    start: dt.datetime | None = None,
    end: dt.datetime | None = None,
) -> list[GcsPair]:
    parsed_mass_runs = [parse_gcs_run_uri(uri) for uri in mass_uris]
    parsed_momentum_runs = [parse_gcs_run_uri(uri) for uri in momentum_uris]
    mass_runs = [run for run in parsed_mass_runs if run.domain == mass_domain]
    momentum_runs = [run for run in parsed_momentum_runs if run.domain == momentum_domain]
    mass_by_key = {run.key: run for run in sorted(mass_runs, key=lambda item: item.uri)}
    momentum_by_key = {run.key: run for run in sorted(momentum_runs, key=lambda item: item.uri)}
    pairs = []
    for key in sorted(set(mass_by_key) & set(momentum_by_key)):
        pair = GcsPair(mass=mass_by_key[key], momentum=momentum_by_key[key])
        if model and pair.mass.model != model:
            continue
        if start is not None and pair.mass.end <= start:
            continue
        if end is not None and pair.mass.start >= end:
            continue
        if not station_window_overlaps(pair.mass.start, pair.mass.end):
            continue
        pairs.append(pair)
    return pairs


def raster_timestamp_from_name(path_name: str) -> dt.datetime | None:
    base = path_name.removesuffix("_vel.asc").removesuffix("_ang.asc")
    matches = list(ml_infer.TIMESTAMP_RE.finditer(base))
    if not matches:
        return None
    return parse_run_label(matches[-1].group("label"))


def collect_raster_inventory(paths: list[str]) -> RasterInventory:
    solver: dict[dt.datetime, set[str]] = {}
    parent: dict[dt.datetime, set[str]] = {}
    for raw_path in paths:
        name = raw_path.rstrip("/").rsplit("/", 1)[-1]
        match = WIND_RASTER_RE.match(name)
        if not match:
            continue
        timestamp = raster_timestamp_from_name(name)
        if timestamp is None:
            continue
        destination = parent if name.startswith(PARENT_PREFIXES) else solver
        if name.startswith("GENERIC-"):
            continue
        destination.setdefault(timestamp, set()).add(match.group("kind"))

    return RasterInventory(
        solver_timestamps=frozenset(
            timestamp for timestamp, kinds in solver.items() if {"vel", "ang"} <= kinds
        ),
        parent_timestamps=frozenset(
            timestamp for timestamp, kinds in parent.items() if {"vel", "ang"} <= kinds
        ),
    )


def coverage_for_pair(pair: GcsPair, mass_paths: list[str], momentum_paths: list[str]) -> Coverage:
    mass_inventory = collect_raster_inventory(mass_paths)
    momentum_inventory = collect_raster_inventory(momentum_paths)
    paired = (
        mass_inventory.solver_timestamps
        & momentum_inventory.solver_timestamps
        & mass_inventory.parent_timestamps
    )
    missing = []
    if len(mass_inventory.solver_timestamps) < pair.mass.hours:
        missing.append("mass_solver")
    if len(momentum_inventory.solver_timestamps) < pair.momentum.hours:
        missing.append("momentum_solver")
    if len(mass_inventory.parent_timestamps) < pair.mass.hours:
        missing.append("parent_hrrr")
    if len(paired) < pair.mass.hours:
        missing.append("paired_timestamps")
    status = "complete" if not missing else "incomplete"
    return Coverage(
        pair=pair,
        mass_solver_count=len(mass_inventory.solver_timestamps),
        momentum_solver_count=len(momentum_inventory.solver_timestamps),
        parent_hrrr_count=len(mass_inventory.parent_timestamps),
        paired_timestamp_count=len(paired),
        status=status,
        reason="ok" if status == "complete" else ",".join(missing),
    )


def gcloud_ls(uri: str, *, allow_empty: bool = False) -> list[str]:
    result = subprocess.run(
        ["gcloud", "storage", "ls", uri],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        empty_markers = ("matched no objects", "No URLs matched", "not found")
        if allow_empty and any(marker in result.stderr for marker in empty_markers):
            return []
        raise subprocess.CalledProcessError(
            result.returncode,
            ["gcloud", "storage", "ls", uri],
            output=result.stdout,
            stderr=result.stderr,
        )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def run_listing_patterns(
    run_prefix: str,
    domain: str,
    model: str,
    start: dt.datetime | None,
    end: dt.datetime | None,
) -> list[str]:
    if start is None or end is None:
        return [f"{run_prefix}/{domain}_*"]
    if start >= end:
        raise ValueError("--end must be later than --start.")
    if end - start > dt.timedelta(days=31):
        return [f"{run_prefix}/{domain}_*"]

    patterns = []
    cursor = start
    while cursor < end:
        patterns.append(
            f"{run_prefix}/{domain}_{cursor.strftime('%Y%m%d_%H%M')}_reanalysis_*h_{model}"
        )
        cursor += dt.timedelta(days=1)
    return patterns


def list_gcs_runs(
    run_prefix: str,
    domain: str,
    model: str,
    start: dt.datetime | None,
    end: dt.datetime | None,
) -> list[str]:
    uris = []
    for pattern in run_listing_patterns(run_prefix, domain, model, start, end):
        uris.extend(gcloud_ls(pattern, allow_empty=True))
    return sorted(set(uris))


def gcloud_cp_recursive(src: str, dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(["gcloud", "storage", "cp", "-r", src, str(dest_dir)], check=True)


def gcloud_rsync(src_dir: Path, dest_uri: str) -> None:
    subprocess.run(["gcloud", "storage", "rsync", "-r", str(src_dir), dest_uri], check=True)


def write_csv(path: Path, rows: list[dict], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        if fieldnames:
            with path.open("w", encoding="utf-8", newline="") as handle:
                csv.DictWriter(handle, fieldnames=fieldnames).writeheader()
        else:
            path.write_text("", encoding="utf-8")
        return
    fields = fieldnames or list(rows[0])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def append_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def inventory_rows(pairs: list[GcsPair]) -> list[dict]:
    return [
        {
            "start_utc": iso(pair.mass.start),
            "end_utc": iso(pair.mass.end),
            "hours": pair.mass.hours,
            "model": pair.mass.model,
            "mass_run": pair.mass.run_name,
            "momentum_run": pair.momentum.run_name,
            "mass_gcs_uri": pair.mass.uri,
            "momentum_gcs_uri": pair.momentum.uri,
        }
        for pair in pairs
    ]


def coverage_rows(coverages: list[Coverage]) -> list[dict]:
    return [
        {
            "start_utc": iso(item.pair.mass.start),
            "end_utc": iso(item.pair.mass.end),
            "hours": item.pair.mass.hours,
            "status": item.status,
            "reason": item.reason,
            "mass_solver_count": item.mass_solver_count,
            "momentum_solver_count": item.momentum_solver_count,
            "parent_hrrr_count": item.parent_hrrr_count,
            "paired_timestamp_count": item.paired_timestamp_count,
            "mass_gcs_uri": item.pair.mass.uri,
            "momentum_gcs_uri": item.pair.momentum.uri,
        }
        for item in coverages
    ]


def collect_local_solver_sets(run_dir: Path) -> dict[dt.datetime, RasterSource]:
    sources = {}
    for speed_path in sorted(run_dir.glob("*_vel.asc")):
        if speed_path.name.startswith(IGNORED_PREFIXES):
            continue
        timestamp = raster_timestamp_from_name(speed_path.name)
        if timestamp is None:
            continue
        direction_path = speed_path.with_name(speed_path.name.replace("_vel.asc", "_ang.asc"))
        if direction_path.exists():
            sources[timestamp] = RasterSource(speed_path=speed_path, direction_path=direction_path)
    return sources


def collect_local_parent_sets(run_dir: Path) -> dict[dt.datetime, RasterSource]:
    sources = {}
    for speed_path in sorted(run_dir.glob("*_vel.asc")):
        if not speed_path.name.startswith(PARENT_PREFIXES):
            continue
        timestamp = raster_timestamp_from_name(speed_path.name)
        if timestamp is None:
            continue
        direction_path = speed_path.with_name(speed_path.name.replace("_vel.asc", "_ang.asc"))
        if direction_path.exists():
            sources[timestamp] = RasterSource(speed_path=speed_path, direction_path=direction_path)
    return sources


def collect_ml_sets(metadata_path: Path) -> dict[dt.datetime, RasterSource]:
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    sources = {}
    for row in metadata.get("samples", []):
        timestamp_raw = row.get("timestamp_utc")
        if not timestamp_raw:
            continue
        speed_path = Path(row["corrected_speed_path"])
        direction_path = Path(row["corrected_direction_path"])
        if speed_path.exists() and direction_path.exists():
            sources[parse_iso(timestamp_raw)] = RasterSource(
                speed_path=speed_path,
                direction_path=direction_path,
            )
    return sources


def build_source_sets(
    mass_run_dir: Path,
    momentum_run_dir: Path,
    ml_metadata_by_model: dict[str, Path],
) -> dict[dt.datetime, dict[str, RasterSource]]:
    mass_sets = collect_local_solver_sets(mass_run_dir)
    momentum_sets = collect_local_solver_sets(momentum_run_dir)
    hrrr_sets = collect_local_parent_sets(mass_run_dir)
    ml_sets = {
        model_name: collect_ml_sets(metadata_path)
        for model_name, metadata_path in ml_metadata_by_model.items()
    }
    timestamps = sorted(set(mass_sets) & set(momentum_sets) & set(hrrr_sets))
    out: dict[dt.datetime, dict[str, RasterSource]] = {}
    for timestamp in timestamps:
        sources = {
            "hrrr": hrrr_sets[timestamp],
            "mass": mass_sets[timestamp],
            "momentum": momentum_sets[timestamp],
        }
        for model_name, model_sets in ml_sets.items():
            if timestamp in model_sets:
                sources[model_name] = model_sets[timestamp]
        out[timestamp] = sources
    return out


def sample_source(
    source: RasterSource,
    lon: float,
    lat: float,
    sampler: Callable[[Path, float, float], float | None],
) -> tuple[float | None, float | None]:
    speed = sampler(source.speed_path, lon, lat)
    direction = sampler(source.direction_path, lon, lat)
    return speed, direction


def add_source_fields(
    row: dict,
    prefix: str,
    speed: float | None,
    direction: float | None,
    obs_row: dict,
) -> None:
    if speed is None or direction is None:
        row.update({
            f"{prefix}_sample_status": "outside_crop_or_nodata",
            f"{prefix}_speed": "",
            f"{prefix}_dir_deg": "",
            f"{prefix}_u": "",
            f"{prefix}_v": "",
            f"{prefix}_speed_error": "",
            f"{prefix}_dir_abs_error_deg": "",
            f"{prefix}_vector_error": "",
        })
        return

    u_value, v_value = sv.obs_to_uv(speed, direction)
    vector_error = math.hypot(u_value - obs_row["u_obs"], v_value - obs_row["v_obs"])
    row.update({
        f"{prefix}_sample_status": "ok",
        f"{prefix}_speed": round(speed, 6),
        f"{prefix}_dir_deg": round(direction, 6),
        f"{prefix}_u": round(u_value, 6),
        f"{prefix}_v": round(v_value, 6),
        f"{prefix}_speed_error": round(speed - obs_row["speed_obs"], 6),
        f"{prefix}_dir_abs_error_deg": round(
            sv.circular_abs_error_deg(direction, obs_row["dir_obs_deg"]),
            6,
        ),
        f"{prefix}_vector_error": round(vector_error, 6),
    })


def sample_fieldnames(model_names: list[str]) -> list[str]:
    base = [
        "station_id",
        "station_label",
        "group",
        "sample_time_utc",
        "obs_time_utc",
        "obs_age_minutes",
        "height_m",
        "speed_obs",
        "dir_obs_deg",
        "u_obs",
        "v_obs",
    ]
    fields = list(base)
    for source_name in ["hrrr", "mass", "momentum", *model_names]:
        prefix = safe_source_name(source_name)
        fields.extend([
            f"{prefix}_sample_status",
            f"{prefix}_speed",
            f"{prefix}_dir_deg",
            f"{prefix}_u",
            f"{prefix}_v",
            f"{prefix}_speed_error",
            f"{prefix}_dir_abs_error_deg",
            f"{prefix}_vector_error",
        ])
    return fields


def build_station_sample_rows(
    station_records: list[dict],
    source_sets: dict[dt.datetime, dict[str, RasterSource]],
    observations_by_station: dict[str, list[dict]],
    *,
    model_names: list[str],
    tolerance_minutes: int,
    sampler: Callable[[Path, float, float], float | None] = rv.sample_raster_value,
) -> list[dict]:
    sample_rows = []
    source_names = ["hrrr", "mass", "momentum", *model_names]
    for station_meta in sorted(station_records, key=lambda item: item["station_id"]):
        station_id = station_meta["station_id"]
        station_obs_rows = observations_by_station.get(station_id) or []
        if not station_obs_rows:
            continue
        lon = float(station_meta["longitude"])
        lat = float(station_meta["latitude"])
        for stamp, sources in sorted(source_sets.items()):
            obs_row = sv.nearest_observation(station_obs_rows, stamp, tolerance_minutes)
            if not obs_row:
                continue
            row = {
                "station_id": station_id,
                "station_label": station_meta["label"],
                "group": station_meta["group"],
                "sample_time_utc": iso(stamp),
                "obs_time_utc": iso(obs_row["datetime"]),
                "obs_age_minutes": round(
                    abs((stamp - obs_row["datetime"]).total_seconds()) / 60.0,
                    3,
                ),
                "height_m": station_meta["height_m"],
                "speed_obs": round(obs_row["speed_obs"], 6),
                "dir_obs_deg": round(obs_row["dir_obs_deg"], 6),
                "u_obs": round(obs_row["u_obs"], 6),
                "v_obs": round(obs_row["v_obs"], 6),
            }
            required_ok = True
            for source_name in source_names:
                prefix = safe_source_name(source_name)
                source = sources.get(source_name)
                if source is None:
                    add_source_fields(row, prefix, None, None, obs_row)
                    if source_name in {"hrrr", "mass", "momentum"}:
                        required_ok = False
                    continue
                speed, direction = sample_source(source, lon, lat, sampler)
                add_source_fields(row, prefix, speed, direction, obs_row)
                if source_name in {"hrrr", "mass", "momentum"} and (speed is None or direction is None):
                    required_ok = False
            if required_ok:
                sample_rows.append(row)
    return sample_rows


def _float_value(row: dict, key: str) -> float | None:
    value = row.get(key)
    if value in (None, ""):
        return None
    return float(value)


def mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def rmse(values: list[float]) -> float | None:
    return math.sqrt(sum(value * value for value in values) / len(values)) if values else None


def metric_summary(rows: list[dict], prefix: str) -> dict[str, float | int | None]:
    speed_errors = []
    direction_errors = []
    vector_errors = []
    for row in rows:
        speed_error = _float_value(row, f"{prefix}_speed_error")
        direction_error = _float_value(row, f"{prefix}_dir_abs_error_deg")
        vector_error = _float_value(row, f"{prefix}_vector_error")
        if speed_error is None or direction_error is None or vector_error is None:
            continue
        speed_errors.append(speed_error)
        direction_errors.append(direction_error)
        vector_errors.append(vector_error)
    return {
        "sample_count": len(vector_errors),
        "speed_bias": mean(speed_errors),
        "speed_mae": mean([abs(value) for value in speed_errors]),
        "speed_rmse": rmse(speed_errors),
        "dir_mae_deg": mean(direction_errors),
        "vector_mae": mean(vector_errors),
        "vector_rmse": rmse(vector_errors),
    }


def model_summary_rows(sample_rows: list[dict], model_names: list[str]) -> list[dict]:
    rows = []
    for source_name in ["hrrr", "mass", "momentum", *model_names]:
        prefix = safe_source_name(source_name)
        summary = metric_summary(sample_rows, prefix)
        rows.append({
            "source": source_name,
            "source_prefix": prefix,
            "comparison": "observed_station_wind",
            **summary,
        })
    return rows


def station_summary_rows(sample_rows: list[dict], model_names: list[str]) -> list[dict]:
    rows = []
    station_ids = sorted({row["station_id"] for row in sample_rows})
    for station_id in station_ids:
        station_rows = [row for row in sample_rows if row["station_id"] == station_id]
        first = station_rows[0]
        for source_name in ["hrrr", "mass", "momentum", *model_names]:
            prefix = safe_source_name(source_name)
            rows.append({
                "station_id": station_id,
                "station_label": first["station_label"],
                "group": first["group"],
                "height_m": first["height_m"],
                "source": source_name,
                "source_prefix": prefix,
                **metric_summary(station_rows, prefix),
            })
    return rows


def emulator_metrics(rows: list[dict], source_prefix: str) -> dict[str, float | int | None]:
    speed_errors = []
    direction_errors = []
    vector_errors = []
    for row in rows:
        source_speed = _float_value(row, f"{source_prefix}_speed")
        source_dir = _float_value(row, f"{source_prefix}_dir_deg")
        source_u = _float_value(row, f"{source_prefix}_u")
        source_v = _float_value(row, f"{source_prefix}_v")
        momentum_speed = _float_value(row, "momentum_speed")
        momentum_dir = _float_value(row, "momentum_dir_deg")
        momentum_u = _float_value(row, "momentum_u")
        momentum_v = _float_value(row, "momentum_v")
        if None in {
            source_speed,
            source_dir,
            source_u,
            source_v,
            momentum_speed,
            momentum_dir,
            momentum_u,
            momentum_v,
        }:
            continue
        speed_errors.append(source_speed - momentum_speed)
        direction_errors.append(sv.circular_abs_error_deg(source_dir, momentum_dir))
        vector_errors.append(math.hypot(source_u - momentum_u, source_v - momentum_v))
    return {
        "sample_count": len(vector_errors),
        "speed_bias_vs_momentum": mean(speed_errors),
        "speed_mae_vs_momentum": mean([abs(value) for value in speed_errors]),
        "speed_rmse_vs_momentum": rmse(speed_errors),
        "dir_mae_deg_vs_momentum": mean(direction_errors),
        "vector_mae_vs_momentum": mean(vector_errors),
        "vector_rmse_vs_momentum": rmse(vector_errors),
    }


def emulator_summary_rows(sample_rows: list[dict], model_names: list[str]) -> list[dict]:
    rows = []
    groupings: list[tuple[str, list[dict], dict]] = [("ALL", sample_rows, {})]
    for station_id in sorted({row["station_id"] for row in sample_rows}):
        station_rows = [row for row in sample_rows if row["station_id"] == station_id]
        first = station_rows[0]
        groupings.append((
            station_id,
            station_rows,
            {
                "station_id": station_id,
                "station_label": first["station_label"],
                "group": first["group"],
                "height_m": first["height_m"],
            },
        ))

    for label, rows_for_group, extra in groupings:
        mass_summary = emulator_metrics(rows_for_group, "mass")
        mass_rmse = mass_summary["vector_rmse_vs_momentum"]
        for source_name in ["mass", *model_names]:
            prefix = safe_source_name(source_name)
            summary = emulator_metrics(rows_for_group, prefix)
            source_rmse = summary["vector_rmse_vs_momentum"]
            improvement = None
            if isinstance(mass_rmse, float) and isinstance(source_rmse, float) and mass_rmse:
                improvement = 100.0 * (mass_rmse - source_rmse) / mass_rmse
            rows.append({
                "scope": label,
                "source": source_name,
                "source_prefix": prefix,
                "comparison": "momentum_solver",
                "mass_vector_rmse_vs_momentum": mass_rmse,
                "vector_rmse_improvement_vs_mass_percent": improvement,
                **extra,
                **summary,
            })
    return rows


def _fmt(value: object, digits: int = 3) -> str:
    if value is None or value == "":
        return "n/a"
    if isinstance(value, (float, int)):
        return f"{value:.{digits}f}"
    return str(value)


def write_report(
    path: Path,
    *,
    sample_rows: list[dict],
    model_rows: list[dict],
    emulator_rows: list[dict],
    coverages: list[Coverage],
    model_names: list[str],
    speed_units: str,
) -> None:
    complete_count = sum(1 for item in coverages if item.status == "complete")
    lines = [
        "# Breck/Tenmile ML Synoptic Validation",
        "",
        "This validation uses existing GCS HRRR mass/momentum pairs only; it does not launch new WindNinja solver work.",
        "",
        "## Coverage",
        "",
        f"- Inventoried paired days: {len(coverages)}",
        f"- Complete paired days: {complete_count}",
        f"- Matched station-hour rows: {len(sample_rows)}",
        "- Stations: CABP6, CABP8, CAHSB at explicit 10.0 m height override.",
        "- Caveat: CABP8 has a known reported-vs-DEM elevation mismatch and should be read station-first, not pooled-only.",
        "",
        "## How Close Is ML To Momentum?",
        "",
    ]
    overall_emulator = [row for row in emulator_rows if row.get("scope") == "ALL"]
    for source_name in ["mass", *model_names]:
        row = next((item for item in overall_emulator if item["source"] == source_name), None)
        if not row:
            continue
        lines.append(
            f"- {source_name}: vector RMSE vs momentum {_fmt(row['vector_rmse_vs_momentum'])} "
            f"{speed_units}; improvement vs mass {_fmt(row['vector_rmse_improvement_vs_mass_percent'], 1)}%."
        )
    lines.extend([
        "",
        "## How Do Sources Compare To Observed Station Winds?",
        "",
    ])
    for row in model_rows:
        lines.append(
            f"- {row['source']}: sample_count={row['sample_count']}, "
            f"speed MAE={_fmt(row['speed_mae'])} {speed_units}, "
            f"direction MAE={_fmt(row['dir_mae_deg'])} deg, "
            f"vector RMSE={_fmt(row['vector_rmse'])} {speed_units}."
        )
    lines.extend([
        "",
        "## Output Files",
        "",
        "- `gcs_pair_inventory.csv`",
        "- `station_metadata.json`",
        "- `coverage_report.csv`",
        "- `samples.csv`",
        "- `model_summary.csv`",
        "- `station_summary.csv`",
        "- `emulator_summary.csv`",
    ])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def prepare_station_metadata(
    *,
    station_manifest: Path,
    output_dir: Path,
    start: dt.datetime,
    end: dt.datetime,
    token: str | None,
    force: bool,
) -> Path:
    metadata_path = output_dir / "station_metadata.json"
    if metadata_path.exists() and not force:
        return metadata_path
    args = SimpleNamespace(
        station_file=str(station_manifest),
        points_output=str(output_dir / "station_points.csv"),
        metadata_output=str(metadata_path),
        bbox_output=str(output_dir / "station_bbox.json"),
        padding_km=2.0,
        default_height=None,
        token=token,
        start=ymdhm(start),
        end=ymdhm(end),
    )
    sv.prepare_points(args)
    return metadata_path


def load_station_records(metadata_path: Path) -> list[dict]:
    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    return payload["stations"]


def resolve_model_checkpoints(
    model_names: list[str],
    overrides: dict[str, Path],
    *,
    bucket: str,
    checkpoint_gcs_prefix: str,
    download: bool,
    work_dir: Path,
) -> dict[str, Path]:
    checkpoints = {}
    for model_name in model_names:
        override = overrides.get(model_name)
        default_checkpoint = DEFAULT_MODEL_CHECKPOINTS.get(model_name)
        if override is not None:
            candidates = [override]
        elif default_checkpoint is not None:
            candidates = [
                default_checkpoint,
                default_checkpoint.parent / "checkpoints" / default_checkpoint.name,
            ]
        else:
            raise ValueError(f"No checkpoint path configured for model {model_name}")
        for checkpoint in candidates:
            if checkpoint.exists():
                checkpoints[model_name] = checkpoint
                break
        if model_name in checkpoints:
            continue
        if not download:
            raise FileNotFoundError(
                f"Missing checkpoint for {model_name}. Checked: "
                f"{', '.join(path.as_posix() for path in candidates)}. "
                "Restore it locally or pass --download-checkpoints."
            )
        target = work_dir / "checkpoints" / model_name / "best.pt"
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            checkpoints[model_name] = target
            continue
        prefix = checkpoint_gcs_prefix.rstrip("/")
        source_candidates = [
            f"{prefix}/{model_name}/checkpoints/best.pt",
            f"{prefix}/{model_name}/best.pt",
        ]
        last_error = None
        for src in source_candidates:
            try:
                if src.startswith("gs://"):
                    gcloud_cp_recursive(src, target.parent)
                else:
                    gcloud_cp_recursive(f"gs://{bucket}/{src.lstrip('/')}", target.parent)
                last_error = None
                break
            except subprocess.CalledProcessError as exc:
                last_error = exc
        if last_error is not None:
            raise last_error
        if not target.exists():
            raise FileNotFoundError(f"Downloaded checkpoint not found at expected path: {target}")
        checkpoints[model_name] = target
    return checkpoints


def parse_model_overrides(values: list[str] | None) -> dict[str, Path]:
    overrides = {}
    for value in values or []:
        if "=" not in value:
            raise ValueError(f"Expected --checkpoint MODEL=PATH, got {value!r}")
        model_name, path = value.split("=", 1)
        overrides[model_name.strip()] = Path(path.strip())
    return overrides


def token_from_runtime_env(source_root: Path) -> str | None:
    for env_key in ("MWN_SYNOPTIC_TOKEN", "CUSTOM_API_KEY"):
        value = os.environ.get(env_key)
        if value:
            return value
    runtime_env = source_root / "config" / "runtime.env"
    if not runtime_env.exists():
        return None
    for raw_line in runtime_env.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() in {"MWN_SYNOPTIC_TOKEN", "CUSTOM_API_KEY"}:
            cleaned = value.strip().strip('"').strip("'")
            if cleaned:
                return cleaned
    return None


def resolve_synoptic_token(explicit_token: str | None, source_root: Path) -> str | None:
    return explicit_token or token_from_runtime_env(source_root)


def output_label(args) -> str:
    if args.label:
        return args.label
    return dt.datetime.now(UTC).strftime("run_%Y%m%dT%H%M%SZ")


def run_inference_for_models(
    pair: GcsPair,
    *,
    model_checkpoints: dict[str, Path],
    mass_run_dir: Path,
    momentum_run_dir: Path,
    output_dir: Path,
    source_root: Path,
    speed_units: str,
    crop_size: int,
    device: str,
    reuse_existing: bool = False,
) -> dict[str, Path]:
    metadata_paths = {}
    for model_name, checkpoint in model_checkpoints.items():
        model_out = output_dir / "work" / "inference" / pair.mass.run_name / model_name
        metadata_path = model_out / "metadata.json"
        if reuse_existing and metadata_path.exists():
            metadata_paths[model_name] = metadata_path
            continue
        summary = ml_infer.infer(
            checkpoint,
            mass_run_dir,
            model_out,
            source_root=source_root,
            speed_units=speed_units,
            output_speed_units=speed_units,
            crop_size=crop_size,
            terrain_domain=DEFAULT_MOMENTUM_DOMAIN,
            momentum_run=momentum_run_dir,
            device_name=device,
        )
        metadata_paths[model_name] = Path(summary["metadata_path"])
    return metadata_paths


def download_pair(pair: GcsPair, work_temp: Path, *, reuse_existing: bool = False) -> tuple[Path, Path]:
    mass_dir = work_temp / pair.mass.run_name
    momentum_dir = work_temp / pair.momentum.run_name
    if reuse_existing and mass_dir.exists() and momentum_dir.exists():
        return mass_dir, momentum_dir
    gcloud_cp_recursive(pair.mass.uri, work_temp)
    gcloud_cp_recursive(pair.momentum.uri, work_temp)
    mass_dir = work_temp / pair.mass.run_name
    momentum_dir = work_temp / pair.momentum.run_name
    if not mass_dir.exists() or not momentum_dir.exists():
        raise FileNotFoundError(f"Downloaded pair missing local run dirs for {pair.mass.run_name}")
    return mass_dir, momentum_dir


def run_validation(args) -> int:
    label = output_label(args)
    output_dir = Path(args.output_root) / label
    output_dir.mkdir(parents=True, exist_ok=True)
    work_dir = output_dir / "work"
    work_temp = work_dir / "runtime" / "temp"
    model_names = args.models.split(",") if args.models else list(DEFAULT_MODEL_NAMES)
    model_names = [name.strip() for name in model_names if name.strip()]
    checkpoint_overrides = parse_model_overrides(args.checkpoint)
    source_root = Path(args.source_root)
    synoptic_token = resolve_synoptic_token(args.token, source_root)

    start = parse_utc(args.start) if args.start else None
    end = parse_utc(args.end) if args.end else None
    run_prefix = f"gs://{args.bucket}/runtime_temp"
    mass_uris = list_gcs_runs(run_prefix, args.mass_domain, args.model, start, end)
    momentum_uris = list_gcs_runs(run_prefix, args.momentum_domain, args.model, start, end)
    pairs = pair_gcs_runs(
        mass_uris,
        momentum_uris,
        mass_domain=args.mass_domain,
        momentum_domain=args.momentum_domain,
        model=args.model,
        start=start,
        end=end,
    )
    write_csv(output_dir / "gcs_pair_inventory.csv", inventory_rows(pairs))

    coverages = []
    for pair in pairs:
        mass_paths = gcloud_ls(f"{pair.mass.uri}/*")
        momentum_paths = gcloud_ls(f"{pair.momentum.uri}/*")
        coverages.append(coverage_for_pair(pair, mass_paths, momentum_paths))
    write_csv(output_dir / "coverage_report.csv", coverage_rows(coverages))

    complete_pairs = [coverage.pair for coverage in coverages if coverage.status == "complete"]
    if args.max_pairs is not None:
        complete_pairs = complete_pairs[: args.max_pairs]
    if args.inventory_only:
        print(json.dumps({
            "output_dir": output_dir.as_posix(),
            "paired_days": len(pairs),
            "complete_days": sum(1 for item in coverages if item.status == "complete"),
        }, indent=2))
        return 0
    if not complete_pairs:
        raise ValueError("No complete Breck mass/momentum GCS pairs were found for the requested filters.")

    metadata_start = min(pair.mass.start for pair in complete_pairs)
    metadata_end = max(pair.mass.end for pair in complete_pairs)
    station_metadata = prepare_station_metadata(
        station_manifest=Path(args.station_manifest),
        output_dir=output_dir,
        start=metadata_start,
        end=metadata_end,
        token=synoptic_token,
        force=args.force_metadata,
    )
    station_records = load_station_records(station_metadata)
    model_checkpoints = resolve_model_checkpoints(
        model_names,
        checkpoint_overrides,
        bucket=args.bucket,
        checkpoint_gcs_prefix=args.checkpoint_gcs_prefix,
        download=args.download_checkpoints,
        work_dir=work_dir,
    )

    sample_csv = output_dir / "samples.csv"
    if sample_csv.exists():
        sample_csv.unlink()
    all_sample_rows: list[dict] = []
    fields = sample_fieldnames(model_names)
    write_csv(sample_csv, [], fields)
    for pair in complete_pairs:
        if not args.reuse_work:
            for run_name in (pair.mass.run_name, pair.momentum.run_name):
                run_work = work_temp / run_name
                if run_work.exists():
                    shutil.rmtree(run_work)
        mass_run_dir, momentum_run_dir = download_pair(
            pair,
            work_temp,
            reuse_existing=args.reuse_work,
        )
        ml_metadata = run_inference_for_models(
            pair,
            model_checkpoints=model_checkpoints,
            mass_run_dir=mass_run_dir,
            momentum_run_dir=momentum_run_dir,
            output_dir=output_dir,
            source_root=source_root,
            speed_units=args.speed_units,
            crop_size=args.crop_size,
            device=args.device,
            reuse_existing=args.reuse_work,
        )
        observations = sv.fetch_observations(
            station_records,
            pair.mass.start,
            pair.mass.end,
            args.tolerance_minutes,
            synoptic_token,
            args.speed_units,
        )
        source_sets = build_source_sets(mass_run_dir, momentum_run_dir, ml_metadata)
        if args.max_timestamps is not None:
            source_sets = dict(sorted(source_sets.items())[: args.max_timestamps])
        rows = build_station_sample_rows(
            station_records,
            source_sets,
            observations,
            model_names=model_names,
            tolerance_minutes=args.tolerance_minutes,
        )
        append_csv(sample_csv, rows, fields)
        all_sample_rows.extend(rows)
        if not args.keep_work:
            shutil.rmtree(work_temp / pair.mass.run_name, ignore_errors=True)
            shutil.rmtree(work_temp / pair.momentum.run_name, ignore_errors=True)
            shutil.rmtree(output_dir / "work" / "inference" / pair.mass.run_name, ignore_errors=True)

    model_rows = model_summary_rows(all_sample_rows, model_names)
    station_rows = station_summary_rows(all_sample_rows, model_names)
    emulator_rows = emulator_summary_rows(all_sample_rows, model_names)
    write_csv(output_dir / "model_summary.csv", model_rows)
    write_csv(output_dir / "station_summary.csv", station_rows)
    write_csv(output_dir / "emulator_summary.csv", emulator_rows)
    write_report(
        output_dir / "report.md",
        sample_rows=all_sample_rows,
        model_rows=model_rows,
        emulator_rows=emulator_rows,
        coverages=coverages,
        model_names=model_names,
        speed_units=args.speed_units,
    )
    if not args.keep_work:
        shutil.rmtree(work_dir, ignore_errors=True)
    if args.sync_gcs:
        destination = f"gs://{args.bucket}/{args.gcs_output_prefix.strip('/')}/{label}"
        gcloud_rsync(output_dir, destination)
    print(json.dumps({
        "output_dir": output_dir.as_posix(),
        "samples": len(all_sample_rows),
        "model_summary": (output_dir / "model_summary.csv").as_posix(),
        "report": (output_dir / "report.md").as_posix(),
    }, indent=2))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate Breck/Tenmile ML momentum candidates against Synoptic station observations."
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--mass-domain", default=DEFAULT_MASS_DOMAIN)
    parser.add_argument("--momentum-domain", default=DEFAULT_MOMENTUM_DOMAIN)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--station-manifest", default=DEFAULT_STATION_MANIFEST.as_posix())
    parser.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT.as_posix())
    parser.add_argument("--label", help="Output subdirectory label. Defaults to a UTC run stamp.")
    parser.add_argument("--source-root", default=".")
    parser.add_argument("--start", help="Optional UTC start filter, e.g. 202501010000.")
    parser.add_argument("--end", help="Optional UTC end filter, e.g. 202604010000.")
    parser.add_argument("--max-pairs", type=int, help="Limit complete pairs processed after coverage filtering.")
    parser.add_argument("--max-timestamps", type=int,
                        help="Limit aligned timestamps sampled within each pair; useful for smoke tests.")
    parser.add_argument("--inventory-only", action="store_true",
                        help="Write GCS inventory and coverage reports without downloading or sampling.")
    parser.add_argument("--models", default=",".join(DEFAULT_MODEL_NAMES),
                        help="Comma-separated model names to compare.")
    parser.add_argument("--checkpoint", action="append",
                        help="Override checkpoint path as MODEL=PATH. Repeat per model.")
    parser.add_argument("--download-checkpoints", action="store_true",
                        help="Download missing checkpoints from --checkpoint-gcs-prefix.")
    parser.add_argument(
        "--checkpoint-gcs-prefix",
        default=f"gs://{DEFAULT_BUCKET}/colab_results",
        help="GCS prefix containing <model>/best.pt for --download-checkpoints.",
    )
    parser.add_argument("--force-metadata", action="store_true")
    parser.add_argument("--token", help="Synoptic API token. Defaults to MWN_SYNOPTIC_TOKEN.")
    parser.add_argument("--tolerance-minutes", type=int, default=DEFAULT_TOLERANCE_MINUTES)
    parser.add_argument("--speed-units", default=DEFAULT_SPEED_UNITS, choices=["mph", "mps", "kph", "kts"])
    parser.add_argument("--crop-size", type=int, default=DEFAULT_CROP_SIZE)
    parser.add_argument("--device", default="auto", choices=["auto", "cpu", "cuda"])
    parser.add_argument("--keep-work", action="store_true",
                        help="Keep downloaded raw runs and intermediate ML rasters under the output work dir.")
    parser.add_argument("--reuse-work", action="store_true",
                        help="Reuse existing downloaded runs/checkpoints/inference metadata in the output work dir.")
    parser.add_argument("--sync-gcs", action="store_true",
                        help="Sync compact validation outputs to GCS after a successful run.")
    parser.add_argument("--gcs-output-prefix", default=DEFAULT_GCS_OUTPUT_PREFIX)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return run_validation(args)


if __name__ == "__main__":
    raise SystemExit(main())
