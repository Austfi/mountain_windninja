#!/usr/bin/env python3
"""Create static validation plots from WindNinja/HRRR/Synoptic samples."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import json
import math
import os
import random
import re
from pathlib import Path

UTC = dt.timezone.utc
BASE_DIR = Path(__file__).resolve().parent.parent

NUMERIC_FIELDS = {
    "obs_age_minutes",
    "height_m",
    "speed_obs",
    "dir_obs_deg",
    "u_obs",
    "v_obs",
    "wn_speed",
    "wn_dir_deg",
    "wn_u",
    "wn_v",
    "wn_speed_error",
    "wn_dir_abs_error_deg",
    "wn_vector_error",
    "wx_speed",
    "wx_dir_deg",
    "wx_u",
    "wx_v",
    "wx_speed_error",
    "wx_dir_abs_error_deg",
    "wx_vector_error",
}

COLORS = {
    "obs": "#111111",
    "windninja": "#1f77b4",
    "hrrr": "#c0392b",
    "grid": "#d8dee9",
    "axis": "#2f3542",
    "text": "#1f2933",
    "muted": "#52616f",
    "bg": "#ffffff",
}

CONTAINER_REPO_PREFIX = "/opt/mountain_windninja/"


def resolve_repo_path(raw_path: str | Path) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return (BASE_DIR / path).resolve()


def resolve_artifact_path(raw_path: str | Path | None) -> Path | None:
    if not raw_path:
        return None
    value = str(raw_path)
    if value.startswith(CONTAINER_REPO_PREFIX):
        return (BASE_DIR / value[len(CONTAINER_REPO_PREFIX):]).resolve()
    return resolve_repo_path(value)


def parse_time(raw_value: str) -> dt.datetime:
    value = raw_value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = dt.datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_float(raw_value: str) -> float | None:
    if raw_value == "":
        return None
    try:
        return float(raw_value)
    except ValueError:
        return None


def collect_sample_paths(study_root: Path) -> list[Path]:
    chunk_paths = sorted(study_root.glob("chunks/*/samples.csv"))
    if chunk_paths:
        return chunk_paths
    aggregate = study_root / "samples.csv"
    if aggregate.exists():
        return [aggregate]
    raise ValueError(f"No samples.csv files found under {study_root}")


def load_samples(paths: list[Path], station_id: str | None = None) -> list[dict]:
    rows_by_key: dict[tuple[str, str], dict] = {}
    station_filter = station_id.upper() if station_id else None

    for path in paths:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for raw_row in reader:
                row_station = (raw_row.get("station_id") or "").upper()
                if station_filter and row_station != station_filter:
                    continue
                if not row_station or not raw_row.get("sample_time_utc"):
                    continue

                row = dict(raw_row)
                row["station_id"] = row_station
                row["_time"] = parse_time(row["sample_time_utc"])
                row["_source_path"] = str(path)
                for field in NUMERIC_FIELDS:
                    if field in row:
                        row[field] = parse_float(row[field])
                key = (row_station, row["sample_time_utc"])
                rows_by_key[key] = row

    rows = sorted(rows_by_key.values(), key=lambda item: (item["_time"], item["station_id"]))
    if not rows:
        raise ValueError("No validation sample rows matched the requested inputs.")
    return rows


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else float("nan")


def rmse(values: list[float]) -> float:
    return math.sqrt(mean([value * value for value in values])) if values else float("nan")


def clean_number(value: float | None, digits: int = 2) -> float | None:
    if value is None or math.isnan(value):
        return None
    return round(value, digits)


def metric_summary(rows: list[dict], model_label: str = "HRRR") -> dict:
    def values(field: str, *, absolute: bool = False) -> list[float]:
        out = []
        for row in rows:
            value = row.get(field)
            if value is None:
                continue
            out.append(abs(value) if absolute else value)
        return out

    start_time = rows[0]["_time"]
    end_time = rows[-1]["_time"]
    stations = sorted({row["station_id"] for row in rows})
    parent_model = {
        "speed_bias": clean_number(mean(values("wx_speed_error"))),
        "speed_mae": clean_number(mean(values("wx_speed_error", absolute=True))),
        "speed_rmse": clean_number(rmse(values("wx_speed_error"))),
        "dir_mae_deg": clean_number(mean(values("wx_dir_abs_error_deg"))),
        "vector_rmse": clean_number(rmse(values("wx_vector_error"))),
    }
    return {
        "sample_count": len(rows),
        "station_count": len(stations),
        "stations": stations,
        "model_label": model_label,
        "start_utc": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_utc": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "windninja": {
            "speed_bias": clean_number(mean(values("wn_speed_error"))),
            "speed_mae": clean_number(mean(values("wn_speed_error", absolute=True))),
            "speed_rmse": clean_number(rmse(values("wn_speed_error"))),
            "dir_mae_deg": clean_number(mean(values("wn_dir_abs_error_deg"))),
            "vector_rmse": clean_number(rmse(values("wn_vector_error"))),
        },
        "hrrr": parent_model,
        "parent_model": parent_model,
    }


def mean_or_none(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def rmse_or_none(values: list[float]) -> float | None:
    return math.sqrt(sum(value * value for value in values) / len(values)) if values else None


def clean_delta(baseline_value: float | None, windninja_value: float | None) -> float | None:
    if not finite(baseline_value) or not finite(windninja_value):
        return None
    return clean_number(baseline_value - windninja_value)


def raw_model_metrics(rows: list[dict], prefix: str) -> dict[str, float | None]:
    speed_errors = field_values(rows, f"{prefix}_speed_error")
    direction_errors = field_values(rows, f"{prefix}_dir_abs_error_deg")
    vector_errors = field_values(rows, f"{prefix}_vector_error")
    return {
        "speed_bias": mean_or_none(speed_errors),
        "speed_mae": mean_or_none([abs(value) for value in speed_errors]),
        "speed_rmse": rmse_or_none(speed_errors),
        "dir_mae_deg": mean_or_none(direction_errors),
        "vector_rmse": rmse_or_none(vector_errors),
    }


def clean_metrics(metrics: dict[str, float | None]) -> dict[str, float | None]:
    return {key: clean_number(value) for key, value in metrics.items()}


def diagnostic_metric_record(label: str, rows: list[dict]) -> dict:
    wn = raw_model_metrics(rows, "wn")
    wx = raw_model_metrics(rows, "wx")
    return {
        "label": label,
        "sample_count": len(rows),
        "windninja": clean_metrics(wn),
        "hrrr": clean_metrics(wx),
        "improvement": {
            "speed_mae": clean_delta(wx["speed_mae"], wn["speed_mae"]),
            "speed_rmse": clean_delta(wx["speed_rmse"], wn["speed_rmse"]),
            "vector_rmse": clean_delta(wx["vector_rmse"], wn["vector_rmse"]),
            "dir_mae_deg": clean_delta(wx["dir_mae_deg"], wn["dir_mae_deg"]),
        },
    }


def grouped_diagnostic_records(
    rows: list[dict],
    labels: list[str],
    group_fn,
) -> list[dict]:
    grouped = {label: [] for label in labels}
    for row in rows:
        label = group_fn(row)
        if label in grouped:
            grouped[label].append(row)
    return [diagnostic_metric_record(label, grouped[label]) for label in labels]


def month_diagnostic_records(rows: list[dict]) -> list[dict]:
    labels = sorted({row["_time"].strftime("%Y-%m") for row in rows})
    return grouped_diagnostic_records(
        rows,
        labels,
        lambda row: row["_time"].strftime("%Y-%m"),
    )


def utc_hour_diagnostic_records(rows: list[dict]) -> list[dict]:
    labels = [f"{hour:02d}Z" for hour in range(24)]
    return grouped_diagnostic_records(
        rows,
        labels,
        lambda row: f"{row['_time'].hour:02d}Z",
    )


SPEED_BINS = [
    ("0-5 mph", 0.0, 5.0),
    ("5-10 mph", 5.0, 10.0),
    ("10-20 mph", 10.0, 20.0),
    ("20-30 mph", 20.0, 30.0),
    ("30+ mph", 30.0, None),
]


def observed_speed_bin(row: dict) -> str | None:
    speed = row.get("speed_obs")
    if not finite(speed):
        return None
    for label, low, high in SPEED_BINS:
        if speed >= low and (high is None or speed < high):
            return label
    return None


SECTOR_LABELS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]


def observed_direction_sector(row: dict) -> str | None:
    direction = row.get("dir_obs_deg")
    if not finite(direction):
        return None
    index = int((((direction % 360.0) + 22.5) % 360.0) // 45.0)
    return SECTOR_LABELS[index]


def speed_bin_diagnostic_records(rows: list[dict]) -> list[dict]:
    return grouped_diagnostic_records(
        rows,
        [label for label, _, _ in SPEED_BINS],
        observed_speed_bin,
    )


def direction_sector_diagnostic_records(rows: list[dict]) -> list[dict]:
    return grouped_diagnostic_records(rows, SECTOR_LABELS, observed_direction_sector)


NOTICEABLE_SAMPLE_COUNT = 20
NOTICEABLE_SPEED_DELTA = 1.0
NOTICEABLE_DIRECTION_DELTA = 10.0


def noticeable_patterns(records: list[dict], category: str, model_label: str) -> list[dict]:
    metric_thresholds = {
        "speed_mae": NOTICEABLE_SPEED_DELTA,
        "speed_rmse": NOTICEABLE_SPEED_DELTA,
        "vector_rmse": NOTICEABLE_SPEED_DELTA,
        "dir_mae_deg": NOTICEABLE_DIRECTION_DELTA,
    }
    patterns = []
    for record in records:
        if record["sample_count"] < NOTICEABLE_SAMPLE_COUNT:
            continue
        for metric, threshold in metric_thresholds.items():
            difference = record["improvement"].get(metric)
            if not finite(difference) or abs(difference) < threshold:
                continue
            patterns.append({
                "category": category,
                "label": record["label"],
                "sample_count": record["sample_count"],
                "metric": metric,
                "difference": clean_number(difference),
                "winner": "WindNinja" if difference > 0 else model_label,
                "threshold": threshold,
            })
    return patterns


def event_record(row: dict) -> dict:
    speed_improvement = None
    if finite(row.get("wn_speed_error")) and finite(row.get("wx_speed_error")):
        speed_improvement = abs(row["wx_speed_error"]) - abs(row["wn_speed_error"])
    direction_improvement = None
    if finite(row.get("wn_dir_abs_error_deg")) and finite(row.get("wx_dir_abs_error_deg")):
        direction_improvement = row["wx_dir_abs_error_deg"] - row["wn_dir_abs_error_deg"]
    vector_improvement = None
    if finite(row.get("wn_vector_error")) and finite(row.get("wx_vector_error")):
        vector_improvement = row["wx_vector_error"] - row["wn_vector_error"]
    vector_errors = [
        value
        for value in (row.get("wn_vector_error"), row.get("wx_vector_error"))
        if finite(value)
    ]
    return {
        "station_id": row["station_id"],
        "sample_time_utc": row["sample_time_utc"],
        "obs_time_utc": row.get("obs_time_utc"),
        "speed_obs": clean_number(row.get("speed_obs")),
        "dir_obs_deg": clean_number(row.get("dir_obs_deg")),
        "wn_speed": clean_number(row.get("wn_speed")),
        "wx_speed": clean_number(row.get("wx_speed")),
        "wn_speed_error": clean_number(row.get("wn_speed_error")),
        "wx_speed_error": clean_number(row.get("wx_speed_error")),
        "wn_dir_abs_error_deg": clean_number(row.get("wn_dir_abs_error_deg")),
        "wx_dir_abs_error_deg": clean_number(row.get("wx_dir_abs_error_deg")),
        "wn_vector_error": clean_number(row.get("wn_vector_error")),
        "wx_vector_error": clean_number(row.get("wx_vector_error")),
        "speed_mae_improvement": clean_number(speed_improvement),
        "dir_mae_improvement": clean_number(direction_improvement),
        "vector_error_improvement": clean_number(vector_improvement),
        "largest_vector_error": clean_number(max(vector_errors) if vector_errors else None),
    }


def top_event_records(rows: list[dict], limit: int = 10) -> dict[str, list[dict]]:
    event_rows = [event_record(row) for row in rows]
    top_errors = sorted(
        event_rows,
        key=lambda row: row["largest_vector_error"] if row["largest_vector_error"] is not None else -1.0,
        reverse=True,
    )[:limit]
    comparable = [
        row
        for row in event_rows
        if row["vector_error_improvement"] is not None
    ]
    top_wins = sorted(
        comparable,
        key=lambda row: row["vector_error_improvement"],
        reverse=True,
    )[:limit]
    top_losses = sorted(comparable, key=lambda row: row["vector_error_improvement"])[:limit]
    return {
        "top_error_events": top_errors,
        "top_windninja_wins": top_wins,
        "top_windninja_losses": top_losses,
    }


def build_diagnostics(rows: list[dict], model_label: str) -> dict:
    monthly = month_diagnostic_records(rows)
    utc_hour = utc_hour_diagnostic_records(rows)
    speed_bins = speed_bin_diagnostic_records(rows)
    direction_sectors = direction_sector_diagnostic_records(rows)
    noticeable = []
    for category, records in (
        ("monthly", monthly),
        ("utc_hour", utc_hour),
        ("observed_speed_bin", speed_bins),
        ("observed_direction_sector", direction_sectors),
    ):
        noticeable.extend(noticeable_patterns(records, category, model_label))

    return {
        "support_rule": {
            "sample_count_min": NOTICEABLE_SAMPLE_COUNT,
            "speed_or_vector_difference_min": NOTICEABLE_SPEED_DELTA,
            "direction_difference_min_deg": NOTICEABLE_DIRECTION_DELTA,
        },
        "overall": diagnostic_metric_record("overall", rows),
        "monthly": monthly,
        "utc_hour": utc_hour,
        "observed_speed_bins": speed_bins,
        "observed_direction_sectors": direction_sectors,
        "noticeable_patterns": noticeable,
        **top_event_records(rows),
    }


def finite(value: float | None) -> bool:
    return value is not None and not math.isnan(value)


def field_values(
    rows: list[dict],
    field: str,
    *,
    absolute: bool = False,
    obs_speed_min: float | None = None,
) -> list[float]:
    out = []
    for row in rows:
        if obs_speed_min is not None:
            obs_speed = row.get("speed_obs")
            if obs_speed is None or obs_speed < obs_speed_min:
                continue
        value = row.get(field)
        if not finite(value):
            continue
        out.append(abs(value) if absolute else value)
    return out


def paired_rows(rows: list[dict], fields: tuple[str, ...]) -> list[dict]:
    return [
        row
        for row in rows
        if all(finite(row.get(field)) for field in fields)
    ]


def skill_score(model_error: float | None, baseline_error: float | None) -> float | None:
    if not finite(model_error) or not finite(baseline_error) or math.isclose(baseline_error, 0.0):
        return None
    return 1.0 - (model_error / baseline_error)


def percentile(values: list[float], pct: float) -> float | None:
    clean = sorted(value for value in values if finite(value))
    if not clean:
        return None
    if len(clean) == 1:
        return clean[0]
    position = (len(clean) - 1) * (pct / 100.0)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return clean[int(position)]
    fraction = position - lower
    return clean[lower] * (1.0 - fraction) + clean[upper] * fraction


def bootstrap_ci(
    rows: list[dict],
    metric_fn,
    *,
    iterations: int = 500,
    seed: int = 20260504,
) -> tuple[float | None, float | None]:
    if len(rows) < 2:
        return None, None
    rng = random.Random(seed)
    values = []
    count = len(rows)
    for _ in range(iterations):
        sample = [rows[rng.randrange(count)] for _ in range(count)]
        value = metric_fn(sample)
        if finite(value):
            values.append(value)
    return percentile(values, 2.5), percentile(values, 97.5)


def regression_stats(rows: list[dict], model_field: str) -> dict[str, float | None]:
    points = [
        (row["speed_obs"], row[model_field])
        for row in rows
        if finite(row.get("speed_obs")) and finite(row.get(model_field))
    ]
    if len(points) < 2:
        return {"slope": None, "intercept": None, "r2": None}

    x_values = [point[0] for point in points]
    y_values = [point[1] for point in points]
    x_mean = mean(x_values)
    y_mean = mean(y_values)
    denominator = sum((value - x_mean) ** 2 for value in x_values)
    if math.isclose(denominator, 0.0):
        return {"slope": None, "intercept": None, "r2": None}
    slope = sum((x - x_mean) * (y - y_mean) for x, y in points) / denominator
    intercept = y_mean - slope * x_mean
    ss_tot = sum((value - y_mean) ** 2 for value in y_values)
    ss_res = sum((y - (slope * x + intercept)) ** 2 for x, y in points)
    r2 = None if math.isclose(ss_tot, 0.0) else 1.0 - (ss_res / ss_tot)
    return {"slope": slope, "intercept": intercept, "r2": r2}


def station_sample_distances(rows: list[dict], stations: list[dict]) -> dict[str, dict[str, float]]:
    rows_by_station: dict[str, dict] = {}
    for row in rows:
        station_id = row["station_id"]
        if station_id in rows_by_station:
            continue
        wn_path = resolve_artifact_path(row.get("wn_vel_path"))
        wx_path = resolve_artifact_path(row.get("wx_vel_path"))
        if wn_path and wx_path and wn_path.exists() and wx_path.exists():
            rows_by_station[station_id] = row
    if not rows_by_station or not stations:
        return {}

    try:
        import rasterio
        from rasterio.warp import transform
    except Exception:
        return {}

    wgs84_proj = "+proj=longlat +datum=WGS84 +no_defs"

    def project(crs, lon: float, lat: float) -> tuple[float, float]:
        xs, ys = transform(wgs84_proj, crs, [lon], [lat])
        return xs[0], ys[0]

    def sample_distance(path: Path, lon: float, lat: float) -> float | None:
        try:
            with rasterio.open(path) as dataset:
                x, y = project(dataset.crs, lon, lat)
                row_index, col_index = dataset.index(x, y)
                if (
                    row_index < 0
                    or col_index < 0
                    or row_index >= dataset.height
                    or col_index >= dataset.width
                ):
                    return None
                center_x, center_y = dataset.xy(row_index, col_index)
                return math.hypot(center_x - x, center_y - y)
        except Exception:
            return None

    station_by_id = {str(station["station_id"]).upper(): station for station in stations}
    distances = {}
    for station_id, row in rows_by_station.items():
        station = station_by_id.get(station_id.upper())
        if not station:
            continue
        station_lon = float(station["longitude"])
        station_lat = float(station["latitude"])
        wn_path = resolve_artifact_path(row.get("wn_vel_path"))
        wx_path = resolve_artifact_path(row.get("wx_vel_path"))
        if not wn_path or not wx_path:
            continue
        distances[station_id] = {
            "wn_sample_distance_m": sample_distance(wn_path, station_lon, station_lat),
            "wx_sample_distance_m": sample_distance(wx_path, station_lon, station_lat),
        }
    return distances


STATION_METRIC_FIELDS = [
    "station_id",
    "station_label",
    "sample_count",
    "obs_height_m",
    "height_source",
    "wn_sample_distance_m",
    "wx_sample_distance_m",
    "wn_speed_bias",
    "wx_speed_bias",
    "wn_speed_mae",
    "wx_speed_mae",
    "speed_mae_skill",
    "speed_mae_improvement",
    "speed_mae_improvement_ci_low",
    "speed_mae_improvement_ci_high",
    "wn_speed_rmse",
    "wx_speed_rmse",
    "wn_vector_rmse",
    "wx_vector_rmse",
    "vector_rmse_skill",
    "vector_rmse_improvement",
    "vector_rmse_improvement_ci_low",
    "vector_rmse_improvement_ci_high",
    "wn_dir_mae_deg",
    "wx_dir_mae_deg",
    "dir_count_ge_5mph",
    "wn_dir_mae_ge_5mph",
    "wx_dir_mae_ge_5mph",
    "dir_count_ge_10mph",
    "wn_dir_mae_ge_10mph",
    "wx_dir_mae_ge_10mph",
    "wn_speed_slope",
    "wx_speed_slope",
    "wn_speed_intercept",
    "wx_speed_intercept",
    "wn_speed_r2",
    "wx_speed_r2",
]


def build_station_metrics(rows: list[dict], stations: list[dict]) -> list[dict]:
    station_metadata = {str(station["station_id"]).upper(): station for station in stations}
    distances = station_sample_distances(rows, stations)
    metrics = []

    for station_id in sorted({row["station_id"] for row in rows}):
        station_rows = [row for row in rows if row["station_id"] == station_id]
        metadata = station_metadata.get(station_id.upper(), {})
        label = (
            metadata.get("label")
            or metadata.get("name")
            or station_rows[0].get("station_label")
            or station_id
        )
        obs_height = (
            metadata.get("height_m")
            if metadata.get("height_m") is not None
            else station_rows[0].get("height_m")
        )
        height_source = metadata.get("height_source") or "sample"

        wn_speed_mae = mean(field_values(station_rows, "wn_speed_error", absolute=True))
        wx_speed_mae = mean(field_values(station_rows, "wx_speed_error", absolute=True))
        wn_vector_rmse = rmse(field_values(station_rows, "wn_vector_error"))
        wx_vector_rmse = rmse(field_values(station_rows, "wx_vector_error"))

        def speed_improvement(sample: list[dict]) -> float:
            paired = paired_rows(sample, ("wn_speed_error", "wx_speed_error"))
            if not paired:
                return float("nan")
            return (
                mean(field_values(paired, "wx_speed_error", absolute=True))
                - mean(field_values(paired, "wn_speed_error", absolute=True))
            )

        def vector_improvement(sample: list[dict]) -> float:
            paired = paired_rows(sample, ("wn_vector_error", "wx_vector_error"))
            if not paired:
                return float("nan")
            return rmse(field_values(paired, "wx_vector_error")) - rmse(field_values(paired, "wn_vector_error"))

        speed_ci_low, speed_ci_high = bootstrap_ci(station_rows, speed_improvement)
        vector_ci_low, vector_ci_high = bootstrap_ci(station_rows, vector_improvement)
        dir_rows_ge_5 = [
            row
            for row in station_rows
            if finite(row.get("speed_obs"))
            and row["speed_obs"] >= 5.0
            and finite(row.get("wn_dir_abs_error_deg"))
            and finite(row.get("wx_dir_abs_error_deg"))
        ]
        dir_rows_ge_10 = [
            row
            for row in station_rows
            if finite(row.get("speed_obs"))
            and row["speed_obs"] >= 10.0
            and finite(row.get("wn_dir_abs_error_deg"))
            and finite(row.get("wx_dir_abs_error_deg"))
        ]
        wn_regression = regression_stats(station_rows, "wn_speed")
        wx_regression = regression_stats(station_rows, "wx_speed")
        station_distances = distances.get(station_id, {})

        metrics.append({
            "station_id": station_id,
            "station_label": label,
            "sample_count": len(station_rows),
            "obs_height_m": clean_number(float(obs_height), 2) if obs_height is not None else None,
            "height_source": height_source,
            "wn_sample_distance_m": clean_number(station_distances.get("wn_sample_distance_m"), 1),
            "wx_sample_distance_m": clean_number(station_distances.get("wx_sample_distance_m"), 1),
            "wn_speed_bias": clean_number(mean(field_values(station_rows, "wn_speed_error"))),
            "wx_speed_bias": clean_number(mean(field_values(station_rows, "wx_speed_error"))),
            "wn_speed_mae": clean_number(wn_speed_mae),
            "wx_speed_mae": clean_number(wx_speed_mae),
            "speed_mae_skill": clean_number(skill_score(wn_speed_mae, wx_speed_mae), 3),
            "speed_mae_improvement": clean_number(wx_speed_mae - wn_speed_mae),
            "speed_mae_improvement_ci_low": clean_number(speed_ci_low),
            "speed_mae_improvement_ci_high": clean_number(speed_ci_high),
            "wn_speed_rmse": clean_number(rmse(field_values(station_rows, "wn_speed_error"))),
            "wx_speed_rmse": clean_number(rmse(field_values(station_rows, "wx_speed_error"))),
            "wn_vector_rmse": clean_number(wn_vector_rmse),
            "wx_vector_rmse": clean_number(wx_vector_rmse),
            "vector_rmse_skill": clean_number(skill_score(wn_vector_rmse, wx_vector_rmse), 3),
            "vector_rmse_improvement": clean_number(wx_vector_rmse - wn_vector_rmse),
            "vector_rmse_improvement_ci_low": clean_number(vector_ci_low),
            "vector_rmse_improvement_ci_high": clean_number(vector_ci_high),
            "wn_dir_mae_deg": clean_number(mean(field_values(station_rows, "wn_dir_abs_error_deg"))),
            "wx_dir_mae_deg": clean_number(mean(field_values(station_rows, "wx_dir_abs_error_deg"))),
            "dir_count_ge_5mph": len(dir_rows_ge_5),
            "wn_dir_mae_ge_5mph": clean_number(mean(field_values(dir_rows_ge_5, "wn_dir_abs_error_deg"))),
            "wx_dir_mae_ge_5mph": clean_number(mean(field_values(dir_rows_ge_5, "wx_dir_abs_error_deg"))),
            "dir_count_ge_10mph": len(dir_rows_ge_10),
            "wn_dir_mae_ge_10mph": clean_number(mean(field_values(dir_rows_ge_10, "wn_dir_abs_error_deg"))),
            "wx_dir_mae_ge_10mph": clean_number(mean(field_values(dir_rows_ge_10, "wx_dir_abs_error_deg"))),
            "wn_speed_slope": clean_number(wn_regression["slope"], 3),
            "wx_speed_slope": clean_number(wx_regression["slope"], 3),
            "wn_speed_intercept": clean_number(wn_regression["intercept"]),
            "wx_speed_intercept": clean_number(wx_regression["intercept"]),
            "wn_speed_r2": clean_number(wn_regression["r2"], 3),
            "wx_speed_r2": clean_number(wx_regression["r2"], 3),
        })
    return metrics


def average_by_time(rows: list[dict], fields: list[str]) -> list[dict]:
    grouped: dict[dt.datetime, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row["_time"], []).append(row)

    records = []
    for stamp, stamp_rows in sorted(grouped.items()):
        record = {"_time": stamp}
        for field in fields:
            values = [row[field] for row in stamp_rows if row.get(field) is not None]
            record[field] = mean(values) if values else None
        records.append(record)
    return records


def daily_records(rows: list[dict]) -> list[dict]:
    grouped: dict[dt.date, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row["_time"].date(), []).append(row)

    records = []
    for day, day_rows in sorted(grouped.items()):
        records.append({
            "_time": dt.datetime.combine(day, dt.time(), tzinfo=UTC),
            "wn_speed_mae": mean([
                abs(row["wn_speed_error"])
                for row in day_rows
                if row.get("wn_speed_error") is not None
            ]),
            "wx_speed_mae": mean([
                abs(row["wx_speed_error"])
                for row in day_rows
                if row.get("wx_speed_error") is not None
            ]),
            "wn_vector_rmse": rmse([
                row["wn_vector_error"]
                for row in day_rows
                if row.get("wn_vector_error") is not None
            ]),
            "wx_vector_rmse": rmse([
                row["wx_vector_error"]
                for row in day_rows
                if row.get("wx_vector_error") is not None
            ]),
        })
    return records


def load_station_metadata(study_root: Path, station_id: str | None = None) -> list[dict]:
    metadata_path = study_root / "station_metadata.json"
    if not metadata_path.exists():
        return []

    payload = json.loads(metadata_path.read_text(encoding="utf-8"))
    stations = payload.get("stations", [])
    station_filter = station_id.upper() if station_id else None
    if station_filter:
        stations = [
            station
            for station in stations
            if (station.get("station_id") or "").upper() == station_filter
        ]
    return [
        station
        for station in stations
        if station.get("latitude") is not None and station.get("longitude") is not None
    ]


def fmt_num(value: float) -> str:
    if abs(value) >= 100:
        return f"{value:.0f}"
    if abs(value) >= 10:
        return f"{value:.1f}"
    return f"{value:.2f}".rstrip("0").rstrip(".")


def fmt_time_tick(timestamp: float) -> str:
    value = dt.datetime.fromtimestamp(timestamp, tz=UTC)
    if value.hour:
        return f"{value:%b} {value.day} {value:%H}Z"
    return f"{value:%b} {value.day}"


def nice_range(values: list[float], include_zero: bool = False) -> tuple[float, float]:
    clean = [value for value in values if value is not None and not math.isnan(value)]
    if not clean:
        return 0.0, 1.0
    original_low = min(clean)
    if include_zero:
        clean.append(0.0)
    low = min(clean)
    high = max(clean)
    if math.isclose(low, high):
        pad = max(abs(low) * 0.1, 1.0)
        return low - pad, high + pad
    pad = (high - low) * 0.08
    padded_low = low - pad
    if include_zero and original_low >= 0:
        padded_low = 0.0
    return padded_low, high + pad


def time_range(values: list[float]) -> tuple[float, float]:
    low = min(values)
    high = max(values)
    if math.isclose(low, high):
        pad = 3600.0
        return low - pad, high + pad
    return low, high


def tick_values(low: float, high: float, count: int = 5) -> list[float]:
    if count <= 1:
        return [low]
    step = (high - low) / (count - 1)
    return [low + step * index for index in range(count)]


def svg_header(width: int, height: int, title: str) -> list[str]:
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">',
        "<style>",
        "text{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;fill:#1f2933}",
        ".title{font-size:22px;font-weight:700}",
        ".axis{font-size:12px;fill:#52616f}",
        ".legend{font-size:13px}",
        ".grid{stroke:#d8dee9;stroke-width:1}",
        ".axis-line{stroke:#2f3542;stroke-width:1.2}",
        "</style>",
        f'<rect x="0" y="0" width="{width}" height="{height}" fill="{COLORS["bg"]}"/>',
        f'<text x="{width / 2:.1f}" y="34" text-anchor="middle" class="title">{html.escape(title)}</text>',
    ]


def svg_footer() -> str:
    return "</svg>\n"


def line_plot(
    path: Path,
    records: list[dict],
    series: list[tuple[str, str, str]],
    *,
    title: str,
    y_label: str,
    include_zero: bool = False,
) -> None:
    width = 980
    height = 560
    margin = {"left": 78, "right": 34, "top": 70, "bottom": 78}
    plot_width = width - margin["left"] - margin["right"]
    plot_height = height - margin["top"] - margin["bottom"]

    x_values = [record["_time"].timestamp() for record in records]
    y_values = [
        record[field]
        for record in records
        for _, field, _ in series
        if record.get(field) is not None
    ]
    x_low, x_high = time_range(x_values)
    y_low, y_high = nice_range(y_values, include_zero=include_zero)

    def x_scale(value: float) -> float:
        return margin["left"] + ((value - x_low) / (x_high - x_low)) * plot_width

    def y_scale(value: float) -> float:
        return margin["top"] + (1 - ((value - y_low) / (y_high - y_low))) * plot_height

    parts = svg_header(width, height, title)
    for tick in tick_values(y_low, y_high):
        y = y_scale(tick)
        parts.append(
            f'<line x1="{margin["left"]}" x2="{width - margin["right"]}" '
            f'y1="{y:.1f}" y2="{y:.1f}" class="grid"/>'
        )
        parts.append(
            f'<text x="{margin["left"] - 10}" y="{y + 4:.1f}" '
            f'text-anchor="end" class="axis">{fmt_num(tick)}</text>'
        )

    for tick in tick_values(x_low, x_high, count=6):
        x = x_scale(tick)
        parts.append(
            f'<line x1="{x:.1f}" x2="{x:.1f}" y1="{margin["top"]}" '
            f'y2="{height - margin["bottom"]}" class="grid"/>'
        )
        parts.append(
            f'<text x="{x:.1f}" y="{height - margin["bottom"] + 24}" '
            f'text-anchor="middle" class="axis">{html.escape(fmt_time_tick(tick))}</text>'
        )

    if include_zero and y_low < 0 < y_high:
        zero_y = y_scale(0)
        parts.append(
            f'<line x1="{margin["left"]}" x2="{width - margin["right"]}" '
            f'y1="{zero_y:.1f}" y2="{zero_y:.1f}" stroke="#6b7280" stroke-width="1.5"/>'
        )

    parts.append(
        f'<line x1="{margin["left"]}" x2="{margin["left"]}" y1="{margin["top"]}" '
        f'y2="{height - margin["bottom"]}" class="axis-line"/>'
    )
    parts.append(
        f'<line x1="{margin["left"]}" x2="{width - margin["right"]}" '
        f'y1="{height - margin["bottom"]}" y2="{height - margin["bottom"]}" '
        'class="axis-line"/>'
    )

    for label, field, color in series:
        points = [
            f'{x_scale(record["_time"].timestamp()):.1f},{y_scale(record[field]):.1f}'
            for record in records
            if record.get(field) is not None
        ]
        if points:
            parts.append(
                f'<polyline points="{" ".join(points)}" fill="none" stroke="{color}" '
                'stroke-width="2.4" stroke-linejoin="round" stroke-linecap="round"/>'
            )

    legend_x = margin["left"]
    legend_y = height - 24
    for index, (label, _, color) in enumerate(series):
        x = legend_x + index * 220
        parts.append(f'<line x1="{x}" x2="{x + 26}" y1="{legend_y}" y2="{legend_y}" '
                     f'stroke="{color}" stroke-width="3"/>')
        parts.append(
            f'<text x="{x + 34}" y="{legend_y + 4}" class="legend">{html.escape(label)}</text>'
        )

    parts.append(
        f'<text x="20" y="{margin["top"] + plot_height / 2:.1f}" '
        f'transform="rotate(-90 20 {margin["top"] + plot_height / 2:.1f})" '
        f'text-anchor="middle" class="axis">{html.escape(y_label)}</text>'
    )
    parts.append(svg_footer())
    path.write_text("\n".join(parts), encoding="utf-8")


def scatter_plot(
    path: Path,
    rows: list[dict],
    *,
    title: str,
    units: str,
    model_label: str = "HRRR",
) -> None:
    width = 1080
    height = 700
    margin = {"left": 78, "right": 42, "top": 70, "bottom": 78}
    plot_width = width - margin["left"] - margin["right"]
    plot_height = height - margin["top"] - margin["bottom"]
    station_colors = [
        "#111111",
        "#6f42c1",
        "#0f766e",
        "#b45309",
        "#7f1d1d",
        "#334155",
    ]

    values = []
    for row in rows:
        for field in ("speed_obs", "wn_speed", "wx_speed"):
            if row.get(field) is not None:
                values.append(row[field])
    _low, high = nice_range(values + [0.0])
    low = 0.0

    def scale_x(value: float) -> float:
        return margin["left"] + ((value - low) / (high - low)) * plot_width

    def scale_y(value: float) -> float:
        return margin["top"] + (1 - ((value - low) / (high - low))) * plot_height

    def station_color(station_id: str) -> str:
        stations = sorted({row["station_id"] for row in rows})
        return station_colors[stations.index(station_id) % len(station_colors)]

    def station_label(station_id: str) -> str:
        if station_id.startswith("USGS-"):
            return "USGS met station"
        return station_id

    def trend_line(points: list[tuple[float, float]]) -> tuple[float, float] | None:
        if len(points) < 2:
            return None
        x_mean = mean([point[0] for point in points])
        y_mean = mean([point[1] for point in points])
        denominator = sum((point[0] - x_mean) ** 2 for point in points)
        if math.isclose(denominator, 0.0):
            return None
        slope = sum((point[0] - x_mean) * (point[1] - y_mean) for point in points) / denominator
        intercept = y_mean - slope * x_mean
        return slope, intercept

    parts = svg_header(width, height, title)
    for tick in tick_values(low, high):
        x = scale_x(tick)
        y = scale_y(tick)
        parts.append(
            f'<line x1="{x:.1f}" x2="{x:.1f}" y1="{margin["top"]}" '
            f'y2="{height - margin["bottom"]}" class="grid"/>'
        )
        parts.append(
            f'<line x1="{margin["left"]}" x2="{width - margin["right"]}" '
            f'y1="{y:.1f}" y2="{y:.1f}" class="grid"/>'
        )
        parts.append(
            f'<text x="{x:.1f}" y="{height - margin["bottom"] + 24}" '
            f'text-anchor="middle" class="axis">{fmt_num(tick)}</text>'
        )
        parts.append(
            f'<text x="{margin["left"] - 10}" y="{y + 4:.1f}" '
            f'text-anchor="end" class="axis">{fmt_num(tick)}</text>'
        )

    parts.append(
        f'<line x1="{scale_x(low):.1f}" x2="{scale_x(high):.1f}" '
        f'y1="{scale_y(low):.1f}" y2="{scale_y(high):.1f}" '
        'stroke="#6b7280" stroke-width="1.4" stroke-dasharray="6 6"/>'
    )

    for row in rows:
        obs = row.get("speed_obs")
        if obs is None:
            continue
        color = station_color(row["station_id"])
        if row.get("wn_speed") is not None:
            parts.append(
                f'<circle cx="{scale_x(obs):.1f}" cy="{scale_y(row["wn_speed"]):.1f}" '
                f'r="3.2" fill="{color}" opacity="0.45"/>'
            )
        if row.get("wx_speed") is not None:
            parts.append(
                f'<rect x="{scale_x(obs) - 2.7:.1f}" y="{scale_y(row["wx_speed"]) - 2.7:.1f}" '
                f'width="5.4" height="5.4" fill="{color}" opacity="0.35"/>'
            )

    for station_id in sorted({row["station_id"] for row in rows}):
        station_rows = [row for row in rows if row["station_id"] == station_id]
        color = station_color(station_id)
        for field, dash in (("wn_speed", ""), ("wx_speed", "6 4")):
            points = [
                (row["speed_obs"], row[field])
                for row in station_rows
                if row.get("speed_obs") is not None and row.get(field) is not None
            ]
            line = trend_line(points)
            if not line:
                continue
            slope, intercept = line
            x1 = min(point[0] for point in points)
            x2 = max(point[0] for point in points)
            y1 = slope * x1 + intercept
            y2 = slope * x2 + intercept
            dash_attr = f' stroke-dasharray="{dash}"' if dash else ""
            parts.append(
                f'<line x1="{scale_x(x1):.1f}" y1="{scale_y(y1):.1f}" '
                f'x2="{scale_x(x2):.1f}" y2="{scale_y(y2):.1f}" '
                f'stroke="{color}" stroke-width="2.4" opacity="0.9"{dash_attr}/>'
            )

    parts.append(
        f'<line x1="{margin["left"]}" x2="{margin["left"]}" y1="{margin["top"]}" '
        f'y2="{height - margin["bottom"]}" class="axis-line"/>'
    )
    parts.append(
        f'<line x1="{margin["left"]}" x2="{width - margin["right"]}" '
        f'y1="{height - margin["bottom"]}" y2="{height - margin["bottom"]}" '
        'class="axis-line"/>'
    )
    parts.append(
        f'<text x="{width / 2:.1f}" y="{height - 28}" text-anchor="middle" class="axis">'
        f'Observed speed ({html.escape(units)})</text>'
    )
    parts.append(
        f'<text x="20" y="{margin["top"] + plot_height / 2:.1f}" '
        f'transform="rotate(-90 20 {margin["top"] + plot_height / 2:.1f})" '
        f'text-anchor="middle" class="axis">Modeled speed ({html.escape(units)})</text>'
    )
    legend_x = margin["left"] + 24
    legend_y = margin["top"] + 24
    legend_height = 154 + 24 * len({row["station_id"] for row in rows})
    parts.append(
        f'<rect x="{legend_x - 16}" y="{legend_y - 24}" width="250" height="{legend_height}" '
        'fill="#ffffff" opacity="0.88" stroke="#d8dee9" rx="4"/>'
    )
    parts.append(f'<circle cx="{legend_x}" cy="{legend_y}" r="4" fill="#333333"/>')
    parts.append(f'<text x="{legend_x + 12}" y="{legend_y + 4}" class="legend">WindNinja points</text>')
    parts.append(f'<rect x="{legend_x - 4}" y="{legend_y + 18}" width="8" height="8" fill="#333333" opacity="0.6"/>')
    parts.append(f'<text x="{legend_x + 12}" y="{legend_y + 26}" class="legend">{html.escape(model_label)} points</text>')
    parts.append(f'<line x1="{legend_x - 5}" x2="{legend_x + 22}" y1="{legend_y + 48}" y2="{legend_y + 48}" stroke="#333333" stroke-width="2.4"/>')
    parts.append(f'<text x="{legend_x + 32}" y="{legend_y + 52}" class="legend">WN fit</text>')
    parts.append(f'<line x1="{legend_x - 5}" x2="{legend_x + 22}" y1="{legend_y + 72}" y2="{legend_y + 72}" stroke="#333333" stroke-width="2.4" stroke-dasharray="6 4"/>')
    parts.append(f'<text x="{legend_x + 32}" y="{legend_y + 76}" class="legend">{html.escape(model_label)} fit</text>')
    parts.append(f'<text x="{legend_x}" y="{legend_y + 112}" class="axis">Stations</text>')
    for index, station_id in enumerate(sorted({row["station_id"] for row in rows})):
        y = legend_y + 138 + index * 24
        color = station_color(station_id)
        parts.append(f'<circle cx="{legend_x}" cy="{y}" r="4.5" fill="{color}"/>')
        parts.append(f'<text x="{legend_x + 12}" y="{y + 4}" class="legend">{html.escape(station_label(station_id))}</text>')
    parts.append(svg_footer())
    path.write_text("\n".join(parts), encoding="utf-8")


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", value).strip("_") or "station"


def infer_domain_from_windninja_path(path: Path) -> str | None:
    match = re.match(r"^(.+?)_(?:\d{2}-\d{2}-\d{4}|\d{8})_\d{4}", path.name)
    return match.group(1) if match else None


def terrain_path_for_domain(domain: str | None) -> Path | None:
    if not domain:
        return None
    config_path = BASE_DIR / "config" / "domains.json"
    if not config_path.exists():
        return None
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    domain_config = (payload.get("domains") or {}).get(domain) or {}
    elevation_file = domain_config.get("elevation_file")
    if not elevation_file:
        return None
    terrain = BASE_DIR / "static_data" / elevation_file
    if terrain.suffix.lower() == ".lcp":
        tif = terrain.with_suffix(".tif")
        if tif.exists():
            return tif
    return terrain if terrain.exists() else None


def sampling_point_maps(
    output_dir: Path,
    rows: list[dict],
    stations: list[dict],
    *,
    model_label: str,
) -> list[str]:
    rows_by_station: dict[str, dict] = {}
    for row in rows:
        station_id = row["station_id"]
        if station_id in rows_by_station:
            continue
        wn_path = resolve_artifact_path(row.get("wn_vel_path"))
        wx_path = resolve_artifact_path(row.get("wx_vel_path"))
        if wn_path and wx_path and wn_path.exists() and wx_path.exists():
            rows_by_station[station_id] = row
    if not rows_by_station:
        return []

    try:
        os.environ.setdefault("MPLCONFIGDIR", str((BASE_DIR / "runtime" / ".matplotlib").resolve()))
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import numpy as np
        import rasterio
        from rasterio.plot import plotting_extent
        from rasterio.warp import transform
        from rasterio.windows import from_bounds, transform as window_transform
    except Exception:
        return []

    wgs84_proj = "+proj=longlat +datum=WGS84 +no_defs"

    def project(crs, lon: float, lat: float) -> tuple[float, float]:
        xs, ys = transform(wgs84_proj, crs, [lon], [lat])
        return xs[0], ys[0]

    def lonlat(crs, x: float, y: float) -> tuple[float, float]:
        lons, lats = transform(crs, wgs84_proj, [x], [y])
        return lons[0], lats[0]

    def sample_cell(path: Path, lon: float, lat: float) -> dict | None:
        if not path.exists():
            return None
        with rasterio.open(path) as dataset:
            x, y = project(dataset.crs, lon, lat)
            row, col = dataset.index(x, y)
            if row < 0 or col < 0 or row >= dataset.height or col >= dataset.width:
                return None
            center_x, center_y = dataset.xy(row, col)
            sample_lon, sample_lat = lonlat(dataset.crs, center_x, center_y)
            return {
                "x": center_x,
                "y": center_y,
                "lon": sample_lon,
                "lat": sample_lat,
                "distance_m": math.hypot(center_x - x, center_y - y),
            }

    station_by_id = {station["station_id"]: station for station in stations}
    written = []
    for station_id, row in sorted(rows_by_station.items()):
        station = station_by_id.get(station_id)
        if not station:
            continue
        wn_path = resolve_artifact_path(row.get("wn_vel_path"))
        wx_path = resolve_artifact_path(row.get("wx_vel_path"))
        if not wn_path or not wx_path:
            continue
        terrain_path = terrain_path_for_domain(infer_domain_from_windninja_path(wn_path))
        if not terrain_path:
            continue

        station_lon = float(station["longitude"])
        station_lat = float(station["latitude"])
        wn_sample = sample_cell(wn_path, station_lon, station_lat)
        wx_sample = sample_cell(wx_path, station_lon, station_lat)
        if not wn_sample or not wx_sample:
            continue

        with rasterio.open(terrain_path) as terrain:
            station_x, station_y = project(terrain.crs, station_lon, station_lat)
            wn_x, wn_y = project(terrain.crs, wn_sample["lon"], wn_sample["lat"])
            wx_x, wx_y = project(terrain.crs, wx_sample["lon"], wx_sample["lat"])
            xs = [station_x, wn_x, wx_x]
            ys = [station_y, wn_y, wx_y]
            span = max(max(xs) - min(xs), max(ys) - min(ys), 3500.0)
            pad = max(span * 0.85, 1800.0)
            left = min(xs) - pad
            right = max(xs) + pad
            bottom = min(ys) - pad
            top = max(ys) + pad
            window = from_bounds(left, bottom, right, top, transform=terrain.transform)
            window = window.round_offsets().round_lengths()
            data = terrain.read(1, window=window, masked=True)
            transform_window = window_transform(window, terrain.transform)
            extent = plotting_extent(data, transform_window)

        rel_extent = [
            (extent[0] - station_x) / 1000.0,
            (extent[1] - station_x) / 1000.0,
            (extent[2] - station_y) / 1000.0,
            (extent[3] - station_y) / 1000.0,
        ]
        height, width = data.shape
        x_coords = np.linspace(rel_extent[0], rel_extent[1], width)
        y_coords = np.linspace(rel_extent[3], rel_extent[2], height)

        fig, ax = plt.subplots(figsize=(10.5, 9.0), dpi=160)
        ax.imshow(data, extent=rel_extent, cmap="terrain", alpha=0.82)
        clean = np.asarray(data.filled(np.nan), dtype=float)
        finite = clean[np.isfinite(clean)]
        if finite.size:
            low = math.floor(float(np.nanmin(finite)) / 100.0) * 100.0
            high = math.ceil(float(np.nanmax(finite)) / 100.0) * 100.0
            levels = np.arange(low, high + 1, 100.0)
            if len(levels) > 1:
                ax.contour(
                    x_coords,
                    y_coords,
                    clean,
                    levels=levels,
                    colors="#2d3748",
                    alpha=0.32,
                    linewidths=0.45,
                )

        station_rel = (0.0, 0.0)
        wn_rel = ((wn_x - station_x) / 1000.0, (wn_y - station_y) / 1000.0)
        wx_rel = ((wx_x - station_x) / 1000.0, (wx_y - station_y) / 1000.0)
        ax.plot(*station_rel, marker="*", color="#111111", markersize=15, markeredgecolor="white",
                markeredgewidth=1.3, linestyle="none", label=f"{station_id} station")
        ax.plot(*wn_rel, marker="o", color=COLORS["windninja"], markersize=8, linestyle="none",
                label="WindNinja sampled cell")
        ax.plot(*wx_rel, marker="s", color=COLORS["hrrr"], markersize=8, linestyle="none",
                label=f"{model_label} sampled cell")
        ax.plot([station_rel[0], wn_rel[0]], [station_rel[1], wn_rel[1]],
                color=COLORS["windninja"], linewidth=1.2)
        ax.plot([station_rel[0], wx_rel[0]], [station_rel[1], wx_rel[1]],
                color=COLORS["hrrr"], linewidth=1.2)

        ax.annotate(
            f"WindNinja sample\nnearest output cell\n{wn_sample['distance_m']:.0f} m from station",
            xy=wn_rel,
            xytext=(18, -54),
            textcoords="offset points",
            color="#1f4f82",
            fontsize=9.5,
            bbox={"boxstyle": "round,pad=0.25", "fc": "white", "ec": "#8ab6dd", "alpha": 0.92},
            arrowprops={"arrowstyle": "-", "color": COLORS["windninja"], "lw": 1.0},
        )
        ax.annotate(
            f"{model_label} parent sample\nnearest raster cell\n{wx_sample['distance_m'] / 1000.0:.2f} km from station",
            xy=wx_rel,
            xytext=(-132, 58),
            textcoords="offset points",
            color="#8b1e1e",
            fontsize=9.5,
            bbox={"boxstyle": "round,pad=0.25", "fc": "white", "ec": "#dd9a9a", "alpha": 0.92},
            arrowprops={"arrowstyle": "-", "color": COLORS["hrrr"], "lw": 1.0},
        )
        ax.annotate(
            f"{station_id}\n{station_lat:.5f}, {station_lon:.5f}",
            xy=station_rel,
            xytext=(22, 18),
            textcoords="offset points",
            fontsize=10.5,
            fontweight="bold",
            bbox={"boxstyle": "round,pad=0.25", "fc": "white", "ec": "#d8dee9", "alpha": 0.92},
        )
        ax.set_title(f"{station_id} Validation Sampling Points", fontsize=16, fontweight="bold")
        ax.set_xlabel(f"Kilometers east/west of {station_id}")
        ax.set_ylabel(f"Kilometers north/south of {station_id}")
        ax.grid(color="white", alpha=0.45, linewidth=0.8)
        ax.legend(loc="lower left", framealpha=0.92)
        fig.text(
            0.02,
            0.018,
            "Background: domain DEM terrain with 100 m contours. "
            "Samples are nearest WindNinja output and parent-model raster cells.",
            fontsize=9,
            color="#333333",
        )
        fig.tight_layout(rect=(0, 0.04, 1, 1))

        filename = f"sampling_map_{safe_name(station_id)}.png"
        fig.savefig(output_dir / filename, facecolor="white")
        plt.close(fig)
        written.append(filename)

    return written


def write_station_metrics(path: Path, station_metrics: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=STATION_METRIC_FIELDS)
        writer.writeheader()
        writer.writerows(station_metrics)


def write_summary_json(
    path: Path,
    summary: dict,
    source_paths: list[Path],
    plots: list[str],
    station_metrics: list[dict],
    diagnostics: dict,
) -> None:
    payload = {
        "generated_at_utc": dt.datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_paths": [str(path) for path in source_paths],
        "plots": plots,
        "summary": summary,
        "station_metrics": station_metrics,
        "analysis": diagnostics,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def format_table_value(value, suffix: str = "") -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        if math.isnan(value):
            return "n/a"
        if abs(value) >= 100:
            text = f"{value:.0f}"
        elif abs(value) >= 10:
            text = f"{value:.1f}"
        else:
            text = f"{value:.2f}".rstrip("0").rstrip(".")
    else:
        text = str(value)
    return f"{text}{suffix}"


def metric_cell(record: dict, model: str, metric: str, suffix: str = "") -> str:
    return html.escape(format_table_value(record[model].get(metric), suffix))


def improvement_cell(record: dict, metric: str, suffix: str = "") -> str:
    value = record["improvement"].get(metric)
    return html.escape(format_table_value(value, suffix))


def diagnostic_table(title: str, records: list[dict], model_label: str) -> str:
    rows = "\n".join(
        "<tr>"
        f"<td>{html.escape(str(record['label']))}</td>"
        f"<td>{html.escape(str(record['sample_count']))}</td>"
        f"<td>{metric_cell(record, 'hrrr', 'speed_mae')}</td>"
        f"<td>{metric_cell(record, 'windninja', 'speed_mae')}</td>"
        f"<td>{improvement_cell(record, 'speed_mae')}</td>"
        f"<td>{metric_cell(record, 'hrrr', 'vector_rmse')}</td>"
        f"<td>{metric_cell(record, 'windninja', 'vector_rmse')}</td>"
        f"<td>{improvement_cell(record, 'vector_rmse')}</td>"
        f"<td>{metric_cell(record, 'hrrr', 'dir_mae_deg', ' deg')}</td>"
        f"<td>{metric_cell(record, 'windninja', 'dir_mae_deg', ' deg')}</td>"
        f"<td>{improvement_cell(record, 'dir_mae_deg', ' deg')}</td>"
        "</tr>"
        for record in records
    )
    return f"""
  <section>
    <h2>{html.escape(title)}</h2>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Segment</th>
            <th>N</th>
            <th>{html.escape(model_label)} speed MAE</th>
            <th>WN speed MAE</th>
            <th>Speed diff</th>
            <th>{html.escape(model_label)} vector RMSE</th>
            <th>WN vector RMSE</th>
            <th>Vector diff</th>
            <th>{html.escape(model_label)} dir MAE</th>
            <th>WN dir MAE</th>
            <th>Dir diff</th>
          </tr>
        </thead>
        <tbody>
          {rows}
        </tbody>
      </table>
    </div>
  </section>
"""


def noticeable_patterns_table(patterns: list[dict], model_label: str) -> str:
    if not patterns:
        body = (
            '<p class="note">No grouped pattern met the support threshold '
            "of at least 20 samples and the minimum model-difference thresholds.</p>"
        )
    else:
        rows = "\n".join(
            "<tr>"
            f"<td>{html.escape(str(pattern['category']))}</td>"
            f"<td>{html.escape(str(pattern['label']))}</td>"
            f"<td>{html.escape(str(pattern['sample_count']))}</td>"
            f"<td>{html.escape(str(pattern['winner']))}</td>"
            f"<td>{html.escape(str(pattern['metric']))}</td>"
            f"<td>{html.escape(format_table_value(pattern['difference']))}</td>"
            "</tr>"
            for pattern in patterns
        )
        body = f"""
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Category</th>
            <th>Segment</th>
            <th>N</th>
            <th>Lower error</th>
            <th>Metric</th>
            <th>Difference</th>
          </tr>
        </thead>
        <tbody>
          {rows}
        </tbody>
      </table>
    </div>
"""
    return f"""
  <section>
    <h2>Noticeable Patterns</h2>
    <p class="note">
      A pattern is listed only when N >= 20 and WindNinja differs from
      {html.escape(model_label)} by at least 1 mph for speed/vector metrics
      or 10 degrees for direction MAE. Positive differences mean WindNinja
      has lower error.
    </p>
    {body}
  </section>
"""


def event_table(title: str, events: list[dict], model_label: str) -> str:
    if not events:
        rows = '<tr><td colspan="11">No events available.</td></tr>'
    else:
        rows = "\n".join(
            "<tr>"
            f"<td>{html.escape(str(event['sample_time_utc']))}</td>"
            f"<td>{html.escape(str(event['station_id']))}</td>"
            f"<td>{html.escape(format_table_value(event['speed_obs']))}</td>"
            f"<td>{html.escape(format_table_value(event['dir_obs_deg'], ' deg'))}</td>"
            f"<td>{html.escape(format_table_value(event['wn_vector_error']))}</td>"
            f"<td>{html.escape(format_table_value(event['wx_vector_error']))}</td>"
            f"<td>{html.escape(format_table_value(event['vector_error_improvement']))}</td>"
            f"<td>{html.escape(format_table_value(event['wn_speed_error']))}</td>"
            f"<td>{html.escape(format_table_value(event['wx_speed_error']))}</td>"
            f"<td>{html.escape(format_table_value(event['wn_dir_abs_error_deg'], ' deg'))}</td>"
            f"<td>{html.escape(format_table_value(event['wx_dir_abs_error_deg'], ' deg'))}</td>"
            "</tr>"
            for event in events
        )
    return f"""
  <section>
    <h2>{html.escape(title)}</h2>
    <p class="note">Positive vector diff means WindNinja has lower vector error.</p>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Time</th>
            <th>Station</th>
            <th>Obs speed</th>
            <th>Obs dir</th>
            <th>WN vector</th>
            <th>{html.escape(model_label)} vector</th>
            <th>Vector diff</th>
            <th>WN speed err</th>
            <th>{html.escape(model_label)} speed err</th>
            <th>WN dir err</th>
            <th>{html.escape(model_label)} dir err</th>
          </tr>
        </thead>
        <tbody>
          {rows}
        </tbody>
      </table>
    </div>
  </section>
"""


def write_index(
    path: Path,
    summary: dict,
    plots: list[str],
    title: str,
    station_metrics: list[dict],
    diagnostics: dict,
) -> None:
    def card(label: str, value: str) -> str:
        return (
            '<div class="card">'
            f'<div class="label">{html.escape(label)}</div>'
            f'<div class="value">{html.escape(value)}</div>'
            '</div>'
        )

    wn = summary["windninja"]
    wx = summary["hrrr"]
    model_label = summary.get("model_label", "HRRR")
    cards = [
        card("Samples", str(summary["sample_count"])),
        card("Stations", str(summary["station_count"])),
        card("Window", f'{summary["start_utc"]} to {summary["end_utc"]}'),
        card("WN Speed MAE", f'{wn["speed_mae"]} mph'),
        card(f"{model_label} Speed MAE", f'{wx["speed_mae"]} mph'),
        card("WN Vector RMSE", f'{wn["vector_rmse"]} mph'),
        card(f"{model_label} Vector RMSE", f'{wx["vector_rmse"]} mph'),
        card("WN Direction MAE", f'{wn["dir_mae_deg"]} deg'),
        card(f"{model_label} Direction MAE", f'{wx["dir_mae_deg"]} deg'),
    ]
    metric_rows = "\n".join(
        "<tr>"
        f"<td>{html.escape(str(row['station_id']))}</td>"
        f"<td>{html.escape(str(row['sample_count']))}</td>"
        f"<td>{html.escape(format_table_value(row['obs_height_m'], ' m'))}</td>"
        f"<td>{html.escape(format_table_value(row['wx_sample_distance_m'], ' m'))}</td>"
        f"<td>{html.escape(format_table_value(row['wn_sample_distance_m'], ' m'))}</td>"
        f"<td>{html.escape(format_table_value(row['wx_speed_mae']))}</td>"
        f"<td>{html.escape(format_table_value(row['wn_speed_mae']))}</td>"
        f"<td>{html.escape(format_table_value(row['speed_mae_skill']))}</td>"
        f"<td>{html.escape(format_table_value(row['wx_vector_rmse']))}</td>"
        f"<td>{html.escape(format_table_value(row['wn_vector_rmse']))}</td>"
        f"<td>{html.escape(format_table_value(row['vector_rmse_skill']))}</td>"
        f"<td>{html.escape(format_table_value(row['wx_dir_mae_ge_5mph'], ' deg'))}</td>"
        f"<td>{html.escape(format_table_value(row['wn_dir_mae_ge_5mph'], ' deg'))}</td>"
        f"<td>{html.escape(format_table_value(row['speed_mae_improvement_ci_low']))}"
        " to "
        f"{html.escape(format_table_value(row['speed_mae_improvement_ci_high']))}</td>"
        "</tr>"
        for row in station_metrics
    )
    metrics_table = ""
    if metric_rows:
        metrics_table = f"""
  <section>
    <h2>Station-Level Metrics</h2>
    <p class="note">
      Skill is 1 - WindNinja error / parent-model error. Direction MAE is also reported
      for observed speed >= 5 mph to reduce light-wind direction noise.
    </p>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Station</th>
            <th>N</th>
            <th>Obs height</th>
            <th>{html.escape(model_label)} dist</th>
            <th>WN dist</th>
            <th>{html.escape(model_label)} speed MAE</th>
            <th>WN speed MAE</th>
            <th>Speed skill</th>
            <th>{html.escape(model_label)} vector RMSE</th>
            <th>WN vector RMSE</th>
            <th>Vector skill</th>
            <th>{html.escape(model_label)} dir MAE >=5</th>
            <th>WN dir MAE >=5</th>
            <th>Speed improvement 95% CI</th>
          </tr>
        </thead>
        <tbody>
          {metric_rows}
        </tbody>
      </table>
    </div>
  </section>
"""
    diagnostic_sections = (
        noticeable_patterns_table(diagnostics["noticeable_patterns"], model_label)
        + diagnostic_table("Monthly Metrics", diagnostics["monthly"], model_label)
        + diagnostic_table("UTC-Hour Metrics", diagnostics["utc_hour"], model_label)
        + diagnostic_table("Observed-Speed Bin Metrics", diagnostics["observed_speed_bins"], model_label)
        + diagnostic_table(
            "Observed-Direction Sector Metrics",
            diagnostics["observed_direction_sectors"],
            model_label,
        )
    )
    event_sections = (
        event_table("Top Vector Error Events", diagnostics["top_error_events"], model_label)
        + event_table("Top WindNinja Wins", diagnostics["top_windninja_wins"], model_label)
        + event_table("Top WindNinja Losses", diagnostics["top_windninja_losses"], model_label)
    )
    images = "\n".join(
        f'<section><h2>{html.escape(Path(plot).stem.replace("_", " ").title())}</h2>'
        f'<img src="{html.escape(plot)}" alt="{html.escape(plot)}"></section>'
        for plot in plots
    )
    content = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{html.escape(title)}</title>
  <style>
    body {{
      margin: 0;
      background: #f5f7fa;
      color: #1f2933;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    main {{ max-width: 1100px; margin: 0 auto; padding: 28px; }}
    h1 {{ margin: 0 0 18px; font-size: 30px; }}
    h2 {{ margin: 28px 0 12px; font-size: 18px; }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
      gap: 12px;
      margin-bottom: 22px;
    }}
    .card {{
      background: white;
      border: 1px solid #d8dee9;
      border-radius: 6px;
      padding: 14px;
    }}
    .label {{ color: #52616f; font-size: 12px; text-transform: uppercase; }}
    .value {{ margin-top: 4px; font-size: 18px; font-weight: 700; }}
    section {{
      background: white;
      border: 1px solid #d8dee9;
      border-radius: 6px;
      padding: 16px;
      margin-bottom: 18px;
    }}
    .note {{ margin: 0 0 12px; color: #52616f; font-size: 14px; }}
    .table-wrap {{ overflow-x: auto; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #e2e8f0; padding: 8px 10px; text-align: right; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ color: #52616f; font-weight: 700; white-space: nowrap; }}
    img {{ display: block; width: 100%; height: auto; }}
  </style>
</head>
<body>
<main>
  <h1>{html.escape(title)}</h1>
  <div class="cards">
    {"".join(cards)}
  </div>
  {metrics_table}
  {diagnostic_sections}
  {event_sections}
  {images}
</main>
</body>
</html>
"""
    path.write_text(content, encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create SVG/HTML plots from validation samples."
    )
    parser.add_argument(
        "--study-root",
        default="runtime/validation/berthoud_pass",
        help="Validation study root containing chunks/*/samples.csv.",
    )
    parser.add_argument(
        "--samples-csv",
        action="append",
        help="Specific samples CSV to plot. Can be passed more than once.",
    )
    parser.add_argument(
        "--output-dir",
        help="Output directory. Defaults to <study-root>/plots.",
    )
    parser.add_argument("--station-id", help="Optional station ID filter.")
    parser.add_argument("--title", default="Berthoud Pass Validation")
    parser.add_argument("--speed-units", default="mph")
    parser.add_argument("--model-label", help="Parent weather model label for plot legends.")
    return parser


def infer_model_label(study_root: Path, override: str | None) -> str:
    if override:
        return override
    summary_path = study_root / "summary.json"
    if summary_path.exists():
        try:
            payload = json.loads(summary_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return "HRRR"
        model = payload.get("model")
        if model:
            return str(model)
    return "HRRR"


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    study_root = resolve_repo_path(args.study_root)
    source_paths = (
        [resolve_repo_path(path) for path in args.samples_csv]
        if args.samples_csv
        else collect_sample_paths(study_root)
    )
    output_dir = resolve_repo_path(args.output_dir) if args.output_dir else study_root / "plots"
    output_dir.mkdir(parents=True, exist_ok=True)

    model_label = infer_model_label(study_root, args.model_label)
    rows = load_samples(source_paths, args.station_id)
    summary = metric_summary(rows, model_label)
    diagnostics = build_diagnostics(rows, model_label)
    time_fields = [
        "speed_obs",
        "wn_speed",
        "wx_speed",
        "wn_speed_error",
        "wx_speed_error",
        "wn_dir_abs_error_deg",
        "wx_dir_abs_error_deg",
    ]
    time_records = average_by_time(rows, time_fields)

    plots = [
        "speed_timeseries.svg",
        "speed_error_timeseries.svg",
        "direction_error_timeseries.svg",
        "speed_scatter.svg",
        "daily_metrics.svg",
    ]
    station_metadata = load_station_metadata(study_root, args.station_id)
    station_metrics = build_station_metrics(rows, station_metadata)
    write_station_metrics(output_dir / "station_metrics.csv", station_metrics)
    if station_metadata:
        plots.extend(sampling_point_maps(
            output_dir,
            rows,
            station_metadata,
            model_label=model_label,
        ))
    line_plot(
        output_dir / plots[0],
        time_records,
        [
            ("Observed", "speed_obs", COLORS["obs"]),
            ("WindNinja", "wn_speed", COLORS["windninja"]),
            (model_label, "wx_speed", COLORS["hrrr"]),
        ],
        title="Wind Speed Time Series",
        y_label=f"Speed ({args.speed_units})",
        include_zero=True,
    )
    line_plot(
        output_dir / plots[1],
        time_records,
        [
            ("WindNinja error", "wn_speed_error", COLORS["windninja"]),
            (f"{model_label} error", "wx_speed_error", COLORS["hrrr"]),
        ],
        title="Wind Speed Error",
        y_label=f"Modeled minus observed ({args.speed_units})",
        include_zero=True,
    )
    line_plot(
        output_dir / plots[2],
        time_records,
        [
            ("WindNinja", "wn_dir_abs_error_deg", COLORS["windninja"]),
            (model_label, "wx_dir_abs_error_deg", COLORS["hrrr"]),
        ],
        title="Direction Absolute Error",
        y_label="Degrees",
        include_zero=True,
    )
    scatter_plot(
        output_dir / plots[3],
        rows,
        title="Observed vs Modeled Wind Speed",
        units=args.speed_units,
        model_label=model_label,
    )
    line_plot(
        output_dir / plots[4],
        daily_records(rows),
        [
            ("WN vector RMSE", "wn_vector_rmse", COLORS["windninja"]),
            (f"{model_label} vector RMSE", "wx_vector_rmse", COLORS["hrrr"]),
            ("WN speed MAE", "wn_speed_mae", "#5dade2"),
            (f"{model_label} speed MAE", "wx_speed_mae", "#e74c3c"),
        ],
        title="Daily Error Metrics",
        y_label=f"Error ({args.speed_units})",
        include_zero=True,
    )

    write_summary_json(
        output_dir / "plot_summary.json",
        summary,
        source_paths,
        plots,
        station_metrics,
        diagnostics,
    )
    write_index(
        output_dir / "index.html",
        summary,
        plots,
        args.title,
        station_metrics,
        diagnostics,
    )

    print(f"Wrote validation plots to {output_dir}")
    print(f"Samples: {summary['sample_count']} | Stations: {summary['station_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
