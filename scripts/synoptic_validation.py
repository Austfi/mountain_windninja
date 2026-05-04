#!/usr/bin/env python3
"""Prepare WindNinja point files and validate runs against Synoptic stations."""
from __future__ import annotations

import argparse
import bisect
import csv
import datetime as dt
import json
import math
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import config_loader
import utils

logger = utils.setup_logging("synoptic_validation")

API_BASE = "https://api.synopticdata.com/v2/stations"
UTC = dt.timezone.utc


def parse_utc_timestamp(raw_value: str) -> dt.datetime:
    formats = (
        "%Y%m%d%H%M",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M",
    )
    for fmt in formats:
        try:
            return dt.datetime.strptime(raw_value, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    raise ValueError(
        f"Unsupported UTC timestamp '{raw_value}'. Use YYYYMMDDHHMM or YYYY-MM-DDTHH:MM."
    )


def isoformat_utc(value: dt.datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def ymdhm_utc(value: dt.datetime) -> str:
    return value.astimezone(UTC).strftime("%Y%m%d%H%M")


def resolve_repo_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return (config_loader.BASE_DIR / path).resolve()


def ensure_parent(path: Path) -> None:
    utils.ensure_dir(str(path.parent))


def get_synoptic_token(explicit_token: str | None = None) -> str:
    token = (
        explicit_token
        or os.getenv("MWN_SYNOPTIC_TOKEN")
        or os.getenv("SYNOPTIC_TOKEN")
        or os.getenv("CUSTOM_API_KEY")
    )
    if not token:
        raise ValueError(
            "Missing Synoptic token. Pass --token or set MWN_SYNOPTIC_TOKEN "
            "(CUSTOM_API_KEY is also supported)."
        )
    return token


def fetch_synoptic_json(service: str, params: dict[str, str], token: str) -> dict:
    query = {"token": token, **params}
    url = f"{API_BASE}/{service}?{urllib.parse.urlencode(query)}"
    with urllib.request.urlopen(url) as response:
        payload = json.load(response)

    summary = payload.get("SUMMARY", {})
    if str(summary.get("RESPONSE_CODE")) not in {"1", "OK", ""}:
        raise RuntimeError(
            f"Synoptic {service} request failed: "
            f"{summary.get('RESPONSE_MESSAGE') or summary}"
        )
    return payload


def load_station_manifest(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = []
        seen = set()
        for raw in reader:
            station_id = (raw.get("station_id") or raw.get("stid") or "").strip().upper()
            if not station_id or station_id in seen:
                continue
            seen.add(station_id)
            rows.append({
                "station_id": station_id,
                "label": (raw.get("label") or "").strip(),
                "group": (raw.get("group") or "").strip(),
                "height_m_override": (raw.get("height_m_override") or raw.get("height_m") or "").strip(),
            })
    if not rows:
        raise ValueError(f"No stations found in manifest: {path}")
    return rows


def station_lookup(manifest_rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {row["station_id"]: row for row in manifest_rows}


def parse_float(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def normalize_sensor_key(key: str, variable: str) -> str:
    prefix_value = f"{variable}_value_"
    prefix_set = f"{variable}_set_"
    if key.startswith(prefix_value):
        return f"{variable}_{key[len(prefix_value):]}"
    if key.startswith(prefix_set):
        return f"{variable}_{key[len(prefix_set):]}"
    return key


def parse_iso_time(raw_value: str) -> dt.datetime:
    if raw_value.endswith("Z"):
        raw_value = raw_value.replace("Z", "+00:00")
    value = dt.datetime.fromisoformat(raw_value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def choose_sensor_position_m(station: dict, variable: str) -> tuple[str | None, float | None]:
    sensors = (station.get("SENSOR_VARIABLES") or {}).get(variable) or {}
    unit = ((station.get("UNITS") or {}).get("position") or "m").lower()

    best_key = None
    best_height = None
    best_end = dt.datetime.min.replace(tzinfo=UTC)

    for sensor_key, meta in sensors.items():
        if not isinstance(meta, dict):
            continue
        height = parse_float(meta.get("position"))
        if height is None:
            continue
        if unit.startswith("ft"):
            height *= 0.3048
        end_raw = ((meta.get("PERIOD_OF_RECORD") or {}).get("end")) or "1900-01-01T00:00:00Z"
        try:
            end_time = parse_iso_time(end_raw)
        except ValueError:
            end_time = dt.datetime.min.replace(tzinfo=UTC)
        if end_time >= best_end:
            best_key = sensor_key
            best_height = height
            best_end = end_time

    return best_key, best_height


def build_station_records(metadata_payload: dict,
                          manifest_rows: list[dict[str, str]],
                          default_height_m: float | None = None) -> list[dict]:
    manifest_by_id = station_lookup(manifest_rows)
    records = []

    for station in metadata_payload.get("STATION", []):
        station_id = (station.get("STID") or "").upper()
        manifest_row = manifest_by_id.get(station_id)
        if not manifest_row:
            continue

        sensor_key, sensor_height_m = choose_sensor_position_m(station, "wind_speed")
        override_height_m = parse_float(manifest_row["height_m_override"])
        chosen_height_m = override_height_m or sensor_height_m or default_height_m
        if chosen_height_m is None:
            raise ValueError(
                f"No wind sensor height available for {station_id}. "
                "Set height_m_override in the station manifest or pass --default-height."
            )

        record = {
            "station_id": station_id,
            "label": manifest_row["label"] or station.get("NAME") or station_id,
            "group": manifest_row["group"] or "ungrouped",
            "name": station.get("NAME") or station_id,
            "latitude": parse_float(station.get("LATITUDE")),
            "longitude": parse_float(station.get("LONGITUDE")),
            "elevation_ft": parse_float(station.get("ELEVATION")),
            "height_m": round(chosen_height_m, 3),
            "height_source": (
                "manifest_override" if override_height_m is not None
                else "synoptic_sensor_metadata" if sensor_height_m is not None
                else "default_height"
            ),
            "wind_speed_sensor_key": sensor_key,
            "network_id": str(station.get("MNET_ID", "")),
            "network_name": station.get("MNET_LONGNAME") or station.get("MNET_SHORTNAME") or "",
        }
        if record["latitude"] is None or record["longitude"] is None:
            raise ValueError(f"Missing coordinates for station {station_id}")
        records.append(record)

    if len(records) != len(manifest_rows):
        found_ids = {record["station_id"] for record in records}
        missing = [row["station_id"] for row in manifest_rows if row["station_id"] not in found_ids]
        raise ValueError(f"Metadata lookup missing stations: {', '.join(missing)}")

    return records


def compute_bbox(station_records: list[dict], padding_km: float) -> dict[str, float]:
    latitudes = [record["latitude"] for record in station_records]
    longitudes = [record["longitude"] for record in station_records]
    mid_lat_rad = math.radians(sum(latitudes) / len(latitudes))
    lat_pad = padding_km / 111.32
    lon_pad = padding_km / max(111.32 * math.cos(mid_lat_rad), 1e-6)
    return {
        "north": max(latitudes) + lat_pad,
        "east": max(longitudes) + lon_pad,
        "south": min(latitudes) - lat_pad,
        "west": min(longitudes) - lon_pad,
    }


def write_points_csv(path: Path, station_records: list[dict]) -> None:
    ensure_parent(path)
    with path.open("w", encoding="utf-8", newline="") as handle:
        handle.write("WGS84\n")
        writer = csv.writer(handle)
        writer.writerow(["point_name", "latitude", "longitude", "height_meters_above_ground"])
        for record in station_records:
            writer.writerow([
                record["station_id"],
                f"{record['latitude']:.8f}",
                f"{record['longitude']:.8f}",
                f"{record['height_m']:.3f}",
            ])


def write_json(path: Path, payload: dict) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def prepare_points(args) -> int:
    token = get_synoptic_token(args.token)
    station_file = resolve_repo_path(args.station_file)
    points_output = resolve_repo_path(args.points_output)
    metadata_output = resolve_repo_path(args.metadata_output)
    bbox_output = resolve_repo_path(args.bbox_output) if args.bbox_output else None

    manifest_rows = load_station_manifest(station_file)
    station_ids = ",".join(row["station_id"] for row in manifest_rows)
    params = {
        "stid": station_ids,
        "complete": "1",
        "sensorvars": "1",
    }
    if bool(args.start) != bool(args.end):
        raise ValueError("--start and --end must be provided together for prepare-points.")
    if args.start and args.end:
        start_time = parse_utc_timestamp(args.start)
        end_time = parse_utc_timestamp(args.end)
        params["obrange"] = f"{ymdhm_utc(start_time)},{ymdhm_utc(end_time)}"

    payload = fetch_synoptic_json("metadata", params, token)
    station_records = build_station_records(
        payload,
        manifest_rows,
        default_height_m=args.default_height,
    )
    bbox = compute_bbox(station_records, padding_km=args.padding_km)

    write_points_csv(points_output, station_records)
    write_json(metadata_output, {
        "generated_at_utc": isoformat_utc(dt.datetime.now(UTC)),
        "source_station_file": str(station_file),
        "bbox": bbox,
        "stations": station_records,
    })
    if bbox_output:
        write_json(bbox_output, bbox)

    logger.info(f"Wrote WindNinja points CSV: {points_output}")
    logger.info(f"Wrote station metadata JSON: {metadata_output}")
    logger.info(
        "Suggested bbox: "
        f"{bbox['north']:.8f}, {bbox['east']:.8f}, {bbox['south']:.8f}, {bbox['west']:.8f}"
    )
    return 0


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def model_speed_direction(u_value: float, v_value: float) -> tuple[float, float]:
    speed = math.hypot(u_value, v_value)
    direction = (270.0 - math.degrees(math.atan2(v_value, u_value))) % 360.0
    return speed, direction


def obs_to_uv(speed: float, direction_deg: float) -> tuple[float, float]:
    radians = math.radians(direction_deg)
    return (-speed * math.sin(radians), -speed * math.cos(radians))


def circular_abs_error_deg(model_dir: float, obs_dir: float) -> float:
    return abs(((model_dir - obs_dir + 180.0) % 360.0) - 180.0)


def load_model_points(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = []
        for raw in reader:
            row = {
                "station_id": (raw.get("ID") or raw.get("point_name") or "").strip().upper(),
                "datetime": parse_iso_time(raw["datetime"]),
                "u": float(raw["u"]),
                "v": float(raw["v"]),
                "wx_u": float(raw["wx_u"]),
                "wx_v": float(raw["wx_v"]),
            }
            rows.append(row)
    if not rows:
        raise ValueError(f"No WindNinja point rows found in {path}")
    return rows


def choose_observation_key(station: dict, variable: str, target_height_m: float | None) -> str | None:
    observations = station.get("OBSERVATIONS") or {}
    sensor_meta = (station.get("SENSOR_VARIABLES") or {}).get(variable) or {}
    unit = ((station.get("UNITS") or {}).get("position") or "m").lower()
    best_choice = None

    for key, values in observations.items():
        if key == "date_time" or not key.startswith(f"{variable}_") or not isinstance(values, list):
            continue
        non_null = sum(1 for value in values if value not in (None, ""))
        if non_null == 0:
            continue
        normalized = normalize_sensor_key(key, variable)
        meta = sensor_meta.get(key) or sensor_meta.get(normalized) or {}
        height_m = parse_float(meta.get("position"))
        if height_m is not None and unit.startswith("ft"):
            height_m *= 0.3048
        if height_m is None:
            height_penalty = 1e9
        else:
            height_penalty = abs(height_m - target_height_m) if target_height_m is not None else 0.0
        score = (height_penalty, -non_null, key)
        if best_choice is None or score < best_choice[0]:
            best_choice = (score, key)

    return best_choice[1] if best_choice else None


def extract_station_observations(station: dict, target_height_m: float | None) -> list[dict]:
    observations = station.get("OBSERVATIONS") or {}
    timestamps = [parse_iso_time(value) for value in observations.get("date_time", [])]
    if not timestamps:
        return []

    speed_key = choose_observation_key(station, "wind_speed", target_height_m)
    dir_key = choose_observation_key(station, "wind_direction", target_height_m)
    if not speed_key or not dir_key:
        return []

    speed_values = observations.get(speed_key, [])
    dir_values = observations.get(dir_key, [])
    rows = []
    for index, timestamp in enumerate(timestamps):
        if index >= len(speed_values) or index >= len(dir_values):
            break
        speed = speed_values[index]
        direction = dir_values[index]
        if speed in (None, ""):
            continue
        speed = float(speed)
        if direction in (None, ""):
            if abs(speed) < 1e-9:
                direction = 0.0
            else:
                continue
        direction = float(direction)
        u_obs, v_obs = obs_to_uv(speed, direction)
        rows.append({
            "datetime": timestamp,
            "speed_obs": speed,
            "dir_obs_deg": direction,
            "u_obs": u_obs,
            "v_obs": v_obs,
        })
    return rows


def fetch_synoptic_observations(station_ids: list[str],
                                start_time: dt.datetime,
                                end_time: dt.datetime,
                                tolerance_minutes: int,
                                token: str,
                                speed_units: str) -> dict[str, list[dict]]:
    expanded_start = start_time - dt.timedelta(minutes=tolerance_minutes)
    expanded_end = end_time + dt.timedelta(minutes=tolerance_minutes)
    payload = fetch_synoptic_json("timeseries", {
        "stid": ",".join(station_ids),
        "start": ymdhm_utc(expanded_start),
        "end": ymdhm_utc(expanded_end),
        "vars": "wind_speed,wind_direction",
        "units": f"speed|{speed_units}",
        "obtimezone": "UTC",
        "sensorvars": "1",
        "showemptystations": "1",
        "showemptyvars": "1",
        "qc": "on",
        "qc_flags": "on",
        "qc_checks": "synopticlabs,madis",
        "qc_remove_data": "on",
    }, token)

    return {station["STID"].upper(): station for station in payload.get("STATION", [])}


def nearest_observation(observations: list[dict],
                        target_time: dt.datetime,
                        tolerance_minutes: int) -> dict | None:
    if not observations:
        return None

    timestamps = [row["datetime"] for row in observations]
    insert_at = bisect.bisect_left(timestamps, target_time)
    candidates = []
    if insert_at < len(observations):
        candidates.append(observations[insert_at])
    if insert_at > 0:
        candidates.append(observations[insert_at - 1])

    tolerance = dt.timedelta(minutes=tolerance_minutes)
    best_row = None
    best_delta = None
    for row in candidates:
        delta = abs(row["datetime"] - target_time)
        if delta <= tolerance and (best_delta is None or delta < best_delta):
            best_row = row
            best_delta = delta
    return best_row


def mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def rmse(values: list[float]) -> float | None:
    return math.sqrt(sum(value * value for value in values) / len(values)) if values else None


def summarize_samples(sample_rows: list[dict]) -> dict:
    def metrics(prefix: str) -> dict[str, float | None]:
        speed_errors = [row[f"{prefix}_speed_error"] for row in sample_rows]
        direction_errors = [row[f"{prefix}_dir_abs_error_deg"] for row in sample_rows]
        vector_errors = [row[f"{prefix}_vector_error"] for row in sample_rows]
        return {
            "speed_bias": mean(speed_errors),
            "speed_mae": mean([abs(value) for value in speed_errors]),
            "speed_rmse": rmse(speed_errors),
            "dir_mae_deg": mean(direction_errors),
            "vector_mae": mean(vector_errors),
            "vector_rmse": rmse(vector_errors),
        }

    wn = metrics("wn")
    wx = metrics("wx")
    return {
        "sample_count": len(sample_rows),
        "windninja": wn,
        "hrrr": wx,
        "parent_model": wx,
        "improvement": {
            "speed_mae": None if wn["speed_mae"] is None or wx["speed_mae"] is None else wx["speed_mae"] - wn["speed_mae"],
            "speed_rmse": None if wn["speed_rmse"] is None or wx["speed_rmse"] is None else wx["speed_rmse"] - wn["speed_rmse"],
            "dir_mae_deg": None if wn["dir_mae_deg"] is None or wx["dir_mae_deg"] is None else wx["dir_mae_deg"] - wn["dir_mae_deg"],
            "vector_mae": None if wn["vector_mae"] is None or wx["vector_mae"] is None else wx["vector_mae"] - wn["vector_mae"],
            "vector_rmse": None if wn["vector_rmse"] is None or wx["vector_rmse"] is None else wx["vector_rmse"] - wn["vector_rmse"],
        },
    }


def rows_to_csv(path: Path, rows: list[dict]) -> None:
    ensure_parent(path)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def compare_against_synoptic(args) -> int:
    token = get_synoptic_token(args.token)
    points_output = resolve_repo_path(args.points_output)
    metadata_file = resolve_repo_path(args.metadata_file)
    samples_csv = resolve_repo_path(args.samples_csv)
    station_summary_csv = resolve_repo_path(args.station_summary_csv)
    group_summary_csv = resolve_repo_path(args.group_summary_csv)
    summary_json = resolve_repo_path(args.summary_json)

    metadata = load_json(metadata_file)
    station_records = metadata["stations"]
    station_by_id = {record["station_id"]: record for record in station_records}
    model_rows = load_model_points(points_output)

    start_time = parse_utc_timestamp(args.start)
    end_time = parse_utc_timestamp(args.end)
    if start_time >= end_time:
        raise ValueError("--end must be later than --start.")
    observations_payload = fetch_synoptic_observations(
        list(station_by_id),
        start_time,
        end_time,
        args.tolerance_minutes,
        token,
        args.speed_units,
    )

    sample_rows = []
    per_station_samples: dict[str, list[dict]] = {}
    per_group_samples: dict[str, list[dict]] = {}

    for model_row in model_rows:
        station_id = model_row["station_id"]
        station_meta = station_by_id.get(station_id)
        if not station_meta:
            continue
        station_payload = observations_payload.get(station_id)
        if not station_payload:
            continue
        station_obs_rows = extract_station_observations(
            station_payload,
            target_height_m=station_meta["height_m"],
        )
        obs_row = nearest_observation(
            station_obs_rows,
            model_row["datetime"],
            args.tolerance_minutes,
        )
        if not obs_row:
            continue

        wn_speed, wn_dir = model_speed_direction(model_row["u"], model_row["v"])
        wx_speed, wx_dir = model_speed_direction(model_row["wx_u"], model_row["wx_v"])
        wn_vector_error = math.hypot(model_row["u"] - obs_row["u_obs"], model_row["v"] - obs_row["v_obs"])
        wx_vector_error = math.hypot(model_row["wx_u"] - obs_row["u_obs"], model_row["wx_v"] - obs_row["v_obs"])

        sample = {
            "station_id": station_id,
            "station_label": station_meta["label"],
            "group": station_meta["group"],
            "sample_time_utc": isoformat_utc(model_row["datetime"]),
            "obs_time_utc": isoformat_utc(obs_row["datetime"]),
            "obs_age_minutes": round(abs((model_row["datetime"] - obs_row["datetime"]).total_seconds()) / 60.0, 3),
            "height_m": station_meta["height_m"],
            "speed_obs": round(obs_row["speed_obs"], 6),
            "dir_obs_deg": round(obs_row["dir_obs_deg"], 6),
            "u_obs": round(obs_row["u_obs"], 6),
            "v_obs": round(obs_row["v_obs"], 6),
            "wn_speed": round(wn_speed, 6),
            "wn_dir_deg": round(wn_dir, 6),
            "wn_u": round(model_row["u"], 6),
            "wn_v": round(model_row["v"], 6),
            "wn_speed_error": round(wn_speed - obs_row["speed_obs"], 6),
            "wn_dir_abs_error_deg": round(circular_abs_error_deg(wn_dir, obs_row["dir_obs_deg"]), 6),
            "wn_vector_error": round(wn_vector_error, 6),
            "wx_speed": round(wx_speed, 6),
            "wx_dir_deg": round(wx_dir, 6),
            "wx_u": round(model_row["wx_u"], 6),
            "wx_v": round(model_row["wx_v"], 6),
            "wx_speed_error": round(wx_speed - obs_row["speed_obs"], 6),
            "wx_dir_abs_error_deg": round(circular_abs_error_deg(wx_dir, obs_row["dir_obs_deg"]), 6),
            "wx_vector_error": round(wx_vector_error, 6),
        }
        sample_rows.append(sample)
        per_station_samples.setdefault(station_id, []).append(sample)
        per_group_samples.setdefault(station_meta["group"], []).append(sample)

    if not sample_rows:
        raise ValueError("No matched station/model samples were found for the requested time window.")

    station_summary_rows = []
    for station_id, rows in sorted(per_station_samples.items()):
        summary = summarize_samples(rows)
        station_meta = station_by_id[station_id]
        station_summary_rows.append({
            "station_id": station_id,
            "station_label": station_meta["label"],
            "group": station_meta["group"],
            "height_m": station_meta["height_m"],
            "sample_count": summary["sample_count"],
            "wn_speed_mae": summary["windninja"]["speed_mae"],
            "wx_speed_mae": summary["hrrr"]["speed_mae"],
            "wn_speed_rmse": summary["windninja"]["speed_rmse"],
            "wx_speed_rmse": summary["hrrr"]["speed_rmse"],
            "wn_dir_mae_deg": summary["windninja"]["dir_mae_deg"],
            "wx_dir_mae_deg": summary["hrrr"]["dir_mae_deg"],
            "wn_vector_rmse": summary["windninja"]["vector_rmse"],
            "wx_vector_rmse": summary["hrrr"]["vector_rmse"],
            "vector_rmse_improvement": summary["improvement"]["vector_rmse"],
        })

    group_summary_rows = []
    for group_name, rows in sorted(per_group_samples.items()):
        summary = summarize_samples(rows)
        group_summary_rows.append({
            "group": group_name,
            "sample_count": summary["sample_count"],
            "wn_speed_mae": summary["windninja"]["speed_mae"],
            "wx_speed_mae": summary["hrrr"]["speed_mae"],
            "wn_speed_rmse": summary["windninja"]["speed_rmse"],
            "wx_speed_rmse": summary["hrrr"]["speed_rmse"],
            "wn_dir_mae_deg": summary["windninja"]["dir_mae_deg"],
            "wx_dir_mae_deg": summary["hrrr"]["dir_mae_deg"],
            "wn_vector_rmse": summary["windninja"]["vector_rmse"],
            "wx_vector_rmse": summary["hrrr"]["vector_rmse"],
            "vector_rmse_improvement": summary["improvement"]["vector_rmse"],
        })

    overall_summary = summarize_samples(sample_rows)
    summary_payload = {
        "generated_at_utc": isoformat_utc(dt.datetime.now(UTC)),
        "points_output": str(points_output),
        "metadata_file": str(metadata_file),
        "start_utc": isoformat_utc(start_time),
        "end_utc": isoformat_utc(end_time),
        "tolerance_minutes": args.tolerance_minutes,
        "speed_units": args.speed_units,
        "matched_sample_count": len(sample_rows),
        "matched_station_count": len(per_station_samples),
        "overall": overall_summary,
    }

    rows_to_csv(samples_csv, sample_rows)
    rows_to_csv(station_summary_csv, station_summary_rows)
    rows_to_csv(group_summary_csv, group_summary_rows)
    write_json(summary_json, summary_payload)

    logger.info(f"Wrote matched samples CSV: {samples_csv}")
    logger.info(f"Wrote station summary CSV: {station_summary_csv}")
    logger.info(f"Wrote group summary CSV: {group_summary_csv}")
    logger.info(f"Wrote overall summary JSON: {summary_json}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare WindNinja point files and validate against Synoptic."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    prep = subparsers.add_parser(
        "prepare-points",
        help="Fetch Synoptic metadata and build a WindNinja points CSV.",
    )
    prep.add_argument("--station-file", required=True,
                      help="CSV manifest with station_id,label,group,height_m_override columns.")
    prep.add_argument("--points-output", required=True,
                      help="Output WindNinja points CSV path.")
    prep.add_argument("--metadata-output", required=True,
                      help="Output enriched station metadata JSON path.")
    prep.add_argument("--bbox-output",
                      help="Optional JSON file for the suggested padded bbox.")
    prep.add_argument("--padding-km", type=float, default=2.0,
                      help="Padding to add around the station extent when suggesting a bbox.")
    prep.add_argument("--default-height", type=float,
                      help="Fallback point height in meters if Synoptic metadata lacks one.")
    prep.add_argument("--start",
                      help="Optional UTC start for metadata obrange filtering.")
    prep.add_argument("--end",
                      help="Optional UTC end for metadata obrange filtering.")
    prep.add_argument("--token",
                      help="Synoptic API token. Defaults to MWN_SYNOPTIC_TOKEN or CUSTOM_API_KEY.")
    prep.set_defaults(func=prepare_points)

    compare = subparsers.add_parser(
        "compare",
        help="Compare WindNinja point output against Synoptic observations.",
    )
    compare.add_argument("--points-output", required=True,
                         help="WindNinja output_points_file CSV to validate.")
    compare.add_argument("--metadata-file", required=True,
                         help="Metadata JSON produced by prepare-points.")
    compare.add_argument("--start", required=True,
                         help="UTC validation window start.")
    compare.add_argument("--end", required=True,
                         help="UTC validation window end.")
    compare.add_argument("--samples-csv", required=True,
                         help="Output CSV with matched per-sample comparisons.")
    compare.add_argument("--station-summary-csv", required=True,
                         help="Output CSV with per-station summary metrics.")
    compare.add_argument("--group-summary-csv", required=True,
                         help="Output CSV with grouped summary metrics.")
    compare.add_argument("--summary-json", required=True,
                         help="Output JSON with overall summary metrics.")
    compare.add_argument("--tolerance-minutes", type=int, default=30,
                         help="Maximum allowed gap between model time and observed time.")
    compare.add_argument("--speed-units", choices=["mph", "mps", "kph", "kts"],
                         default="mph",
                         help="Units used by WindNinja output_points_file.")
    compare.add_argument("--token",
                         help="Synoptic API token. Defaults to MWN_SYNOPTIC_TOKEN or CUSTOM_API_KEY.")
    compare.set_defaults(func=compare_against_synoptic)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
