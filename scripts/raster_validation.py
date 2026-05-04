#!/usr/bin/env python3
"""Validate WindNinja and parent-model rasters against Synoptic stations."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import subprocess
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))

import synoptic_validation as sv


UTC = dt.timezone.utc

RUN_LABEL_RE = r"(?:\d{2}-\d{2}-\d{4}|\d{8})_\d{4}"
WINDNINJA_RASTER_RE = re.compile(
    rf"^(?P<domain>.+)_(?P<label>{RUN_LABEL_RE})(?:_(?P<resolution>\d+)m)?_"
    r"(?P<kind>vel|ang)\.asc$"
)
WX_MODEL_RASTER_RE = re.compile(
    rf"^(?P<model>.+?)[-_](?P<label>{RUN_LABEL_RE})_(?P<kind>vel|ang)\.asc$"
)


def parse_run_label(label: str) -> dt.datetime:
    for fmt in ("%m-%d-%Y_%H%M", "%Y%m%d_%H%M"):
        try:
            return dt.datetime.strptime(label, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    raise ValueError(f"Unsupported raster timestamp: {label}")


def sample_raster_value(path: Path, lon: float, lat: float) -> float | None:
    result = subprocess.run(
        [
            "gdallocationinfo",
            "-wgs84",
            "-valonly",
            str(path),
            str(lon),
            str(lat),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"gdallocationinfo failed for {path}: {result.stderr.strip() or result.stdout.strip()}"
        )
    value = result.stdout.strip()
    if not value:
        return None
    sampled = float(value)
    if sampled <= -9990:
        return None
    return sampled


def collect_raster_sets(run_dir: Path) -> dict[dt.datetime, dict[str, Path]]:
    raster_sets: dict[dt.datetime, dict[str, Path]] = {}

    for path in sorted(run_dir.glob("*.asc")):
        windninja_match = WINDNINJA_RASTER_RE.match(path.name)
        if windninja_match:
            label = windninja_match.group("label")
            stamp = parse_run_label(label)
            entry = raster_sets.setdefault(stamp, {})
            entry[f"wn_{windninja_match.group('kind')}"] = path
            continue

        wx_match = WX_MODEL_RASTER_RE.match(path.name)
        if wx_match:
            label = wx_match.group("label")
            stamp = parse_run_label(label)
            entry = raster_sets.setdefault(stamp, {})
            entry[f"wx_{wx_match.group('kind')}"] = path

    return {
        stamp: paths
        for stamp, paths in raster_sets.items()
        if {"wn_vel", "wn_ang", "wx_vel", "wx_ang"} <= set(paths)
    }


def load_metadata(path: Path) -> list[dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload["stations"]


def build_sample_rows(station_records: list[dict],
                      raster_sets: dict[dt.datetime, dict[str, Path]],
                      observations_by_station: dict[str, list[dict]],
                      tolerance_minutes: int) -> list[dict]:
    sample_rows = []
    station_by_id = {record["station_id"]: record for record in station_records}

    for station_id, station_meta in sorted(station_by_id.items()):
        station_obs_rows = observations_by_station.get(station_id) or []
        if not station_obs_rows:
            continue

        lon = station_meta["longitude"]
        lat = station_meta["latitude"]
        for stamp, paths in sorted(raster_sets.items()):
            obs_row = sv.nearest_observation(
                station_obs_rows,
                stamp,
                tolerance_minutes,
            )
            if not obs_row:
                continue

            wn_speed = sample_raster_value(paths["wn_vel"], lon, lat)
            wn_dir = sample_raster_value(paths["wn_ang"], lon, lat)
            wx_speed = sample_raster_value(paths["wx_vel"], lon, lat)
            wx_dir = sample_raster_value(paths["wx_ang"], lon, lat)
            if None in {wn_speed, wn_dir, wx_speed, wx_dir}:
                continue

            wn_u, wn_v = sv.obs_to_uv(wn_speed, wn_dir)
            wx_u, wx_v = sv.obs_to_uv(wx_speed, wx_dir)
            wn_vector_error = ((wn_u - obs_row["u_obs"]) ** 2 + (wn_v - obs_row["v_obs"]) ** 2) ** 0.5
            wx_vector_error = ((wx_u - obs_row["u_obs"]) ** 2 + (wx_v - obs_row["v_obs"]) ** 2) ** 0.5

            sample_rows.append({
                "station_id": station_id,
                "station_label": station_meta["label"],
                "group": station_meta["group"],
                "sample_time_utc": sv.isoformat_utc(stamp),
                "obs_time_utc": sv.isoformat_utc(obs_row["datetime"]),
                "obs_age_minutes": round(abs((stamp - obs_row["datetime"]).total_seconds()) / 60.0, 3),
                "height_m": station_meta["height_m"],
                "speed_obs": round(obs_row["speed_obs"], 6),
                "dir_obs_deg": round(obs_row["dir_obs_deg"], 6),
                "u_obs": round(obs_row["u_obs"], 6),
                "v_obs": round(obs_row["v_obs"], 6),
                "wn_speed": round(wn_speed, 6),
                "wn_dir_deg": round(wn_dir, 6),
                "wn_u": round(wn_u, 6),
                "wn_v": round(wn_v, 6),
                "wn_speed_error": round(wn_speed - obs_row["speed_obs"], 6),
                "wn_dir_abs_error_deg": round(sv.circular_abs_error_deg(wn_dir, obs_row["dir_obs_deg"]), 6),
                "wn_vector_error": round(wn_vector_error, 6),
                "wx_speed": round(wx_speed, 6),
                "wx_dir_deg": round(wx_dir, 6),
                "wx_u": round(wx_u, 6),
                "wx_v": round(wx_v, 6),
                "wx_speed_error": round(wx_speed - obs_row["speed_obs"], 6),
                "wx_dir_abs_error_deg": round(sv.circular_abs_error_deg(wx_dir, obs_row["dir_obs_deg"]), 6),
                "wx_vector_error": round(wx_vector_error, 6),
                "wn_vel_path": str(paths["wn_vel"]),
                "wx_vel_path": str(paths["wx_vel"]),
            })

    return sample_rows


def summary_rows(sample_rows: list[dict], key: str) -> list[dict]:
    grouped: dict[str, list[dict]] = {}
    for row in sample_rows:
        grouped.setdefault(row[key], []).append(row)

    rows = []
    for group_key, rows_for_key in sorted(grouped.items()):
        summary = sv.summarize_samples(rows_for_key)
        out = {
            key: group_key,
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
        }
        if key == "station_id":
            sample = rows_for_key[0]
            out["station_label"] = sample["station_label"]
            out["group"] = sample["group"]
            out["height_m"] = sample["height_m"]
        rows.append(out)
    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare nearest WindNinja/parent-model raster cells against Synoptic observations."
    )
    parser.add_argument("--run-dir", required=True,
                        help="Run directory under runtime/temp containing *_vel.asc and *_ang.asc files.")
    parser.add_argument("--metadata-file", required=True,
                        help="Metadata JSON produced by synoptic-points.")
    parser.add_argument("--start", required=True,
                        help="UTC validation window start.")
    parser.add_argument("--end", required=True,
                        help="UTC validation window end.")
    parser.add_argument("--samples-csv", required=True,
                        help="Output CSV with matched per-sample comparisons.")
    parser.add_argument("--station-summary-csv", required=True,
                        help="Output CSV with per-station summary metrics.")
    parser.add_argument("--group-summary-csv", required=True,
                        help="Output CSV with grouped summary metrics.")
    parser.add_argument("--summary-json", required=True,
                        help="Output JSON with overall summary metrics.")
    parser.add_argument("--tolerance-minutes", type=int, default=30,
                        help="Maximum allowed gap between model time and observed time.")
    parser.add_argument("--speed-units", choices=["mph", "mps", "kph", "kts"],
                        default="mph",
                        help="Units used by the model raster outputs.")
    parser.add_argument("--token",
                        help="Synoptic API token. Defaults to MWN_SYNOPTIC_TOKEN or CUSTOM_API_KEY.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    run_dir = sv.resolve_repo_path(args.run_dir)
    metadata_file = sv.resolve_repo_path(args.metadata_file)
    samples_csv = sv.resolve_repo_path(args.samples_csv)
    station_summary_csv = sv.resolve_repo_path(args.station_summary_csv)
    group_summary_csv = sv.resolve_repo_path(args.group_summary_csv)
    summary_json = sv.resolve_repo_path(args.summary_json)

    start_time = sv.parse_utc_timestamp(args.start)
    end_time = sv.parse_utc_timestamp(args.end)
    if start_time >= end_time:
        raise ValueError("--end must be later than --start.")

    station_records = load_metadata(metadata_file)
    raster_sets = collect_raster_sets(run_dir)
    if not raster_sets:
        raise ValueError(f"No complete WindNinja/parent-model raster sets found in {run_dir}")

    observations_by_station = sv.fetch_observations(
        station_records,
        start_time,
        end_time,
        args.tolerance_minutes,
        args.token,
        args.speed_units,
    )

    sample_rows = build_sample_rows(
        station_records,
        raster_sets,
        observations_by_station,
        args.tolerance_minutes,
    )
    if not sample_rows:
        raise ValueError("No matched station/model samples were found for the requested time window.")

    station_rows = summary_rows(sample_rows, "station_id")
    group_rows = summary_rows(sample_rows, "group")
    overall_summary = sv.summarize_samples(sample_rows)

    sv.rows_to_csv(samples_csv, sample_rows)
    sv.rows_to_csv(station_summary_csv, station_rows)
    sv.rows_to_csv(group_summary_csv, group_rows)
    sv.write_json(summary_json, {
        "generated_at_utc": sv.isoformat_utc(dt.datetime.now(UTC)),
        "run_dir": str(run_dir),
        "metadata_file": str(metadata_file),
        "start_utc": sv.isoformat_utc(start_time),
        "end_utc": sv.isoformat_utc(end_time),
        "tolerance_minutes": args.tolerance_minutes,
        "speed_units": args.speed_units,
        "matched_sample_count": len(sample_rows),
        "matched_station_count": len({row["station_id"] for row in sample_rows}),
        "timestamp_count": len({row["sample_time_utc"] for row in sample_rows}),
        "overall": overall_summary,
    })

    print(f"Wrote matched samples CSV: {samples_csv}")
    print(f"Wrote station summary CSV: {station_summary_csv}")
    print(f"Wrote group summary CSV: {group_summary_csv}")
    print(f"Wrote overall summary JSON: {summary_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
