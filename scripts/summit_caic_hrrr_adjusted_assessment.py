#!/usr/bin/env python3
"""HRRR-only assessment for Summit County CAIC ridge stations."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import json
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    from . import k0co_height_hrrr_validation as kh
    from . import k0co_hrrr_point_tuning as tuning
    from . import synoptic_validation as sv
    from . import validation_study as vs
except ImportError:
    import k0co_height_hrrr_validation as kh
    import k0co_hrrr_point_tuning as tuning
    import synoptic_validation as sv
    import validation_study as vs


UTC = dt.timezone.utc
DEFAULT_START = "202601010000"
DEFAULT_END = "202604010000"
DEFAULT_ROOT = Path("runtime/validation/summit_caic_hrrr_adjusted")
DEFAULT_GMTED_GRID = DEFAULT_ROOT / "gmted_500m" / "summit_caic_gmted_500m.tif"
STATIONS = [
    {
        "station_id": "CABP8",
        "provider": "synoptic",
        "label": "Breckenridge Ski Area Peak 8",
        "group": "ridge",
        "name": "BRECKENRIDGE SKI AREA PEAK 8",
        "latitude": 39.47269,
        "longitude": -106.10255,
        "elevation_ft": 12998.0,
        "alos_dem_ft": 11902.9,
        "height_m": 5.0,
        "height_source": "assessment_assumption",
        "network_name": "Colorado Avalanche Information Center",
    },
    {
        "station_id": "CAHSB",
        "provider": "synoptic",
        "label": "Breckenridge Ski Area Horseshoe",
        "group": "ridge",
        "name": "BRECKENRIDGE SKI AREA HORSESHOE",
        "latitude": 39.47532,
        "longitude": -106.09150,
        "elevation_ft": 11900.0,
        "alos_dem_ft": None,
        "height_m": 5.0,
        "height_source": "assessment_assumption",
        "network_name": "Colorado Avalanche Information Center",
    },
]
SETTING_NAMES = [
    "HRRR_10m",
    "HRRR_80m",
    "blend_scale_300m_low_0.75_high_1.35",
    "blend_scale_300m_cap_10_80_low_0.75_high_1.10",
    "blend_scale_450m_no_cap",
]


def selected_settings() -> list[tuning.Setting]:
    by_name = {setting.name: setting for setting in tuning.settings()}
    return [by_name[name] for name in SETTING_NAMES]


def metric_row(station_id: str, setting_name: str, rows: list[dict]) -> dict:
    speed_errors = [float(row["speed_error_mph"]) for row in rows]
    direction_errors = [float(row["dir_abs_error_deg"]) for row in rows]
    vector_errors = [float(row["vector_error_mph"]) for row in rows]
    weights = [float(row["weight"]) for row in rows]
    return {
        "station_id": station_id,
        "setting": setting_name,
        "sample_count": len(rows),
        "speed_bias_mph": sv.mean(speed_errors),
        "speed_mae_mph": sv.mean([abs(value) for value in speed_errors]),
        "speed_rmse_mph": sv.rmse(speed_errors),
        "direction_mae_deg": sv.mean(direction_errors),
        "vector_rmse_mph": sv.rmse(vector_errors),
        "cap_low_fraction": sum(1 for row in rows if row["cap"] == "low") / len(rows),
        "cap_high_fraction": sum(1 for row in rows if row["cap"] == "high") / len(rows),
        "mean_weight": sv.mean(weights),
    }


def write_csv(path: Path, rows: list[dict], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    names = fieldnames or list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=names)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                name: round(value, 6) if isinstance((value := row.get(name)), float) else value
                for name in names
            })


def load_existing_fields(paths: list[Path]) -> dict[tuple[str, str], dict]:
    rows = {}
    for path in paths:
        if not path.exists():
            continue
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                rows[(row["station_id"], row["sample_time_utc"])] = row
    return rows


def append_field_rows(path: Path, rows: list[dict]) -> None:
    exists = path.exists() and path.stat().st_size > 0
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames())
        if not exists:
            writer.writeheader()
        writer.writerows(rows)


def fieldnames() -> list[str]:
    return [
        "station_id",
        "sample_time_utc",
        "obs_time_utc",
        "obs_speed",
        "obs_dir_deg",
        "u10",
        "v10",
        "u80",
        "v80",
        "hrrr_surface_hgt_m",
        "gmted_elevation_m",
        "reported_elevation_ft",
        "alos_dem_ft",
    ]


def sample_hour(
    run_time: dt.datetime,
    observation_by_station: dict[str, dict],
    stations_by_id: dict[str, dict],
    *,
    cache_root: Path,
    archive_base_url: str,
    gmted_elevation_by_station: dict[str, float],
) -> list[dict]:
    hour_dir = cache_root / run_time.strftime("%Y%m%d%H%M")
    try:
        grib_path = kh.download_hrrr_subset(run_time, hour_dir / "grib", archive_base_url)
        output = []
        for station_id, obs in observation_by_station.items():
            station = stations_by_id[station_id]
            lon = station["longitude"]
            lat = station["latitude"]
            output.append({
                "station_id": station_id,
                "sample_time_utc": sv.isoformat_utc(run_time),
                "obs_time_utc": sv.isoformat_utc(obs["datetime"]),
                "obs_speed": obs["speed_obs"],
                "obs_dir_deg": obs["dir_obs_deg"],
                "u10": tuning.sample_band(grib_path, 1, lon, lat),
                "v10": tuning.sample_band(grib_path, 2, lon, lat),
                "u80": tuning.sample_band(grib_path, 3, lon, lat),
                "v80": tuning.sample_band(grib_path, 4, lon, lat),
                "hrrr_surface_hgt_m": tuning.sample_band(grib_path, 5, lon, lat),
                "gmted_elevation_m": gmted_elevation_by_station[station_id],
                "reported_elevation_ft": station["elevation_ft"],
                "alos_dem_ft": station.get("alos_dem_ft"),
            })
        return output
    finally:
        shutil.rmtree(hour_dir, ignore_errors=True)


def hourly_observations(
    stations: list[dict],
    start: dt.datetime,
    end: dt.datetime,
    tolerance_minutes: int,
    token: str | None,
) -> dict[dt.datetime, dict[str, dict]]:
    observations = sv.fetch_observations(stations, start, end, tolerance_minutes, token, "mph")
    by_hour = {}
    cursor = start
    while cursor < end:
        station_obs = {}
        for station in stations:
            obs = sv.nearest_observation(
                observations.get(station["station_id"], []),
                cursor,
                tolerance_minutes,
            )
            if obs is not None:
                station_obs[station["station_id"]] = obs
        if station_obs:
            by_hour[cursor] = station_obs
        cursor += dt.timedelta(hours=1)
    return by_hour


def evaluate_fields(records: list[dict]) -> tuple[list[dict], list[dict]]:
    settings = selected_settings()
    samples = []
    for record in records:
        gmted_elevation = float(record["gmted_elevation_m"])
        for setting in settings:
            result = tuning.evaluate_setting(record, setting, gmted_elevation)
            samples.append({
                "station_id": record["station_id"],
                "sample_time_utc": record["sample_time_utc"],
                "obs_time_utc": record["obs_time_utc"],
                "setting": setting.name,
                "observed_speed": float(record["obs_speed"]),
                "observed_dir_deg": float(record["obs_dir_deg"]),
                "speed_mph": result["speed_mph"],
                "direction_deg": result["direction_deg"],
                "speed_error_mph": result["speed_error_mph"],
                "dir_abs_error_deg": result["dir_abs_error_deg"],
                "vector_error_mph": result["vector_error_mph"],
                "weight": result["weight"],
                "cap": result["cap"],
            })

    metrics = []
    station_ids = sorted({row["station_id"] for row in samples})
    for station_id in station_ids:
        for setting in settings:
            rows = [
                row
                for row in samples
                if row["station_id"] == station_id and row["setting"] == setting.name
            ]
            if rows:
                metrics.append(metric_row(station_id, setting.name, rows))
    return metrics, samples


def format_metric(value: object, digits: int = 2) -> str:
    if value in (None, ""):
        return ""
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return html.escape(str(value))


def write_html(path: Path, stations: list[dict], metrics: list[dict], summary: dict) -> None:
    by_station = {}
    for row in metrics:
        by_station.setdefault(row["station_id"], []).append(row)

    tables = []
    metric_fields = [
        "setting",
        "sample_count",
        "speed_bias_mph",
        "speed_mae_mph",
        "direction_mae_deg",
        "vector_rmse_mph",
        "cap_high_fraction",
        "mean_weight",
    ]
    station_by_id = {station["station_id"]: station for station in stations}
    for station_id, rows in sorted(by_station.items()):
        station = station_by_id[station_id]
        ordered = sorted(rows, key=lambda row: (row["speed_mae_mph"], row["vector_rmse_mph"]))
        body = "\n".join(
            "<tr>"
            + "".join(
                f"<td>{html.escape(str(row[field]))}</td>"
                if field in {"setting", "sample_count"}
                else f"<td>{format_metric(row[field])}</td>"
                for field in metric_fields
            )
            + "</tr>"
            for row in ordered
        )
        tables.append(
            f"""
<h2>{html.escape(station_id)} / {html.escape(station["label"])}</h2>
<p class="note">
  Lat/lon {station["latitude"]:.5f}, {station["longitude"]:.5f}; reported elevation
  {station["elevation_ft"]:.0f} ft; assumed wind sensor height {station["height_m"]:.1f} m.
</p>
<table>
  <thead><tr>{''.join(f'<th>{html.escape(field)}</th>' for field in metric_fields)}</tr></thead>
  <tbody>{body}</tbody>
</table>
"""
        )

    path.write_text(
        f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Summit CAIC HRRR Adjusted Assessment</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #111827; }}
    .wrap {{ max-width: 1180px; }}
    .note {{ color: #4b5563; line-height: 1.45; }}
    table {{ border-collapse: collapse; width: 100%; margin: 16px 0 28px; font-size: 13px; }}
    th, td {{ border: 1px solid #d1d5db; padding: 6px 8px; text-align: right; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ background: #f3f4f6; }}
  </style>
</head>
<body>
<div class="wrap">
  <h1>Summit CAIC HRRR Adjusted Assessment</h1>
  <p class="note">
    Period: {html.escape(summary["start_utc"])} through {html.escape(summary["end_utc"])}.
    This is HRRR-only point validation: no WindNinja run is included.
    Adjusted variants use station-sampled GMTED2010 elevation and HRRR 10 m/80 m U/V shear.
  </p>
  {''.join(tables)}
</div>
</body>
</html>
""",
        encoding="utf-8",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Assess HRRR adjusted forcing for Summit CAIC stations.")
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--gmted-grid", type=Path, default=DEFAULT_GMTED_GRID)
    parser.add_argument("--archive-base-url", default=kh.DEFAULT_ARCHIVE_BASE_URL)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--fields-cache", type=Path)
    parser.add_argument("--token")
    parser.add_argument("--tolerance-minutes", type=int, default=30)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    start = vs.parse_utc(args.start)
    end = vs.parse_utc(args.end)
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    gmted_grid = args.gmted_grid
    if not gmted_grid.exists():
        raise FileNotFoundError(
            f"Missing GMTED grid: {gmted_grid}. Fetch it with mwn.sh fetch-dem gmted first."
        )

    stations = STATIONS
    stations_by_id = {station["station_id"]: station for station in stations}
    gmted_elevation_by_station = {}
    for station in stations:
        value = kh.sample_dataset_value(gmted_grid, station["longitude"], station["latitude"])
        if value is None:
            raise RuntimeError(f"Could not sample GMTED grid for {station['station_id']}")
        gmted_elevation_by_station[station["station_id"]] = value

    stamp = f"{start:%Y%m%d%H%M}_{end:%Y%m%d%H%M}"
    fields_csv = out_dir / f"summit_caic_hrrr_fields_{stamp}.csv"
    samples_csv = out_dir / f"summit_caic_hrrr_adjusted_samples_{stamp}.csv"
    metrics_csv = out_dir / f"summit_caic_hrrr_adjusted_metrics_{stamp}.csv"
    summary_json = out_dir / f"summit_caic_hrrr_adjusted_summary_{stamp}.json"
    html_path = out_dir / f"summit_caic_hrrr_adjusted_{stamp}.html"
    fields_cache_csv = args.fields_cache or out_dir / "summit_caic_hrrr_fields_cache.csv"
    cache_root = out_dir / "raw_cache"
    cache_root.mkdir(parents=True, exist_ok=True)

    hourly_obs = hourly_observations(
        stations,
        start,
        end,
        args.tolerance_minutes,
        args.token,
    )
    existing = load_existing_fields([fields_cache_csv, *sorted(out_dir.glob("summit_caic_hrrr_fields_*.csv"))])
    missing_hours = [
        hour
        for hour, obs_by_station in hourly_obs.items()
        if any((station_id, sv.isoformat_utc(hour)) not in existing for station_id in obs_by_station)
    ]
    print(
        f"Loaded observations for {len(hourly_obs)} hours; "
        f"{len(missing_hours)} HRRR hours need sampling.",
        flush=True,
    )
    if missing_hours:
        started = time.monotonic()
        pending = []
        with ThreadPoolExecutor(max_workers=max(args.workers, 1)) as pool:
            futures = {
                pool.submit(
                    sample_hour,
                    hour,
                    hourly_obs[hour],
                    stations_by_id,
                    cache_root=cache_root,
                    archive_base_url=args.archive_base_url,
                    gmted_elevation_by_station=gmted_elevation_by_station,
                ): hour
                for hour in missing_hours
            }
            for completed, future in enumerate(as_completed(futures), start=1):
                rows = future.result()
                pending.extend(rows)
                if len(pending) >= 48:
                    append_field_rows(fields_cache_csv, pending)
                    existing.update({(row["station_id"], row["sample_time_utc"]): row for row in pending})
                    pending.clear()
                if completed == 1 or completed % 24 == 0 or completed == len(futures):
                    elapsed = time.monotonic() - started
                    rate = completed / elapsed if elapsed else 0.0
                    remaining = (len(futures) - completed) / rate if rate else 0.0
                    print(
                        f"Sampled HRRR hours {completed}/{len(futures)}; "
                        f"ETA {remaining / 60.0:.1f} min",
                        flush=True,
                    )
            if pending:
                append_field_rows(fields_cache_csv, pending)
                existing.update({(row["station_id"], row["sample_time_utc"]): row for row in pending})

    records = [
        existing[(station_id, sv.isoformat_utc(hour))]
        for hour, obs_by_station in sorted(hourly_obs.items())
        for station_id in sorted(obs_by_station)
        if (station_id, sv.isoformat_utc(hour)) in existing
    ]
    cache_records = sorted(existing.values(), key=lambda row: (row["sample_time_utc"], row["station_id"]))
    write_csv(fields_cache_csv, cache_records, fieldnames())
    write_csv(fields_csv, records, fieldnames())
    metrics, samples = evaluate_fields(records)
    write_csv(samples_csv, samples)
    write_csv(metrics_csv, metrics)
    summary = {
        "generated_at_utc": sv.isoformat_utc(dt.datetime.now(UTC)),
        "start_utc": sv.isoformat_utc(start),
        "end_utc": sv.isoformat_utc(end),
        "stations": stations,
        "gmted_grid": str(gmted_grid),
        "gmted_elevation_by_station_m": gmted_elevation_by_station,
        "sample_count": len(records),
        "settings": SETTING_NAMES,
        "outputs": {
            "fields_csv": str(fields_csv),
            "fields_cache_csv": str(fields_cache_csv),
            "samples_csv": str(samples_csv),
            "metrics_csv": str(metrics_csv),
            "summary_json": str(summary_json),
            "html": str(html_path),
        },
        "metrics": metrics,
    }
    summary_json.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    write_html(html_path, stations, metrics, summary)
    shutil.rmtree(cache_root, ignore_errors=True)
    print(json.dumps(summary, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
