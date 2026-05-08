#!/usr/bin/env python3
"""Point-level HRRR exposure-gate test for the 10 m to 80 m blend."""
from __future__ import annotations

import csv
import datetime as dt
import html
import json
import math
import subprocess
from dataclasses import dataclass
from pathlib import Path

try:
    from . import k0co_hrrr_point_tuning as tuning
    from . import synoptic_validation as sv
    from .wind_math import convert_speed
except ImportError:
    import k0co_hrrr_point_tuning as tuning
    import synoptic_validation as sv
    from wind_math import convert_speed


UTC = dt.timezone.utc
OUT_DIR = Path("runtime/validation/hrrr_exposure_gate")
K0CO_ROOT = Path("runtime/validation/berthoud_pass_k0co_height_hrrr")
SUMMIT_ROOT = Path("runtime/validation/summit_caic_hrrr_adjusted")


@dataclass(frozen=True)
class StationInput:
    station_id: str
    label: str
    lon: float
    lat: float
    fields_csv: Path
    gmted_grid: Path
    station_filter: str | None = None


STATIONS = [
    StationInput(
        "K0CO",
        "Berthoud Pass / Mines Peak AWOS",
        -105.76393,
        39.79453,
        K0CO_ROOT / "tuning" / "point_fields_202601010000_202604010000.csv",
        K0CO_ROOT / "gmted_500m" / "elevation.asc",
    ),
    StationInput(
        "CABP8",
        "Breckenridge Ski Area Peak 8",
        -106.10255,
        39.47269,
        SUMMIT_ROOT / "summit_caic_hrrr_fields_cache.csv",
        SUMMIT_ROOT / "gmted_500m" / "summit_caic_gmted_500m.tif",
        station_filter="CABP8",
    ),
    StationInput(
        "CAHSB",
        "Breckenridge Ski Area Horseshoe",
        -106.09150,
        39.47532,
        SUMMIT_ROOT / "summit_caic_hrrr_fields_cache.csv",
        SUMMIT_ROOT / "gmted_500m" / "summit_caic_gmted_500m.tif",
        station_filter="CAHSB",
    ),
]


def lonlat_to_utm13n(lon_deg: float, lat_deg: float) -> tuple[float, float]:
    """Convert WGS84 lon/lat to UTM 13N meters without adding a runtime dependency."""
    semi_major = 6378137.0
    flattening = 1.0 / 298.257223563
    scale = 0.9996
    eccentricity_sq = flattening * (2.0 - flattening)
    second_ecc_sq = eccentricity_sq / (1.0 - eccentricity_sq)
    lat = math.radians(lat_deg)
    lon = math.radians(lon_deg)
    central_lon = math.radians(-105.0)
    sin_lat = math.sin(lat)
    cos_lat = math.cos(lat)
    n = semi_major / math.sqrt(1.0 - eccentricity_sq * sin_lat * sin_lat)
    tan_sq = math.tan(lat) ** 2
    c = second_ecc_sq * cos_lat * cos_lat
    a = cos_lat * (lon - central_lon)
    meridian = semi_major * (
        (1.0 - eccentricity_sq / 4.0 - 3.0 * eccentricity_sq**2 / 64.0 - 5.0 * eccentricity_sq**3 / 256.0) * lat
        - (3.0 * eccentricity_sq / 8.0 + 3.0 * eccentricity_sq**2 / 32.0 + 45.0 * eccentricity_sq**3 / 1024.0)
        * math.sin(2.0 * lat)
        + (15.0 * eccentricity_sq**2 / 256.0 + 45.0 * eccentricity_sq**3 / 1024.0) * math.sin(4.0 * lat)
        - (35.0 * eccentricity_sq**3 / 3072.0) * math.sin(6.0 * lat)
    )
    easting = scale * n * (
        a
        + (1.0 - tan_sq + c) * a**3 / 6.0
        + (5.0 - 18.0 * tan_sq + tan_sq**2 + 72.0 * c - 58.0 * second_ecc_sq) * a**5 / 120.0
    ) + 500000.0
    northing = scale * (
        meridian
        + n
        * math.tan(lat)
        * (
            a**2 / 2.0
            + (5.0 - tan_sq + 9.0 * c + 4.0 * c**2) * a**4 / 24.0
            + (61.0 - 58.0 * tan_sq + tan_sq**2 + 600.0 * c - 330.0 * second_ecc_sq) * a**6 / 720.0
        )
    )
    return easting, northing


def load_xyz(path: Path) -> list[tuple[float, float, float]]:
    result = subprocess.run(
        ["gdal_translate", "-q", "-of", "XYZ", str(path), "/vsistdout/"],
        text=True,
        capture_output=True,
        check=True,
    )
    points = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        x, y, z = map(float, parts[:3])
        if z > -9990.0:
            points.append((x, y, z))
    return points


def terrain_exposure(
    station: StationInput,
    *,
    radius_m: float = 3000.0,
    inner_skip_m: float = 500.0,
    full_exposure_tpi_m: float = 250.0,
) -> dict:
    x0, y0 = lonlat_to_utm13n(station.lon, station.lat)
    points = load_xyz(station.gmted_grid)
    center = min(points, key=lambda point: math.hypot(point[0] - x0, point[1] - y0))[2]
    surrounding = [
        z
        for x, y, z in points
        if inner_skip_m < math.hypot(x - x0, y - y0) <= radius_m
    ]
    if not surrounding:
        raise RuntimeError(f"No surrounding GMTED cells for {station.station_id}")
    surrounding_mean = sv.mean(surrounding)
    tpi = center - surrounding_mean
    exposure_weight = max(0.0, min(tpi / full_exposure_tpi_m, 1.0))
    return {
        "gmted_elevation_m": center,
        "surrounding_mean_m": surrounding_mean,
        "tpi_m": tpi,
        "exposure_weight": exposure_weight,
        "radius_m": radius_m,
        "full_exposure_tpi_m": full_exposure_tpi_m,
        "surrounding_cell_count": len(surrounding),
    }


def load_fields(station: StationInput) -> list[dict]:
    if not station.fields_csv.exists():
        raise FileNotFoundError(station.fields_csv)
    rows = []
    with station.fields_csv.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if station.station_filter and row.get("station_id") != station.station_filter:
                continue
            rows.append(row)
    return rows


def evaluate_weight(record: dict, setting_name: str, weight: float) -> dict:
    obs_speed = float(record["obs_speed"])
    obs_dir = float(record["obs_dir_deg"])
    obs_u, obs_v = tuning.obs_to_uv(obs_speed, obs_dir)
    u10 = float(record["u10"])
    v10 = float(record["v10"])
    u80 = float(record["u80"])
    v80 = float(record["v80"])
    u = (1.0 - weight) * u10 + weight * u80
    v = (1.0 - weight) * v10 + weight * v80
    speed_mps, direction = tuning.speed_dir_from_uv(u, v)
    u_mph = convert_speed(u, "mps", "mph")
    v_mph = convert_speed(v, "mps", "mph")
    speed_mph = convert_speed(speed_mps, "mps", "mph")
    return {
        "sample_time_utc": record["sample_time_utc"],
        "setting": setting_name,
        "speed_mph": speed_mph,
        "direction_deg": direction,
        "speed_error_mph": speed_mph - obs_speed,
        "dir_abs_error_deg": tuning.circular_abs_error(direction, obs_dir),
        "vector_error_mph": math.hypot(u_mph - obs_u, v_mph - obs_v),
        "weight": weight,
    }


def summarize(station_id: str, setting_name: str, values: list[dict], baseline: dict | None) -> dict:
    speed_errors = [row["speed_error_mph"] for row in values]
    vector_errors = [row["vector_error_mph"] for row in values]
    speed_mae = sv.mean([abs(value) for value in speed_errors])
    vector_rmse = sv.rmse(vector_errors)
    return {
        "station_id": station_id,
        "setting": setting_name,
        "sample_count": len(values),
        "speed_mae_mph": speed_mae,
        "speed_bias_mph": sv.mean(speed_errors),
        "direction_mae_deg": sv.mean([row["dir_abs_error_deg"] for row in values]),
        "vector_rmse_mph": vector_rmse,
        "mean_weight": sv.mean([row["weight"] for row in values]),
        "speed_mae_improvement_mph": None if baseline is None else baseline["speed_mae_mph"] - speed_mae,
        "vector_rmse_improvement_mph": None if baseline is None else baseline["vector_rmse_mph"] - vector_rmse,
    }


def evaluate_station(station: StationInput) -> tuple[list[dict], list[dict], dict]:
    exposure = terrain_exposure(station)
    records = load_fields(station)
    grouped = {"HRRR_10m": [], "HRRR_80m": [], "balanced_300m_10_80_cap": [], "tpi_exposure_gate_300m": []}
    for record in records:
        gmted_elevation = float(record.get("gmted_elevation_m") or exposure["gmted_elevation_m"])
        elevation_delta = gmted_elevation - float(record["hrrr_surface_hgt_m"])
        balanced_weight = max(0.0, min(elevation_delta / 300.0, 1.0))
        gated_weight = balanced_weight * exposure["exposure_weight"]
        grouped["HRRR_10m"].append(evaluate_weight(record, "HRRR_10m", 0.0))
        grouped["HRRR_80m"].append(evaluate_weight(record, "HRRR_80m", 1.0))
        grouped["balanced_300m_10_80_cap"].append(evaluate_weight(record, "balanced_300m_10_80_cap", balanced_weight))
        grouped["tpi_exposure_gate_300m"].append(evaluate_weight(record, "tpi_exposure_gate_300m", gated_weight))
    baseline = summarize(station.station_id, "HRRR_10m", grouped["HRRR_10m"], None)
    metrics = [baseline]
    samples = []
    for setting, values in grouped.items():
        if setting != "HRRR_10m":
            metrics.append(summarize(station.station_id, setting, values, baseline))
        for row in values:
            samples.append({"station_id": station.station_id, **row})
    exposure = {"station_id": station.station_id, "label": station.label, **exposure}
    return metrics, samples, exposure


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                key: round(value, 6) if isinstance(value, float) else value
                for key, value in row.items()
            })


def format_value(value: object, digits: int = 2) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, str):
        return html.escape(value)
    return f"{float(value):.{digits}f}"


def write_html(path: Path, metrics: list[dict], exposures: list[dict]) -> None:
    by_station: dict[str, list[dict]] = {}
    exposure_by_station = {row["station_id"]: row for row in exposures}
    for row in metrics:
        by_station.setdefault(row["station_id"], []).append(row)
    sections = []
    fields = [
        "setting",
        "sample_count",
        "speed_mae_mph",
        "speed_bias_mph",
        "direction_mae_deg",
        "vector_rmse_mph",
        "mean_weight",
        "speed_mae_improvement_mph",
    ]
    for station_id, rows in by_station.items():
        exposure = exposure_by_station[station_id]
        body = []
        for row in rows:
            body.append(
                "<tr>"
                + "".join(
                    f"<td>{html.escape(str(row[field]))}</td>"
                    if field in {"setting", "sample_count"}
                    else f"<td>{format_value(row[field])}</td>"
                    for field in fields
                )
                + "</tr>"
            )
        sections.append(
            f"""
<h2>{html.escape(station_id)} / {html.escape(exposure["label"])}</h2>
<p class="note">
  GMTED {float(exposure["gmted_elevation_m"]):.1f} m; 3 km TPI
  {float(exposure["tpi_m"]):.1f} m; exposure gate weight
  {float(exposure["exposure_weight"]):.2f}.
</p>
<table>
  <thead><tr>{''.join(f'<th>{html.escape(field)}</th>' for field in fields)}</tr></thead>
  <tbody>{''.join(body)}</tbody>
</table>
"""
        )
    path.write_text(
        f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>HRRR Exposure Gate Assessment</title>
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
  <h1>HRRR Exposure Gate Assessment</h1>
  <p class="note">
    This is HRRR-only point validation. The exposure variant keeps the existing
    300 m HRRR 10 m to 80 m blend, then multiplies that weight by a simple
    3 km terrain-position gate from the 500 m GMTED grid.
  </p>
  {''.join(sections)}
</div>
</body>
</html>
""",
        encoding="utf-8",
    )


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    metrics: list[dict] = []
    samples: list[dict] = []
    exposures: list[dict] = []
    for station in STATIONS:
        station_metrics, station_samples, station_exposure = evaluate_station(station)
        metrics.extend(station_metrics)
        samples.extend(station_samples)
        exposures.append(station_exposure)
    summary = {
        "generated_at_utc": dt.datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "method": "final_weight = clamp((GMTED - HRRR_HGT) / 300m, 0, 1) * clamp(TPI_3km / 250m, 0, 1)",
        "metrics": metrics,
        "exposures": exposures,
    }
    metrics_csv = OUT_DIR / "exposure_gate_metrics.csv"
    samples_csv = OUT_DIR / "exposure_gate_samples.csv"
    exposures_csv = OUT_DIR / "exposure_gate_terrain.csv"
    summary_json = OUT_DIR / "exposure_gate_summary.json"
    html_path = OUT_DIR / "exposure_gate.html"
    write_csv(metrics_csv, metrics)
    write_csv(samples_csv, samples)
    write_csv(exposures_csv, exposures)
    summary_json.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    write_html(html_path, metrics, exposures)
    print(json.dumps({
        "metrics_csv": str(metrics_csv),
        "exposures_csv": str(exposures_csv),
        "summary_json": str(summary_json),
        "html": str(html_path),
        "metrics": metrics,
        "exposures": exposures,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
