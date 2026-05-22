#!/usr/bin/env python3
"""CABTP observed vs raw HRRR and height-adjusted HRRR point assessment."""
from __future__ import annotations

import csv
import datetime as dt
import html
import json
import math
import os
import sys
from pathlib import Path

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import synoptic_validation as sv


UTC = dt.timezone.utc
START = dt.datetime(2026, 1, 1, tzinfo=UTC)
END = dt.datetime(2026, 4, 1, tzinfo=UTC)
STATION = {
    "station_id": "CABTP",
    "provider": "synoptic",
    "label": "Berthoud Pass CAIC",
    "group": "ridge",
    "name": "BERTHOUD PASS",
    "latitude": 39.80194,
    "longitude": -105.78389,
    "elevation_ft": 11860.0,
    "height_m": 5.0,
    "height_source": "assessment_assumption",
    "network_name": "Colorado Avalanche Information Center",
}
ROOT = Path("runtime/validation/berthoud_pass_k0co_height_hrrr")
OUT_DIR = ROOT / "cabtp_hrrr_adjusted"
RAW_RUN_ROOT = Path("runtime/temp")
ADJUSTED_FORCING_ROOT = ROOT / "forcing"


def utm13n(lon_deg: float, lat_deg: float) -> tuple[float, float]:
    """WGS84 lon/lat to UTM zone 13N, enough for local raster sampling."""
    a = 6378137.0
    f = 1 / 298.257223563
    k0 = 0.9996
    e2 = f * (2.0 - f)
    ep2 = e2 / (1.0 - e2)
    lat = math.radians(lat_deg)
    lon = math.radians(lon_deg)
    lon0 = math.radians(-105.0)
    sin_lat = math.sin(lat)
    cos_lat = math.cos(lat)
    tan_lat = math.tan(lat)
    n = a / math.sqrt(1.0 - e2 * sin_lat * sin_lat)
    t = tan_lat * tan_lat
    c = ep2 * cos_lat * cos_lat
    aa = cos_lat * (lon - lon0)
    m = a * (
        (1.0 - e2 / 4.0 - 3.0 * e2**2 / 64.0 - 5.0 * e2**3 / 256.0) * lat
        - (3.0 * e2 / 8.0 + 3.0 * e2**2 / 32.0 + 45.0 * e2**3 / 1024.0) * math.sin(2.0 * lat)
        + (15.0 * e2**2 / 256.0 + 45.0 * e2**3 / 1024.0) * math.sin(4.0 * lat)
        - (35.0 * e2**3 / 3072.0) * math.sin(6.0 * lat)
    )
    easting = k0 * n * (
        aa
        + (1.0 - t + c) * aa**3 / 6.0
        + (5.0 - 18.0 * t + t * t + 72.0 * c - 58.0 * ep2) * aa**5 / 120.0
    ) + 500000.0
    northing = k0 * (
        m
        + n
        * tan_lat
        * (
            aa**2 / 2.0
            + (5.0 - t + 9.0 * c + 4.0 * c * c) * aa**4 / 24.0
            + (61.0 - 58.0 * t + t * t + 600.0 * c - 330.0 * ep2) * aa**6 / 720.0
        )
    )
    return easting, northing


def sample_ascii_grid(path: Path, x: float, y: float) -> float | None:
    with path.open("r", encoding="utf-8") as handle:
        header = {}
        for _ in range(6):
            key, value = handle.readline().split()[:2]
            header[key.lower()] = float(value)
        ncols = int(header["ncols"])
        nrows = int(header["nrows"])
        cellsize = header["cellsize"]
        nodata = header.get("nodata_value", -9999.0)
        xll = header.get("xllcorner", header.get("xllcenter", 0.0) - cellsize / 2.0)
        yll = header.get("yllcorner", header.get("yllcenter", 0.0) - cellsize / 2.0)
        col = int((x - xll) / cellsize)
        row = int((yll + nrows * cellsize - y) / cellsize)
        if not (0 <= col < ncols and 0 <= row < nrows):
            return None
        for row_index, line in enumerate(handle):
            if row_index != row:
                continue
            parts = line.split()
            if col >= len(parts):
                return None
            value = float(parts[col])
            return None if math.isclose(value, nodata) else value
    return None


def raw_paths(hour: dt.datetime) -> tuple[Path, Path]:
    run_dir = RAW_RUN_ROOT / f"berthoud_pass_{hour:%Y%m%d}_0000_reanalysis_24h_HRRR"
    prefix = f"PASTCAST-GCP-HRRR-CONUS-3-KM-{hour:%m-%d-%Y}_{hour:%H%M}"
    return run_dir / f"{prefix}_vel.asc", run_dir / f"{prefix}_ang.asc"


def adjusted_paths(hour: dt.datetime) -> tuple[Path, Path]:
    hour_dir = ADJUSTED_FORCING_ROOT / f"{hour:%Y%m%d%H%M}" / "validation_hrrr_gmted_500m"
    return hour_dir / "speed_mph.asc", hour_dir / "direction.asc"


def obs_to_error_fields(prefix: str, speed: float, direction: float, obs: dict) -> dict:
    u, v = sv.obs_to_uv(speed, direction)
    return {
        f"{prefix}_speed": round(speed, 6),
        f"{prefix}_dir_deg": round(direction, 6),
        f"{prefix}_u": round(u, 6),
        f"{prefix}_v": round(v, 6),
        f"{prefix}_speed_error": round(speed - obs["speed_obs"], 6),
        f"{prefix}_dir_abs_error_deg": round(sv.circular_abs_error_deg(direction, obs["dir_obs_deg"]), 6),
        f"{prefix}_vector_error": round(math.hypot(u - obs["u_obs"], v - obs["v_obs"]), 6),
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


def rows_to_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def daily_means(rows: list[dict]) -> list[dict]:
    buckets: dict[dt.date, dict[str, list[float]]] = {}
    for row in rows:
        day = sv.parse_iso_time(row["sample_time_utc"]).date()
        bucket = buckets.setdefault(day, {"observed": [], "hrrr": [], "adjusted": []})
        bucket["observed"].append(row["observed_speed"])
        bucket["hrrr"].append(row["hrrr_speed"])
        bucket["adjusted"].append(row["adjusted_hrrr_speed"])
    return [
        {
            "date": day,
            "observed": sv.mean(values["observed"]),
            "hrrr": sv.mean(values["hrrr"]),
            "adjusted": sv.mean(values["adjusted"]),
        }
        for day, values in sorted(buckets.items())
    ]


def line_svg(rows: list[dict]) -> str:
    daily = daily_means(rows)
    width, height = 1120, 420
    left, right, top, bottom = 70, 24, 44, 62
    plot_width = width - left - right
    plot_height = height - top - bottom
    values = [value for day in daily for value in (day["observed"], day["hrrr"], day["adjusted"])]
    ymax = max(10.0, math.ceil(max(values) / 4.0) * 4.0)

    def x_for(index: int) -> float:
        return left + (plot_width / 2.0 if len(daily) == 1 else plot_width * index / (len(daily) - 1))

    def y_for(value: float) -> float:
        return top + (ymax - value) / ymax * plot_height

    def polyline(key: str, color: str) -> str:
        points = " ".join(f"{x_for(i):.1f},{y_for(day[key]):.1f}" for i, day in enumerate(daily))
        return f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="2.3"/>'

    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="CABTP daily wind speed comparison">',
        '<rect width="1120" height="420" fill="#ffffff"/>',
    ]
    for tick in range(0, 6):
        value = ymax * tick / 5.0
        y = y_for(value)
        parts.append(f'<line x1="{left}" x2="{width-right}" y1="{y:.1f}" y2="{y:.1f}" stroke="#e5e7eb"/>')
        parts.append(f'<text x="{left-10}" y="{y+4:.1f}" text-anchor="end" font-size="12">{value:.0f}</text>')
    parts.append(polyline("observed", "#111827"))
    parts.append(polyline("hrrr", "#2563eb"))
    parts.append(polyline("adjusted", "#dc2626"))
    parts.append('<text x="70" y="398" font-size="13">Observed CABTP</text>')
    parts.append('<text x="235" y="398" font-size="13" fill="#2563eb">HRRR</text>')
    parts.append('<text x="320" y="398" font-size="13" fill="#dc2626">Adjusted HRRR</text>')
    parts.append('<text x="18" y="210" transform="rotate(-90 18 210)" text-anchor="middle" font-size="12">mph</text>')
    parts.append("</svg>")
    return "\n".join(parts)


def write_html(path: Path, rows: list[dict], metrics: list[dict], summary: dict) -> None:
    hrrr, adjusted = metrics
    delta_speed = adjusted["speed_mae"] - hrrr["speed_mae"]
    delta_vector = adjusted["vector_rmse"] - hrrr["vector_rmse"]
    cards = [
        ("Samples", str(len(rows))),
        ("HRRR Speed MAE", f'{hrrr["speed_mae"]:.2f} mph'),
        ("Adjusted Speed MAE", f'{adjusted["speed_mae"]:.2f} mph'),
        ("Speed MAE Change", f"{delta_speed:.2f} mph"),
        ("Vector RMSE Change", f"{delta_vector:.2f} mph"),
        ("Assumed Sensor Height", "5.0 m"),
    ]
    metric_fields = ["result", "sample_count", "speed_bias", "speed_mae", "speed_rmse", "dir_mae_deg", "vector_rmse"]
    metric_rows = "\n".join(
        "<tr>" + "".join(f"<td>{html.escape(str(round(row[field], 3) if isinstance(row[field], float) else row[field]))}</td>" for field in metric_fields) + "</tr>"
        for row in metrics
    )
    sample_rows = "\n".join(
        "<tr>"
        f"<td>{html.escape(row['sample_time_utc'])}</td>"
        f"<td>{row['observed_speed']:.2f}</td>"
        f"<td>{row['hrrr_speed']:.2f}</td>"
        f"<td>{row['adjusted_hrrr_speed']:.2f}</td>"
        f"<td>{row['observed_dir_deg']:.0f}</td>"
        f"<td>{row['hrrr_dir_deg']:.0f}</td>"
        f"<td>{row['adjusted_hrrr_dir_deg']:.0f}</td>"
        "</tr>"
        for row in rows[:24]
    )
    cards_html = "\n".join(
        f'<div class="card"><div class="label">{html.escape(label)}</div><div class="value">{html.escape(value)}</div></div>'
        for label, value in cards
    )
    path.write_text(
        f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>CABTP HRRR vs Adjusted HRRR</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #111827; }}
    .wrap {{ max-width: 1180px; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; margin: 18px 0; }}
    .card {{ border: 1px solid #d1d5db; border-radius: 6px; padding: 12px; background: #f9fafb; }}
    .label {{ color: #4b5563; font-size: 12px; text-transform: uppercase; }}
    .value {{ font-size: 20px; font-weight: 700; margin-top: 6px; }}
    .note {{ color: #4b5563; line-height: 1.45; }}
    table {{ border-collapse: collapse; width: 100%; margin: 16px 0 28px; font-size: 13px; }}
    th, td {{ border: 1px solid #d1d5db; padding: 6px 8px; text-align: right; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ background: #f3f4f6; }}
    svg {{ width: 100%; max-width: 1120px; border: 1px solid #d1d5db; }}
  </style>
</head>
<body>
<div class="wrap">
  <h1>CABTP HRRR vs Adjusted HRRR</h1>
  <p class="note">
    Station: CABTP / Berthoud Pass CAIC, {STATION["latitude"]:.5f}, {STATION["longitude"]:.5f}.
    Period: {html.escape(summary["start_utc"])} through {html.escape(summary["end_utc"])}.
    Observations are treated as the station-reported winds, with a 5 m sensor-height assumption for sensor selection only.
  </p>
  <p class="note">
    HRRR is sampled from the existing native HRRR parent raster. Adjusted HRRR is sampled from the existing GMTED 500 m height-adjusted grids used by the K0CO experiment. No WindNinja run is included here.
  </p>
  <div class="cards">{cards_html}</div>
  {line_svg(rows)}
  <h2>Metrics</h2>
  <table><thead><tr>{''.join(f'<th>{field}</th>' for field in metric_fields)}</tr></thead><tbody>{metric_rows}</tbody></table>
  <h2>First 24 Hourly Samples</h2>
  <table>
    <thead><tr><th>time UTC</th><th>obs speed</th><th>HRRR speed</th><th>adjusted speed</th><th>obs dir</th><th>HRRR dir</th><th>adjusted dir</th></tr></thead>
    <tbody>{sample_rows}</tbody>
  </table>
</div>
</body>
</html>
""",
        encoding="utf-8",
    )


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    x, y = utm13n(STATION["longitude"], STATION["latitude"])
    observations = sv.fetch_observations([STATION], START, END, 30, None, "mph")[STATION["station_id"]]
    rows = []
    missing_adjusted = 0
    missing_raw = 0
    for hour_index in range(int((END - START).total_seconds() // 3600)):
        hour = START + dt.timedelta(hours=hour_index)
        obs = sv.nearest_observation(observations, hour, 30)
        if obs is None:
            continue
        raw_speed_path, raw_dir_path = raw_paths(hour)
        adjusted_speed_path, adjusted_dir_path = adjusted_paths(hour)
        if not raw_speed_path.exists() or not raw_dir_path.exists():
            missing_raw += 1
            continue
        if not adjusted_speed_path.exists() or not adjusted_dir_path.exists():
            missing_adjusted += 1
            continue
        raw_speed = sample_ascii_grid(raw_speed_path, x, y)
        raw_dir = sample_ascii_grid(raw_dir_path, x, y)
        adjusted_speed = sample_ascii_grid(adjusted_speed_path, x, y)
        adjusted_dir = sample_ascii_grid(adjusted_dir_path, x, y)
        if None in (raw_speed, raw_dir, adjusted_speed, adjusted_dir):
            continue
        row = {
            "station_id": STATION["station_id"],
            "station_label": STATION["label"],
            "sample_time_utc": sv.isoformat_utc(hour),
            "obs_time_utc": sv.isoformat_utc(obs["datetime"]),
            "obs_age_minutes": round(abs((hour - obs["datetime"]).total_seconds()) / 60.0, 3),
            "assumed_height_m": STATION["height_m"],
            "observed_speed": round(obs["speed_obs"], 6),
            "observed_dir_deg": round(obs["dir_obs_deg"], 6),
            "u_obs": round(obs["u_obs"], 6),
            "v_obs": round(obs["v_obs"], 6),
        }
        row.update(obs_to_error_fields("hrrr", raw_speed, raw_dir, obs))
        row.update(obs_to_error_fields("adjusted_hrrr", adjusted_speed, adjusted_dir, obs))
        rows.append(row)
    if not rows:
        raise RuntimeError("No CABTP samples matched existing HRRR and adjusted HRRR grids.")
    samples_csv = OUT_DIR / "cabtp_hrrr_adjusted_samples.csv"
    metrics_csv = OUT_DIR / "cabtp_hrrr_adjusted_metrics.csv"
    summary_json = OUT_DIR / "cabtp_hrrr_adjusted_summary.json"
    html_path = OUT_DIR / "cabtp_hrrr_adjusted.html"
    metrics = [
        metric_row("hrrr", rows, "hrrr"),
        metric_row("adjusted_hrrr", rows, "adjusted_hrrr"),
    ]
    rows_to_csv(samples_csv, rows)
    rows_to_csv(metrics_csv, metrics)
    summary = {
        "generated_at_utc": sv.isoformat_utc(dt.datetime.now(UTC)),
        "station": STATION,
        "start_utc": sv.isoformat_utc(START),
        "end_utc": sv.isoformat_utc(END),
        "sample_count": len(rows),
        "missing_raw_hours": missing_raw,
        "missing_adjusted_hours": missing_adjusted,
        "metrics": {"hrrr": metrics[0], "adjusted_hrrr": metrics[1]},
        "adjusted_hrrr_vs_hrrr_delta": {
            "speed_mae": metrics[1]["speed_mae"] - metrics[0]["speed_mae"],
            "speed_bias": metrics[1]["speed_bias"] - metrics[0]["speed_bias"],
            "dir_mae_deg": metrics[1]["dir_mae_deg"] - metrics[0]["dir_mae_deg"],
            "vector_rmse": metrics[1]["vector_rmse"] - metrics[0]["vector_rmse"],
        },
        "outputs": {
            "samples_csv": str(samples_csv),
            "metrics_csv": str(metrics_csv),
            "summary_json": str(summary_json),
            "html": str(html_path),
        },
    }
    summary_json.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    write_html(html_path, rows, metrics, summary)
    print(json.dumps(summary, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
