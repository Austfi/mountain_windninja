#!/usr/bin/env python3
"""HRRR-only K0CO point tuning for height-adjusted forcing assumptions."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import json
import math
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

try:
    from . import k0co_height_hrrr_validation as kh
    from . import synoptic_validation as sv
    from . import validation_study as vs
    from .wind_math import convert_speed
except ImportError:
    import k0co_height_hrrr_validation as kh
    import synoptic_validation as sv
    import validation_study as vs
    from wind_math import convert_speed


DEFAULT_VALIDATION_ROOT = Path("runtime/validation/berthoud_pass_k0co_height_hrrr")
DEFAULT_START = "202601010000"
DEFAULT_END = "202604010000"
K0CO_LON = -105.76393
K0CO_LAT = 39.79453


@dataclass(frozen=True)
class Setting:
    name: str
    formula: str
    low_cap: float
    high_cap: float
    blend_scale_m: float | None = None
    fixed_weight: float | None = None
    threshold_m: float | None = None


def obs_to_uv(speed: float, direction_deg: float) -> tuple[float, float]:
    radians = math.radians(direction_deg)
    return -speed * math.sin(radians), -speed * math.cos(radians)


def speed_dir_from_uv(u: float, v: float) -> tuple[float, float]:
    speed = math.hypot(u, v)
    direction = (270.0 - math.degrees(math.atan2(v, u))) % 360.0
    return speed, direction


def circular_abs_error(model_dir: float, obs_dir: float) -> float:
    return abs(((model_dir - obs_dir + 180.0) % 360.0) - 180.0)


def sample_band(path: Path, band: int, lon: float, lat: float) -> float:
    result = subprocess.run(
        ["gdallocationinfo", "-b", str(band), "-wgs84", "-valonly", str(path), str(lon), str(lat)],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    for line in reversed(result.stdout.splitlines()):
        try:
            return float(line.strip())
        except ValueError:
            continue
    raise RuntimeError(f"No numeric sample for band {band} in {path}")


def fieldnames() -> list[str]:
    return [
        "sample_time_utc",
        "obs_time_utc",
        "obs_speed",
        "obs_dir_deg",
        "u10",
        "v10",
        "u80",
        "v80",
        "hrrr_surface_hgt_m",
    ]


def metric_fieldnames() -> list[str]:
    return [
        "setting",
        "formula",
        "sample_count",
        "speed_mae_mph",
        "speed_bias_mph",
        "direction_mae_deg",
        "vector_rmse_mph",
        "speed_mae_improvement_mph",
        "vector_rmse_improvement_mph",
        "cap_low_fraction",
        "cap_high_fraction",
        "mean_weight",
        "blend_scale_m",
        "fixed_weight",
        "threshold_m",
        "low_cap",
        "high_cap",
    ]


def parse_float(value: str | float | int | None) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def load_observation_rows(samples_csv: Path, start: dt.datetime, end: dt.datetime) -> list[dict]:
    rows = []
    with samples_csv.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            run_time = sv.parse_iso_time(row["sample_time_utc"])
            if start <= run_time < end:
                rows.append(row)
    return rows


def load_existing_fields(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    rows = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            rows[row["sample_time_utc"]] = row
    return rows


def append_field_rows(path: Path, rows: list[dict]) -> None:
    exists = path.exists() and path.stat().st_size > 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames())
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def sample_hour(
    row: dict,
    *,
    cache_root: Path,
    archive_base_url: str,
    lon: float,
    lat: float,
) -> dict:
    run_time = sv.parse_iso_time(row["sample_time_utc"])
    hour_dir = cache_root / run_time.strftime("%Y%m%d%H%M")
    try:
        grib_path = kh.download_hrrr_subset(run_time, hour_dir / "grib", archive_base_url)
        return {
            "sample_time_utc": row["sample_time_utc"],
            "obs_time_utc": row["obs_time_utc"],
            "obs_speed": float(row["observed_speed"]),
            "obs_dir_deg": float(row["observed_dir_deg"]),
            "u10": sample_band(grib_path, 1, lon, lat),
            "v10": sample_band(grib_path, 2, lon, lat),
            "u80": sample_band(grib_path, 3, lon, lat),
            "v80": sample_band(grib_path, 4, lon, lat),
            "hrrr_surface_hgt_m": sample_band(grib_path, 5, lon, lat),
        }
    finally:
        shutil.rmtree(hour_dir, ignore_errors=True)


def settings() -> list[Setting]:
    output = [Setting("HRRR_10m", "raw_hrrr_10m", 1.0, 1.0, fixed_weight=0.0)]
    for scale in (200.0, 300.0, 450.0, 600.0):
        for low in (0.75, 0.85):
            for high in (1.20, 1.35, 1.50):
                output.append(
                    Setting(
                        f"blend_scale_{int(scale)}m_low_{low:.2f}_high_{high:.2f}",
                        "elevation_blend",
                        low,
                        high,
                        blend_scale_m=scale,
                    )
                )
    for weight in (0.25, 0.50, 0.75, 1.0):
        for high in (1.20, 1.35, 1.50):
            output.append(
                Setting(
                    f"fixed_weight_{weight:.2f}_low_0.75_high_{high:.2f}",
                    "fixed_weight",
                    0.75,
                    high,
                    fixed_weight=weight,
                )
            )
    for threshold in (0.0, 100.0, 200.0, 300.0):
        for high in (1.20, 1.35, 1.50):
            output.append(
                Setting(
                    f"threshold_{int(threshold)}m_low_0.75_high_{high:.2f}",
                    "threshold_80m",
                    0.75,
                    high,
                    threshold_m=threshold,
                )
            )
    return output


def setting_weight(setting: Setting, elevation_delta_m: float) -> float:
    if setting.formula == "raw_hrrr_10m":
        return 0.0
    if setting.formula == "elevation_blend":
        assert setting.blend_scale_m is not None
        return max(0.0, min(elevation_delta_m / setting.blend_scale_m, 1.0))
    if setting.formula == "fixed_weight":
        assert setting.fixed_weight is not None
        return setting.fixed_weight
    if setting.formula == "threshold_80m":
        assert setting.threshold_m is not None
        return 1.0 if elevation_delta_m >= setting.threshold_m else 0.0
    raise ValueError(f"Unknown formula {setting.formula}")


def evaluate_setting(record: dict, setting: Setting, gmted_elevation_m: float) -> dict:
    obs_speed = float(record["obs_speed"])
    obs_dir = float(record["obs_dir_deg"])
    obs_u, obs_v = obs_to_uv(obs_speed, obs_dir)
    u10 = float(record["u10"])
    v10 = float(record["v10"])
    u80 = float(record["u80"])
    v80 = float(record["v80"])
    raw_speed_mps, raw_dir = speed_dir_from_uv(u10, v10)
    elevation_delta_m = gmted_elevation_m - float(record["hrrr_surface_hgt_m"])
    weight = setting_weight(setting, elevation_delta_m)
    u = (1.0 - weight) * u10 + weight * u80
    v = (1.0 - weight) * v10 + weight * v80
    adjusted_speed_mps, adjusted_dir = speed_dir_from_uv(u, v)

    cap = ""
    capped_speed_mps = adjusted_speed_mps
    low_limit = raw_speed_mps * setting.low_cap
    high_limit = raw_speed_mps * setting.high_cap
    if capped_speed_mps < low_limit:
        capped_speed_mps = low_limit
        cap = "low"
    elif capped_speed_mps > high_limit:
        capped_speed_mps = high_limit
        cap = "high"
    if adjusted_speed_mps > 0.0 and capped_speed_mps != adjusted_speed_mps:
        factor = capped_speed_mps / adjusted_speed_mps
        u *= factor
        v *= factor

    speed_mph = convert_speed(capped_speed_mps, "mps", "mph")
    u_mph = convert_speed(u, "mps", "mph")
    v_mph = convert_speed(v, "mps", "mph")
    return {
        "sample_time_utc": record["sample_time_utc"],
        "setting": setting.name,
        "speed_mph": speed_mph,
        "direction_deg": adjusted_dir if setting.formula != "raw_hrrr_10m" else raw_dir,
        "speed_error_mph": speed_mph - obs_speed,
        "dir_abs_error_deg": circular_abs_error(adjusted_dir if setting.formula != "raw_hrrr_10m" else raw_dir, obs_dir),
        "vector_error_mph": math.hypot(u_mph - obs_u, v_mph - obs_v),
        "weight": weight,
        "cap": cap,
    }


def summarize(setting: Setting, values: list[dict], baseline: dict | None = None) -> dict:
    sample_count = len(values)
    speed_mae = sum(abs(row["speed_error_mph"]) for row in values) / sample_count
    vector_rmse = math.sqrt(sum(row["vector_error_mph"] ** 2 for row in values) / sample_count)
    return {
        "setting": setting.name,
        "formula": setting.formula,
        "sample_count": sample_count,
        "speed_mae_mph": speed_mae,
        "speed_bias_mph": sum(row["speed_error_mph"] for row in values) / sample_count,
        "direction_mae_deg": sum(row["dir_abs_error_deg"] for row in values) / sample_count,
        "vector_rmse_mph": vector_rmse,
        "speed_mae_improvement_mph": None if baseline is None else baseline["speed_mae_mph"] - speed_mae,
        "vector_rmse_improvement_mph": None if baseline is None else baseline["vector_rmse_mph"] - vector_rmse,
        "cap_low_fraction": sum(1 for row in values if row["cap"] == "low") / sample_count,
        "cap_high_fraction": sum(1 for row in values if row["cap"] == "high") / sample_count,
        "mean_weight": sum(row["weight"] for row in values) / sample_count,
        "blend_scale_m": setting.blend_scale_m,
        "fixed_weight": setting.fixed_weight,
        "threshold_m": setting.threshold_m,
        "low_cap": setting.low_cap,
        "high_cap": setting.high_cap,
    }


def evaluate(records: list[dict], gmted_elevation_m: float) -> tuple[list[dict], list[dict]]:
    grouped = {}
    samples = []
    for setting in settings():
        values = [evaluate_setting(record, setting, gmted_elevation_m) for record in records]
        grouped[setting.name] = (setting, values)
        samples.extend(values)
    baseline_summary = summarize(*grouped["HRRR_10m"], baseline=None)
    metrics = [
        summarize(setting, values, baseline_summary)
        for setting, values in grouped.values()
    ]
    metrics.sort(key=lambda row: (row["speed_mae_mph"], row["vector_rmse_mph"]))
    return metrics, samples


def format_number(value: object, digits: int = 2) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            return html.escape(value)
    return f"{float(value):.{digits}f}"


def metric_table(rows: list[dict], limit: int = 15) -> str:
    headers = [
        "Setting",
        "Formula",
        "Speed MAE",
        "Bias",
        "Dir MAE",
        "Vector RMSE",
        "High Cap",
        "Mean Weight",
    ]
    body = []
    for row in rows[:limit]:
        body.append(
            "<tr>"
            f"<td>{html.escape(str(row['setting']))}</td>"
            f"<td>{html.escape(str(row['formula']))}</td>"
            f"<td>{format_number(row['speed_mae_mph'])}</td>"
            f"<td>{format_number(row['speed_bias_mph'])}</td>"
            f"<td>{format_number(row['direction_mae_deg'])}</td>"
            f"<td>{format_number(row['vector_rmse_mph'])}</td>"
            f"<td>{format_number(float(row['cap_high_fraction']) * 100.0, 1)}%</td>"
            f"<td>{format_number(row['mean_weight'])}</td>"
            "</tr>"
        )
    return (
        "<table><thead><tr>"
        + "".join(f"<th>{header}</th>" for header in headers)
        + "</tr></thead><tbody>"
        + "".join(body)
        + "</tbody></table>"
    )


def scatter_svg(rows: list[dict], baseline: dict) -> str:
    width = 720
    height = 420
    pad = 54
    x_values = [float(row["speed_mae_mph"]) for row in rows]
    y_values = [float(row["vector_rmse_mph"]) for row in rows]
    min_x, max_x = min(x_values), max(x_values)
    min_y, max_y = min(y_values), max(y_values)

    def sx(value: float) -> float:
        return pad + (value - min_x) / (max_x - min_x or 1.0) * (width - 2 * pad)

    def sy(value: float) -> float:
        return height - pad - (value - min_y) / (max_y - min_y or 1.0) * (height - 2 * pad)

    colors = {
        "raw_hrrr_10m": "#222222",
        "elevation_blend": "#1f77b4",
        "fixed_weight": "#2ca02c",
        "threshold_80m": "#d62728",
    }
    points = []
    for row in rows:
        x = sx(float(row["speed_mae_mph"]))
        y = sy(float(row["vector_rmse_mph"]))
        color = colors.get(str(row["formula"]), "#555555")
        radius = 6 if row["setting"] == "HRRR_10m" else 4
        title = html.escape(
            f"{row['setting']}: speed MAE {float(row['speed_mae_mph']):.2f}, "
            f"vector RMSE {float(row['vector_rmse_mph']):.2f}"
        )
        points.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{radius}" fill="{color}"><title>{title}</title></circle>')
    baseline_lines = (
        f'<line x1="{sx(float(baseline["speed_mae_mph"])):.1f}" y1="{pad}" '
        f'x2="{sx(float(baseline["speed_mae_mph"])):.1f}" y2="{height-pad}" stroke="#777" stroke-dasharray="4 4"/>'
        f'<line x1="{pad}" y1="{sy(float(baseline["vector_rmse_mph"])):.1f}" '
        f'x2="{width-pad}" y2="{sy(float(baseline["vector_rmse_mph"])):.1f}" stroke="#777" stroke-dasharray="4 4"/>'
    )
    return f"""
<svg viewBox="0 0 {width} {height}" role="img" aria-label="Speed MAE versus vector RMSE">
  <rect x="0" y="0" width="{width}" height="{height}" fill="#fff"/>
  <line x1="{pad}" y1="{height-pad}" x2="{width-pad}" y2="{height-pad}" stroke="#222"/>
  <line x1="{pad}" y1="{pad}" x2="{pad}" y2="{height-pad}" stroke="#222"/>
  {baseline_lines}
  {''.join(points)}
  <text x="{width/2}" y="{height-12}" text-anchor="middle">Speed MAE (mph)</text>
  <text x="18" y="{height/2}" transform="rotate(-90 18 {height/2})" text-anchor="middle">Vector RMSE (mph)</text>
  <text x="{pad}" y="{height-pad+22}" text-anchor="middle">{min_x:.1f}</text>
  <text x="{width-pad}" y="{height-pad+22}" text-anchor="middle">{max_x:.1f}</text>
  <text x="{pad-8}" y="{height-pad}" text-anchor="end">{min_y:.1f}</text>
  <text x="{pad-8}" y="{pad}" text-anchor="end">{max_y:.1f}</text>
</svg>
"""


def write_html(path: Path, summary: dict, metrics: list[dict]) -> None:
    baseline = next(row for row in metrics if row["setting"] == "HRRR_10m")
    by_speed = sorted(metrics, key=lambda row: (row["speed_mae_mph"], row["vector_rmse_mph"]))
    by_vector = sorted(metrics, key=lambda row: (row["vector_rmse_mph"], row["speed_mae_mph"]))
    best_by_formula = []
    for formula in ("elevation_blend", "fixed_weight", "threshold_80m"):
        candidates = [row for row in metrics if row["formula"] == formula]
        if candidates:
            best_by_formula.append(sorted(candidates, key=lambda row: row["speed_mae_mph"])[0])
    path.write_text(
        f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>K0CO HRRR Point Tuning</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 32px; color: #1f2933; }}
    h1, h2 {{ margin-bottom: 0.3rem; }}
    .meta {{ color: #52606d; margin-top: 0; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin: 18px 0; }}
    .stat {{ border: 1px solid #d9e2ec; border-radius: 6px; padding: 12px; background: #f8fafc; }}
    .stat b {{ display: block; font-size: 1.4rem; }}
    table {{ border-collapse: collapse; width: 100%; margin: 16px 0 28px; font-size: 0.92rem; }}
    th, td {{ border-bottom: 1px solid #d9e2ec; padding: 7px 8px; text-align: right; }}
    th:first-child, td:first-child, th:nth-child(2), td:nth-child(2) {{ text-align: left; }}
    th {{ background: #f1f5f9; }}
    svg {{ width: 100%; max-width: 860px; border: 1px solid #d9e2ec; border-radius: 6px; }}
    .note {{ background: #fff8e1; border-left: 4px solid #f0b429; padding: 10px 12px; }}
    code {{ background: #edf2f7; padding: 1px 4px; border-radius: 3px; }}
  </style>
</head>
<body>
  <h1>K0CO HRRR Point Tuning</h1>
  <p class="meta">Generated {html.escape(summary['generated_at_utc'])}</p>
  <div class="grid">
    <div class="stat"><span>Period</span><b>{html.escape(summary['period_start'])}</b><span>to {html.escape(summary['period_end'])}</span></div>
    <div class="stat"><span>Samples</span><b>{summary['sample_count']}</b><span>K0CO station-hours</span></div>
    <div class="stat"><span>GMTED Elevation</span><b>{float(summary['gmted_elevation_m']):.1f} m</b><span>at K0CO</span></div>
    <div class="stat"><span>Settings Tested</span><b>{len(metrics)}</b><span>HRRR-only formulas</span></div>
  </div>
  <p class="note">This report tunes HRRR/adjusted-HRRR at the nearest K0CO point only. It does not include WindNinja response.</p>
  <h2>Best By Speed MAE</h2>
  {metric_table(by_speed)}
  <h2>Best By Vector RMSE</h2>
  {metric_table(by_vector)}
  <h2>Best Per Formula Family</h2>
  {metric_table(best_by_formula, limit=len(best_by_formula))}
  <h2>Speed MAE vs Vector RMSE</h2>
  {scatter_svg(metrics, baseline)}
  <p>Files: <code>{html.escape(summary['metrics_csv'])}</code>, <code>{html.escape(summary['samples_csv'])}</code></p>
</body>
</html>
""",
        encoding="utf-8",
    )


def write_csv(path: Path, rows: list[dict], names: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=names)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                name: round(value, 6) if isinstance((value := row.get(name)), float) else value
                for name in names
            })


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run K0CO HRRR-only point tuning.")
    parser.add_argument("--start", default=DEFAULT_START)
    parser.add_argument("--end", default=DEFAULT_END)
    parser.add_argument("--validation-root", type=Path, default=DEFAULT_VALIDATION_ROOT)
    parser.add_argument("--out-dir", type=Path)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--archive-base-url", default=kh.DEFAULT_ARCHIVE_BASE_URL)
    parser.add_argument("--lon", type=float, default=K0CO_LON)
    parser.add_argument("--lat", type=float, default=K0CO_LAT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    start = vs.parse_utc(args.start)
    end = vs.parse_utc(args.end)
    validation_root = args.validation_root
    out_dir = args.out_dir or validation_root / "tuning"
    out_dir.mkdir(parents=True, exist_ok=True)
    cache_root = out_dir / "point_raw_cache"
    cache_root.mkdir(parents=True, exist_ok=True)
    stamp = f"{start:%Y%m%d%H%M}_{end:%Y%m%d%H%M}"
    field_csv = out_dir / f"point_fields_{stamp}.csv"
    metrics_csv = out_dir / f"point_tuning_{stamp}_metrics.csv"
    samples_csv = out_dir / f"point_tuning_{stamp}_samples.csv"
    summary_json = out_dir / f"point_tuning_{stamp}_summary.json"
    html_path = out_dir / f"point_tuning_{stamp}.html"

    observation_rows = load_observation_rows(validation_root / "hrrr_comparison_samples.csv", start, end)
    existing = load_existing_fields(field_csv)
    missing = [row for row in observation_rows if row["sample_time_utc"] not in existing]
    print(f"Loaded {len(observation_rows)} observed K0CO hours; {len(missing)} need HRRR point sampling.", flush=True)
    gmted_elevation = kh.sample_dataset_value(validation_root / "gmted_500m" / "elevation.asc", args.lon, args.lat)
    if gmted_elevation is None:
        raise RuntimeError("Could not sample GMTED 500 m elevation at K0CO.")
    print(f"GMTED sample elevation at K0CO: {gmted_elevation:.2f} m", flush=True)

    if missing:
        start_wall = time.monotonic()
        pending_rows = []
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = {
                pool.submit(
                    sample_hour,
                    row,
                    cache_root=cache_root,
                    archive_base_url=args.archive_base_url,
                    lon=args.lon,
                    lat=args.lat,
                ): row
                for row in missing
            }
            for completed, future in enumerate(as_completed(futures), start=1):
                pending_rows.append(future.result())
                if len(pending_rows) >= 24:
                    append_field_rows(field_csv, pending_rows)
                    existing.update({row["sample_time_utc"]: row for row in pending_rows})
                    pending_rows.clear()
                if completed == 1 or completed % 24 == 0 or completed == len(futures):
                    elapsed = time.monotonic() - start_wall
                    rate = completed / elapsed if elapsed else 0.0
                    remaining = (len(futures) - completed) / rate if rate else 0.0
                    print(
                        f"Sampled HRRR point fields {completed}/{len(futures)}; "
                        f"ETA {remaining / 60.0:.1f} min",
                        flush=True,
                    )
            if pending_rows:
                append_field_rows(field_csv, pending_rows)
                existing.update({row["sample_time_utc"]: row for row in pending_rows})

    records = [existing[row["sample_time_utc"]] for row in observation_rows if row["sample_time_utc"] in existing]
    records.sort(key=lambda row: row["sample_time_utc"])
    metrics, samples = evaluate(records, gmted_elevation)
    write_csv(metrics_csv, metrics, metric_fieldnames())
    write_csv(
        samples_csv,
        samples,
        [
            "sample_time_utc",
            "setting",
            "speed_mph",
            "direction_deg",
            "speed_error_mph",
            "dir_abs_error_deg",
            "vector_error_mph",
            "weight",
            "cap",
        ],
    )
    summary = {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "period_start": sv.isoformat_utc(start),
        "period_end": sv.isoformat_utc(end),
        "sample_count": len(records),
        "gmted_elevation_m": gmted_elevation,
        "field_csv": str(field_csv),
        "metrics_csv": str(metrics_csv),
        "samples_csv": str(samples_csv),
        "html": str(html_path),
        "best_by_speed_mae": metrics[:10],
        "best_by_vector_rmse": sorted(metrics, key=lambda row: row["vector_rmse_mph"])[:10],
    }
    summary_json.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    write_html(html_path, summary, metrics)
    print(json.dumps(summary, indent=2), flush=True)
    shutil.rmtree(cache_root, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
