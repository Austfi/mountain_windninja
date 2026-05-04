#!/usr/bin/env python3
"""Create static validation plots from WindNinja/HRRR/Synoptic samples."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import json
import math
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


def resolve_repo_path(raw_path: str | Path) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return (BASE_DIR / path).resolve()


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


def metric_summary(rows: list[dict]) -> dict:
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
    return {
        "sample_count": len(rows),
        "station_count": len(stations),
        "stations": stations,
        "start_utc": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_utc": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "windninja": {
            "speed_bias": clean_number(mean(values("wn_speed_error"))),
            "speed_mae": clean_number(mean(values("wn_speed_error", absolute=True))),
            "speed_rmse": clean_number(rmse(values("wn_speed_error"))),
            "dir_mae_deg": clean_number(mean(values("wn_dir_abs_error_deg"))),
            "vector_rmse": clean_number(rmse(values("wn_vector_error"))),
        },
        "hrrr": {
            "speed_bias": clean_number(mean(values("wx_speed_error"))),
            "speed_mae": clean_number(mean(values("wx_speed_error", absolute=True))),
            "speed_rmse": clean_number(rmse(values("wx_speed_error"))),
            "dir_mae_deg": clean_number(mean(values("wx_dir_abs_error_deg"))),
            "vector_rmse": clean_number(rmse(values("wx_vector_error"))),
        },
    }


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


def scatter_plot(path: Path, rows: list[dict], *, title: str, units: str) -> None:
    width = 720
    height = 640
    margin = {"left": 78, "right": 34, "top": 70, "bottom": 78}
    plot_width = width - margin["left"] - margin["right"]
    plot_height = height - margin["top"] - margin["bottom"]

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
        if row.get("wn_speed") is not None:
            parts.append(
                f'<circle cx="{scale_x(obs):.1f}" cy="{scale_y(row["wn_speed"]):.1f}" '
                f'r="3.3" fill="{COLORS["windninja"]}" opacity="0.58"/>'
            )
        if row.get("wx_speed") is not None:
            parts.append(
                f'<circle cx="{scale_x(obs):.1f}" cy="{scale_y(row["wx_speed"]):.1f}" '
                f'r="3.3" fill="{COLORS["hrrr"]}" opacity="0.48"/>'
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
    parts.append(f'<circle cx="90" cy="{height - 24}" r="4" fill="{COLORS["windninja"]}"/>')
    parts.append(f'<text x="102" y="{height - 20}" class="legend">WindNinja</text>')
    parts.append(f'<circle cx="220" cy="{height - 24}" r="4" fill="{COLORS["hrrr"]}"/>')
    parts.append(f'<text x="232" y="{height - 20}" class="legend">HRRR</text>')
    parts.append(svg_footer())
    path.write_text("\n".join(parts), encoding="utf-8")


def write_summary_json(path: Path, summary: dict, source_paths: list[Path], plots: list[str]) -> None:
    payload = {
        "generated_at_utc": dt.datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_paths": [str(path) for path in source_paths],
        "plots": plots,
        "summary": summary,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_index(path: Path, summary: dict, plots: list[str], title: str) -> None:
    def card(label: str, value: str) -> str:
        return (
            '<div class="card">'
            f'<div class="label">{html.escape(label)}</div>'
            f'<div class="value">{html.escape(value)}</div>'
            '</div>'
        )

    wn = summary["windninja"]
    wx = summary["hrrr"]
    cards = [
        card("Samples", str(summary["sample_count"])),
        card("Stations", str(summary["station_count"])),
        card("Window", f'{summary["start_utc"]} to {summary["end_utc"]}'),
        card("WN Speed MAE", f'{wn["speed_mae"]} mph'),
        card("HRRR Speed MAE", f'{wx["speed_mae"]} mph'),
        card("WN Vector RMSE", f'{wn["vector_rmse"]} mph'),
        card("HRRR Vector RMSE", f'{wx["vector_rmse"]} mph'),
        card("WN Direction MAE", f'{wn["dir_mae_deg"]} deg'),
        card("HRRR Direction MAE", f'{wx["dir_mae_deg"]} deg'),
    ]
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
    img {{ display: block; width: 100%; height: auto; }}
  </style>
</head>
<body>
<main>
  <h1>{html.escape(title)}</h1>
  <div class="cards">
    {"".join(cards)}
  </div>
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
    return parser


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

    rows = load_samples(source_paths, args.station_id)
    summary = metric_summary(rows)
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
    line_plot(
        output_dir / plots[0],
        time_records,
        [
            ("Observed", "speed_obs", COLORS["obs"]),
            ("WindNinja", "wn_speed", COLORS["windninja"]),
            ("HRRR", "wx_speed", COLORS["hrrr"]),
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
            ("HRRR error", "wx_speed_error", COLORS["hrrr"]),
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
            ("HRRR", "wx_dir_abs_error_deg", COLORS["hrrr"]),
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
    )
    line_plot(
        output_dir / plots[4],
        daily_records(rows),
        [
            ("WN vector RMSE", "wn_vector_rmse", COLORS["windninja"]),
            ("HRRR vector RMSE", "wx_vector_rmse", COLORS["hrrr"]),
            ("WN speed MAE", "wn_speed_mae", "#5dade2"),
            ("HRRR speed MAE", "wx_speed_mae", "#e74c3c"),
        ],
        title="Daily Error Metrics",
        y_label=f"Error ({args.speed_units})",
        include_zero=True,
    )

    write_summary_json(output_dir / "plot_summary.json", summary, source_paths, plots)
    write_index(output_dir / "index.html", summary, plots, args.title)

    print(f"Wrote validation plots to {output_dir}")
    print(f"Samples: {summary['sample_count']} | Stations: {summary['station_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
