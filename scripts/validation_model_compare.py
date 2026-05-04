#!/usr/bin/env python3
"""Compare completed validation study roots on common station-hours."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import json
import math
from pathlib import Path

try:
    from . import validation_plots as vp
except ImportError:
    import validation_plots as vp


UTC = dt.timezone.utc

METRIC_FIELDS = [
    "station_id",
    "model",
    "series",
    "sample_count",
    "speed_bias",
    "speed_mae",
    "speed_rmse",
    "vector_rmse",
    "dir_mae_deg",
    "dir_count_ge_5mph",
    "dir_mae_ge_5mph",
    "dir_count_ge_10mph",
    "dir_mae_ge_10mph",
]

MODEL_COLORS = {
    ("HRRR", "parent"): "#c0392b",
    ("HRRR", "windninja"): "#1f77b4",
    ("NBM", "parent"): "#8e44ad",
    ("NBM", "windninja"): "#16a085",
}


def clean_number(value: float | None, digits: int = 2) -> float | None:
    if value is None or math.isnan(value):
        return None
    return round(value, digits)


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else float("nan")


def rmse(values: list[float]) -> float:
    return math.sqrt(mean([value * value for value in values])) if values else float("nan")


def finite(value: float | None) -> bool:
    return value is not None and not math.isnan(value)


def infer_model_label(study_root: Path, index: int) -> str:
    label = vp.infer_model_label(study_root, None)
    if label != "HRRR" or index == 0:
        return label
    if "nbm" in study_root.name.lower():
        return "NBM"
    return label


def load_studies(study_roots: list[Path]) -> list[dict]:
    studies = []
    for index, root in enumerate(study_roots):
        source_paths = vp.collect_sample_paths(root)
        rows = vp.load_samples(source_paths)
        label = infer_model_label(root, index)
        rows_by_key = {
            (row["station_id"], row["sample_time_utc"]): row
            for row in rows
        }
        studies.append({
            "root": root,
            "model": label,
            "source_paths": source_paths,
            "rows_by_key": rows_by_key,
        })
    return studies


def common_keys(studies: list[dict]) -> list[tuple[str, str]]:
    key_sets = [set(study["rows_by_key"]) for study in studies]
    shared = set.intersection(*key_sets)
    if not shared:
        raise ValueError("No common station/timestamp samples found across study roots.")
    return sorted(shared, key=lambda item: (item[1], item[0]))


def metric_row(
    *,
    station_id: str,
    model: str,
    series: str,
    rows: list[dict],
    prefix: str,
) -> dict:
    speed_errors = [
        row[f"{prefix}_speed_error"]
        for row in rows
        if finite(row.get(f"{prefix}_speed_error"))
    ]
    vector_errors = [
        row[f"{prefix}_vector_error"]
        for row in rows
        if finite(row.get(f"{prefix}_vector_error"))
    ]
    dir_errors = [
        row[f"{prefix}_dir_abs_error_deg"]
        for row in rows
        if finite(row.get(f"{prefix}_dir_abs_error_deg"))
    ]
    dir_rows_ge_5 = [
        row
        for row in rows
        if finite(row.get("speed_obs"))
        and row["speed_obs"] >= 5.0
        and finite(row.get(f"{prefix}_dir_abs_error_deg"))
    ]
    dir_rows_ge_10 = [
        row
        for row in rows
        if finite(row.get("speed_obs"))
        and row["speed_obs"] >= 10.0
        and finite(row.get(f"{prefix}_dir_abs_error_deg"))
    ]

    return {
        "station_id": station_id,
        "model": model,
        "series": series,
        "sample_count": len(rows),
        "speed_bias": clean_number(mean(speed_errors)),
        "speed_mae": clean_number(mean([abs(value) for value in speed_errors])),
        "speed_rmse": clean_number(rmse(speed_errors)),
        "vector_rmse": clean_number(rmse(vector_errors)),
        "dir_mae_deg": clean_number(mean(dir_errors)),
        "dir_count_ge_5mph": len(dir_rows_ge_5),
        "dir_mae_ge_5mph": clean_number(mean([
            row[f"{prefix}_dir_abs_error_deg"]
            for row in dir_rows_ge_5
        ])),
        "dir_count_ge_10mph": len(dir_rows_ge_10),
        "dir_mae_ge_10mph": clean_number(mean([
            row[f"{prefix}_dir_abs_error_deg"]
            for row in dir_rows_ge_10
        ])),
    }


def build_metrics(studies: list[dict], keys: list[tuple[str, str]]) -> list[dict]:
    metrics = []
    stations = sorted({station_id for station_id, _ in keys})
    station_groups = {"ALL": keys}
    station_groups.update({
        station_id: [key for key in keys if key[0] == station_id]
        for station_id in stations
    })

    for study in studies:
        model = study["model"]
        rows_by_key = study["rows_by_key"]
        for station_id, station_keys in station_groups.items():
            rows = [rows_by_key[key] for key in station_keys]
            metrics.append(metric_row(
                station_id=station_id,
                model=model,
                series=model,
                rows=rows,
                prefix="wx",
            ))
            metrics.append(metric_row(
                station_id=station_id,
                model=model,
                series=f"WindNinja ({model})",
                rows=rows,
                prefix="wn",
            ))
    return metrics


def write_metrics_csv(path: Path, metrics: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=METRIC_FIELDS)
        writer.writeheader()
        writer.writerows(metrics)


def format_value(value) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        if math.isnan(value):
            return "n/a"
        if abs(value) >= 100:
            return f"{value:.0f}"
        if abs(value) >= 10:
            return f"{value:.1f}"
        return f"{value:.2f}".rstrip("0").rstrip(".")
    return str(value)


def series_color(model: str, series: str) -> str:
    kind = "windninja" if series.startswith("WindNinja") else "parent"
    return MODEL_COLORS.get((model.upper(), kind), "#334155")


def bar_chart(
    path: Path,
    metrics: list[dict],
    *,
    field: str,
    title: str,
    y_label: str,
) -> None:
    station_rows = [row for row in metrics if row["station_id"] != "ALL"]
    if not station_rows:
        return

    stations = sorted({row["station_id"] for row in station_rows})
    series = []
    for row in station_rows:
        label = row["series"]
        if label not in series:
            series.append(label)

    width = 1120
    height = 660
    margin = {"left": 82, "right": 36, "top": 76, "bottom": 130}
    plot_width = width - margin["left"] - margin["right"]
    plot_height = height - margin["top"] - margin["bottom"]
    values = [
        row[field]
        for row in station_rows
        if isinstance(row.get(field), (int, float)) and row[field] is not None
    ]
    high = max(values) * 1.15 if values else 1.0
    if math.isclose(high, 0.0):
        high = 1.0

    def x_station(index: int) -> float:
        return margin["left"] + (index + 0.5) * (plot_width / len(stations))

    def y_scale(value: float) -> float:
        return margin["top"] + (1.0 - value / high) * plot_height

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}">',
        "<style>",
        "text{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;fill:#1f2933}",
        ".title{font-size:22px;font-weight:700}",
        ".axis{font-size:12px;fill:#52616f}",
        ".legend{font-size:13px}",
        ".grid{stroke:#d8dee9;stroke-width:1}",
        "</style>",
        '<rect x="0" y="0" width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{width / 2:.1f}" y="36" text-anchor="middle" class="title">{html.escape(title)}</text>',
    ]

    for tick_index in range(6):
        value = high * tick_index / 5
        y = y_scale(value)
        parts.append(
            f'<line x1="{margin["left"]}" x2="{width - margin["right"]}" '
            f'y1="{y:.1f}" y2="{y:.1f}" class="grid"/>'
        )
        parts.append(
            f'<text x="{margin["left"] - 10}" y="{y + 4:.1f}" '
            f'text-anchor="end" class="axis">{html.escape(format_value(clean_number(value)))}</text>'
        )

    group_width = plot_width / len(stations)
    bar_gap = 4
    bar_width = max((group_width - 26) / max(len(series), 1) - bar_gap, 8)
    lookup = {
        (row["station_id"], row["series"]): row
        for row in station_rows
    }

    for station_index, station in enumerate(stations):
        center = x_station(station_index)
        start_x = center - ((bar_width + bar_gap) * len(series) - bar_gap) / 2
        for series_index, label in enumerate(series):
            row = lookup.get((station, label))
            if not row or row.get(field) is None:
                continue
            value = row[field]
            x = start_x + series_index * (bar_width + bar_gap)
            y = y_scale(value)
            color = series_color(row["model"], row["series"])
            parts.append(
                f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_width:.1f}" '
                f'height="{height - margin["bottom"] - y:.1f}" fill="{color}" opacity="0.9"/>'
            )
        parts.append(
            f'<text x="{center:.1f}" y="{height - margin["bottom"] + 26}" '
            f'text-anchor="middle" class="axis">{html.escape(station)}</text>'
        )

    parts.append(
        f'<line x1="{margin["left"]}" x2="{margin["left"]}" y1="{margin["top"]}" '
        f'y2="{height - margin["bottom"]}" stroke="#2f3542" stroke-width="1.2"/>'
    )
    parts.append(
        f'<line x1="{margin["left"]}" x2="{width - margin["right"]}" '
        f'y1="{height - margin["bottom"]}" y2="{height - margin["bottom"]}" '
        'stroke="#2f3542" stroke-width="1.2"/>'
    )
    parts.append(
        f'<text x="20" y="{margin["top"] + plot_height / 2:.1f}" '
        f'transform="rotate(-90 20 {margin["top"] + plot_height / 2:.1f})" '
        f'text-anchor="middle" class="axis">{html.escape(y_label)}</text>'
    )

    legend_y = height - 62
    legend_x = margin["left"]
    for index, label in enumerate(series):
        model = label.split("(")[-1].rstrip(")") if label.startswith("WindNinja") else label
        x = legend_x + index * 235
        parts.append(
            f'<rect x="{x}" y="{legend_y}" width="14" height="14" '
            f'fill="{series_color(model, label)}"/>'
        )
        parts.append(
            f'<text x="{x + 22}" y="{legend_y + 12}" class="legend">{html.escape(label)}</text>'
        )

    parts.append("</svg>\n")
    path.write_text("\n".join(parts), encoding="utf-8")


def write_summary(path: Path, studies: list[dict], keys: list[tuple[str, str]], plots: list[str]) -> None:
    times = [vp.parse_time(stamp) for _, stamp in keys]
    payload = {
        "generated_at_utc": dt.datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "common_sample_count": len(keys),
        "station_count": len({station_id for station_id, _ in keys}),
        "stations": sorted({station_id for station_id, _ in keys}),
        "start_utc": min(times).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_utc": max(times).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "studies": [
            {
                "model": study["model"],
                "root": str(study["root"]),
                "source_paths": [str(path) for path in study["source_paths"]],
                "sample_count": len(study["rows_by_key"]),
            }
            for study in studies
        ],
        "plots": plots,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def write_index(path: Path, title: str, metrics: list[dict], keys: list[tuple[str, str]], plots: list[str]) -> None:
    all_rows = [row for row in metrics if row["station_id"] == "ALL"]
    station_rows = [row for row in metrics if row["station_id"] != "ALL"]
    table_rows = "\n".join(
        "<tr>"
        f"<td>{html.escape(row['station_id'])}</td>"
        f"<td>{html.escape(row['series'])}</td>"
        f"<td>{html.escape(str(row['sample_count']))}</td>"
        f"<td>{html.escape(format_value(row['speed_bias']))}</td>"
        f"<td>{html.escape(format_value(row['speed_mae']))}</td>"
        f"<td>{html.escape(format_value(row['vector_rmse']))}</td>"
        f"<td>{html.escape(format_value(row['dir_mae_ge_5mph']))}</td>"
        "</tr>"
        for row in station_rows
    )
    cards = "\n".join(
        '<div class="card">'
        f'<div class="label">{html.escape(row["series"])}</div>'
        f'<div class="value">{html.escape(format_value(row["speed_mae"]))} mph</div>'
        '<div class="sub">pooled speed MAE</div>'
        '</div>'
        for row in all_rows
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
    body {{ margin: 0; background: #f5f7fa; color: #1f2933; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
    main {{ max-width: 1120px; margin: 0 auto; padding: 28px; }}
    h1 {{ margin: 0 0 18px; font-size: 30px; }}
    h2 {{ margin: 28px 0 12px; font-size: 18px; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 12px; margin-bottom: 22px; }}
    .card {{ background: white; border: 1px solid #d8dee9; border-radius: 6px; padding: 14px; }}
    .label {{ color: #52616f; font-size: 12px; text-transform: uppercase; }}
    .value {{ margin-top: 4px; font-size: 18px; font-weight: 700; }}
    .sub {{ color: #52616f; font-size: 12px; }}
    section {{ background: white; border: 1px solid #d8dee9; border-radius: 6px; padding: 16px; margin-bottom: 18px; }}
    .note {{ color: #52616f; font-size: 14px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid #e2e8f0; padding: 8px 10px; text-align: right; }}
    th:first-child, td:first-child, th:nth-child(2), td:nth-child(2) {{ text-align: left; }}
    th {{ color: #52616f; font-weight: 700; white-space: nowrap; }}
    img {{ display: block; width: 100%; height: auto; }}
    .table-wrap {{ overflow-x: auto; }}
  </style>
</head>
<body>
<main>
  <h1>{html.escape(title)}</h1>
  <p class="note">
    Metrics use only station/timestamp samples common to every selected study root.
    Common samples: {len(keys)}.
  </p>
  <div class="cards">{cards}</div>
  <section>
    <h2>Station Metrics</h2>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Station</th>
            <th>Series</th>
            <th>N</th>
            <th>Speed bias</th>
            <th>Speed MAE</th>
            <th>Vector RMSE</th>
            <th>Dir MAE >=5 mph</th>
          </tr>
        </thead>
        <tbody>{table_rows}</tbody>
      </table>
    </div>
  </section>
  {images}
</main>
</body>
</html>
"""
    path.write_text(content, encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Compare completed validation study roots on common station-hours."
    )
    parser.add_argument(
        "study_roots",
        nargs="*",
        help="Validation study roots. Defaults to HRRR berthoud_pass and NBM berthoud_pass_nbm.",
    )
    parser.add_argument(
        "--output-dir",
        default="runtime/validation/model_comparison",
        help="Output directory for comparison CSV, summary, plots, and index.",
    )
    parser.add_argument(
        "--title",
        default="Berthoud Pass HRRR/NBM Validation Comparison",
        help="HTML and plot title.",
    )
    parser.add_argument("--speed-units", default="mph")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    roots = args.study_roots or [
        "runtime/validation/berthoud_pass",
        "runtime/validation/berthoud_pass_nbm",
    ]
    study_roots = [vp.resolve_repo_path(root) for root in roots]
    output_dir = vp.resolve_repo_path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    studies = load_studies(study_roots)
    keys = common_keys(studies)
    metrics = build_metrics(studies, keys)
    write_metrics_csv(output_dir / "metrics.csv", metrics)

    plots = ["speed_mae_by_station.svg", "vector_rmse_by_station.svg"]
    bar_chart(
        output_dir / plots[0],
        metrics,
        field="speed_mae",
        title="Station Speed MAE by Model",
        y_label=f"Speed MAE ({args.speed_units})",
    )
    bar_chart(
        output_dir / plots[1],
        metrics,
        field="vector_rmse",
        title="Station Vector RMSE by Model",
        y_label=f"Vector RMSE ({args.speed_units})",
    )
    write_summary(output_dir / "summary.json", studies, keys, plots)
    write_index(output_dir / "index.html", args.title, metrics, keys, plots)

    print(f"Wrote validation model comparison to {output_dir}")
    print(f"Common samples: {len(keys)} | Stations: {len({station_id for station_id, _ in keys})}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
