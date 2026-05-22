"""Compare residual U-Net Colab result directories across model runs."""
from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any

from .analyze_results import read_csv_rows, summarize_training


PRIMARY_FIELDS = [
    "run_name",
    "run_kind",
    "source_dataset",
    "source_key",
    "domain",
    "source_kind",
    "valid_pixel_count",
    "mass_vector_rmse",
    "ml_vector_rmse",
    "vector_rmse_improvement_percent",
    "mass_speed_mae",
    "ml_speed_mae",
    "ml_better_pixel_fraction",
    "mass_better_pixel_fraction",
    "ml_better_by_1mps_pixel_fraction",
    "ml_worse_by_1mps_pixel_fraction",
    "ml_vector_error_le_1p0mps_fraction",
    "ml_vector_error_le_2p0mps_fraction",
    "ml_vector_error_le_3p0mps_fraction",
    "ml_vector_error_le_5p0mps_fraction",
    "best_epoch",
    "best_val_vector_rmse",
    "metrics_path",
]


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _run_kind(run_name: str) -> str:
    if "_holdout_" in run_name:
        return "terrain_holdout"
    if "mountain_general" in run_name:
        return "all_domain"
    return "other"


def _source_kind(source_dataset: str) -> str:
    if "_controlled" in source_dataset:
        return "controlled"
    if "_hrrr" in source_dataset:
        return "hrrr"
    return "other"


def _source_domain(source_dataset: str) -> str:
    for marker in ("_hrrr", "_controlled"):
        if marker in source_dataset:
            return source_dataset.split(marker, 1)[0]
    return source_dataset


def _source_key(source_dataset: str) -> str:
    return f"{_source_domain(source_dataset)}:{_source_kind(source_dataset)}"


def _iter_metric_paths(run_dir: Path) -> list[tuple[str, Path]]:
    eval_dir = run_dir / "eval"
    paths: list[tuple[str, Path]] = []
    if eval_dir.exists():
        for metrics_path in sorted(eval_dir.glob("*/metrics.json")):
            paths.append((metrics_path.parent.name, metrics_path))
    top_level = run_dir / "metrics.json"
    if top_level.exists():
        paths.append((run_dir.name, top_level))
    return paths


def _training_summary(run_dir: Path) -> dict[str, Any]:
    path = run_dir / "train_log.csv"
    if not path.exists():
        return {}
    try:
        return summarize_training(read_csv_rows(path))
    except (KeyError, ValueError):
        return {}


def collect_result_rows(results_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for run_dir in sorted(path for path in results_root.iterdir() if path.is_dir()):
        if run_dir.name.startswith("_"):
            continue
        metric_paths = _iter_metric_paths(run_dir)
        if not metric_paths:
            continue
        training = _training_summary(run_dir)
        for source_dataset, metrics_path in metric_paths:
            metrics = _read_json(metrics_path)
            row = {
                "run_name": run_dir.name,
                "run_kind": _run_kind(run_dir.name),
                "source_dataset": source_dataset,
                "source_key": _source_key(source_dataset),
                "domain": _source_domain(source_dataset),
                "source_kind": _source_kind(source_dataset),
                "best_epoch": training.get("best_epoch", ""),
                "best_val_vector_rmse": training.get("best_val_vector_rmse", ""),
                "metrics_path": metrics_path.as_posix(),
            }
            row.update(metrics)
            rows.append(row)
    return rows


def _weighted_run_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries = []
    by_run: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_run.setdefault(str(row["run_name"]), []).append(row)

    for run_name, run_rows in sorted(by_run.items()):
        count = sum(int(row.get("valid_pixel_count", 0) or 0) for row in run_rows)
        if count <= 0:
            continue
        mass_sse = sum(
            float(row.get("mass_vector_rmse", 0.0)) ** 2
            * int(row.get("valid_pixel_count", 0) or 0)
            for row in run_rows
        )
        ml_sse = sum(
            float(row.get("ml_vector_rmse", 0.0)) ** 2
            * int(row.get("valid_pixel_count", 0) or 0)
            for row in run_rows
        )
        mass_rmse = math.sqrt(mass_sse / count)
        ml_rmse = math.sqrt(ml_sse / count)
        summary = {
            "run_name": run_name,
            "source_count": len(run_rows),
            "valid_pixel_count": count,
            "mass_vector_rmse": mass_rmse,
            "ml_vector_rmse": ml_rmse,
            "vector_rmse_improvement_percent": (
                100.0 * (mass_rmse - ml_rmse) / mass_rmse if mass_rmse else 0.0
            ),
            "mass_speed_mae": sum(
                float(row.get("mass_speed_mae", 0.0)) * int(row.get("valid_pixel_count", 0) or 0)
                for row in run_rows
            )
            / count,
            "ml_speed_mae": sum(
                float(row.get("ml_speed_mae", 0.0)) * int(row.get("valid_pixel_count", 0) or 0)
                for row in run_rows
            )
            / count,
        }
        for field in (
            "ml_better_pixel_count",
            "mass_better_pixel_count",
            "ml_better_by_1mps_pixel_count",
            "ml_worse_by_1mps_pixel_count",
            "ml_vector_error_le_1p0mps_count",
            "ml_vector_error_le_2p0mps_count",
            "ml_vector_error_le_3p0mps_count",
            "ml_vector_error_le_5p0mps_count",
        ):
            if any(field in row for row in run_rows):
                total = sum(int(row.get(field, 0) or 0) for row in run_rows)
                summary[field] = total
                summary[field.replace("_count", "_fraction")] = total / count
        summaries.append(summary)
    return summaries


def _best_by_source(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best = []
    by_source: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_source.setdefault(str(row["source_key"]), []).append(row)
    for source_key, source_rows in sorted(by_source.items()):
        winner = min(source_rows, key=lambda row: float(row.get("ml_vector_rmse", float("inf"))))
        best.append({
            "source_key": source_key,
            "best_run_name": winner["run_name"],
            "source_dataset": winner["source_dataset"],
            "ml_vector_rmse": winner.get("ml_vector_rmse", ""),
            "mass_vector_rmse": winner.get("mass_vector_rmse", ""),
            "vector_rmse_improvement_percent": winner.get(
                "vector_rmse_improvement_percent", ""
            ),
            "ml_better_pixel_fraction": winner.get("ml_better_pixel_fraction", ""),
        })
    return best


def _fieldnames(rows: list[dict[str, Any]]) -> list[str]:
    extras = sorted({key for row in rows for key in row if key not in PRIMARY_FIELDS})
    return PRIMARY_FIELDS + extras


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = _fieldnames(rows) if rows else PRIMARY_FIELDS
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _fmt(value: Any, digits: int = 3) -> str:
    if value == "":
        return ""
    return f"{float(value):.{digits}f}"


def _write_report(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Residual U-Net Model Comparison",
        "",
        "## Run Summary",
        "",
        "| Run | Sources | Mass RMSE | ML RMSE | Improvement | ML Better Pixels |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for row in summary["run_summaries"]:
        lines.append(
            f"| {row['run_name']} | {row['source_count']} | "
            f"{_fmt(row['mass_vector_rmse'])} | {_fmt(row['ml_vector_rmse'])} | "
            f"{_fmt(row['vector_rmse_improvement_percent'], 1)}% | "
            f"{_fmt(row.get('ml_better_pixel_fraction', ''), 3)} |"
        )

    lines += [
        "",
        "## Best Run By Source",
        "",
        "| Source | Best Run | Source Dataset | ML RMSE | Improvement | ML Better Pixels |",
        "|---|---|---|---:|---:|---:|",
    ]
    for row in summary["best_by_source"]:
        lines.append(
            f"| {row['source_key']} | {row['best_run_name']} | {row['source_dataset']} | "
            f"{_fmt(row['ml_vector_rmse'])} | "
            f"{_fmt(row['vector_rmse_improvement_percent'], 1)}% | "
            f"{_fmt(row.get('ml_better_pixel_fraction', ''), 3)} |"
        )
    lines += [
        "",
        "## Notes",
        "",
        "- Aggregate run RMSE is weighted by valid pixel count across source evaluations.",
        "- Source comparisons are grouped by domain and source kind, so HRRR and controlled "
        "stress tests stay separate.",
        "- Pixel fractions are blank for older evaluations that have not been rerun with the "
        "new pixel-level evaluator.",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def compare_results(results_root: Path, out_dir: Path) -> dict[str, Any]:
    rows = collect_result_rows(results_root)
    summary = {
        "results_root": results_root.as_posix(),
        "run_summaries": _weighted_run_summary(rows),
        "best_by_source": _best_by_source(rows),
        "rows": rows,
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(out_dir / "comparison_metrics.csv", rows)
    _write_csv(out_dir / "comparison_run_summary.csv", summary["run_summaries"])
    _write_json(out_dir / "comparison_summary.json", summary)
    _write_report(out_dir / "comparison_report.md", summary)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compare residual U-Net Colab result folders.")
    parser.add_argument("--results-root", required=True)
    parser.add_argument("--out", required=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    summary = compare_results(Path(args.results_root), Path(args.out))
    print(json.dumps({
        "result_rows": len(summary["rows"]),
        "run_count": len(summary["run_summaries"]),
        "comparison_report": str(Path(args.out) / "comparison_report.md"),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
