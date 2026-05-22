"""Analyze residual U-Net training/evaluation artifacts."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Iterable


def read_csv_rows(path: str | Path) -> list[dict]:
    with Path(path).open("r", encoding="utf-8", newline="") as f:
        rows = []
        for row in csv.DictReader(f):
            converted = {}
            for key, value in row.items():
                if key == "sample_id":
                    converted[key] = value
                    continue
                try:
                    converted[key] = float(value)
                except (TypeError, ValueError):
                    converted[key] = value
            rows.append(converted)
    return rows


def quantile(values: Iterable[float], fraction: float) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        raise ValueError("Cannot compute quantile of an empty sequence.")
    if fraction <= 0:
        return ordered[0]
    if fraction >= 1:
        return ordered[-1]
    index = round((len(ordered) - 1) * fraction)
    return ordered[index]


def metric_distribution(rows: list[dict], field: str) -> dict[str, float]:
    values = [float(row[field]) for row in rows]
    return {
        "min": min(values),
        "p25": quantile(values, 0.25),
        "median": quantile(values, 0.5),
        "p75": quantile(values, 0.75),
        "max": max(values),
    }


def summarize_training(rows: list[dict]) -> dict:
    if not rows:
        raise ValueError("Training log has no rows.")
    best = min(rows, key=lambda row: float(row["val_ml_vector_rmse"]))
    final = rows[-1]
    return {
        "epochs": int(final["epoch"]),
        "best_epoch": int(best["epoch"]),
        "best_val_vector_rmse": float(best["val_ml_vector_rmse"]),
        "best_val_loss": float(best["val_loss"]),
        "final_train_vector_rmse": float(final["train_ml_vector_rmse"]),
        "final_val_vector_rmse": float(final["val_ml_vector_rmse"]),
        "final_train_loss": float(final["train_loss"]),
        "final_val_loss": float(final["val_loss"]),
        "final_val_mass_vector_rmse": float(final["val_mass_vector_rmse"]),
    }


def summarize_samples(rows: list[dict], *, top_n: int = 5) -> dict:
    if not rows:
        raise ValueError("Sample metrics file has no rows.")
    improved = [
        row for row in rows
        if float(row["ml_vector_rmse"]) < float(row["mass_vector_rmse"])
    ]
    fields = [
        "mass_vector_rmse",
        "ml_vector_rmse",
        "vector_rmse_improvement_percent",
        "mass_speed_mae",
        "ml_speed_mae",
    ]
    worst_ml = sorted(rows, key=lambda row: float(row["ml_vector_rmse"]), reverse=True)[:top_n]
    worst_improvement = sorted(
        rows,
        key=lambda row: float(row["vector_rmse_improvement_percent"]),
    )[:top_n]
    return {
        "sample_count": len(rows),
        "improved_sample_count": len(improved),
        "improved_sample_percent": 100.0 * len(improved) / len(rows),
        "distributions": {
            field: metric_distribution(rows, field)
            for field in fields
        },
        "worst_ml_vector_rmse": worst_ml,
        "worst_improvement_percent": worst_improvement,
    }


def load_metrics(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _fmt(value: float, digits: int = 3) -> str:
    return f"{float(value):.{digits}f}"


def write_report(path: Path, summary: dict) -> None:
    metrics = summary["overall_metrics"]
    training = summary["training"]
    samples = summary["samples"]
    dist = samples["distributions"]
    lines = [
        "# Residual U-Net Colab Result",
        "",
        "## Overall Metrics",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Mass vector RMSE | {_fmt(metrics['mass_vector_rmse'])} m/s |",
        f"| ML vector RMSE | {_fmt(metrics['ml_vector_rmse'])} m/s |",
        f"| Vector RMSE improvement | {_fmt(metrics['vector_rmse_improvement_percent'], 1)}% |",
        f"| Mass speed MAE | {_fmt(metrics['mass_speed_mae'])} m/s |",
        f"| ML speed MAE | {_fmt(metrics['ml_speed_mae'])} m/s |",
        f"| Valid test pixels | {int(metrics['valid_pixel_count'])} |",
        "",
        "## Training Behavior",
        "",
        f"- Epochs completed: {training['epochs']}",
        f"- Best validation epoch: {training['best_epoch']}",
        f"- Best validation vector RMSE: {_fmt(training['best_val_vector_rmse'])} m/s",
        f"- Final train vector RMSE: {_fmt(training['final_train_vector_rmse'])} m/s",
        f"- Final validation vector RMSE: {_fmt(training['final_val_vector_rmse'])} m/s",
        f"- Validation mass-solver baseline RMSE: "
        f"{_fmt(training['final_val_mass_vector_rmse'])} m/s",
        "",
        "## Test Sample Spread",
        "",
        f"- Improved samples: {samples['improved_sample_count']} / "
        f"{samples['sample_count']} "
        f"({_fmt(samples['improved_sample_percent'], 1)}%)",
        f"- ML vector RMSE median: {_fmt(dist['ml_vector_rmse']['median'])} m/s",
        f"- ML vector RMSE max: {_fmt(dist['ml_vector_rmse']['max'])} m/s",
        f"- Improvement median: "
        f"{_fmt(dist['vector_rmse_improvement_percent']['median'], 1)}%",
        f"- Worst improvement: "
        f"{_fmt(dist['vector_rmse_improvement_percent']['min'], 1)}%",
        "",
        "## Worst ML RMSE Samples",
        "",
        "| Sample | ML RMSE | Mass RMSE | Improvement |",
        "|---|---:|---:|---:|",
    ]
    for row in samples["worst_ml_vector_rmse"]:
        lines.append(
            f"| {row['sample_id']} | {_fmt(row['ml_vector_rmse'])} | "
            f"{_fmt(row['mass_vector_rmse'])} | "
            f"{_fmt(row['vector_rmse_improvement_percent'], 1)}% |"
        )

    lines += [
        "",
        "## Interpretation",
        "",
        "- The model strongly emulates WindNinja momentum output on this held-out test split.",
        "- Train and validation RMSE are close, so this run does not show obvious overfitting.",
        "- The result is still same-terrain, same-season emulation; it should not be treated as "
        "real-world wind validation.",
        "- Next research check: use a harder split or controlled mass/momentum paired runs to "
        "test whether the correction generalizes outside nearby HRRR cases.",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def write_plots(out_dir: Path, train_rows: list[dict], sample_rows: list[dict]) -> list[str]:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return []

    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []

    epochs = [int(row["epoch"]) for row in train_rows]
    fig, axes = plt.subplots(1, 2, figsize=(10, 4), constrained_layout=True)
    axes[0].plot(epochs, [row["train_ml_vector_rmse"] for row in train_rows], label="train ML")
    axes[0].plot(epochs, [row["val_ml_vector_rmse"] for row in train_rows], label="val ML")
    axes[0].plot(epochs, [row["val_mass_vector_rmse"] for row in train_rows], label="val mass")
    axes[0].set_title("Vector RMSE")
    axes[0].set_xlabel("epoch")
    axes[0].set_ylabel("m/s")
    axes[0].legend()
    axes[1].plot(epochs, [row["train_loss"] for row in train_rows], label="train")
    axes[1].plot(epochs, [row["val_loss"] for row in train_rows], label="val")
    axes[1].set_title("Loss")
    axes[1].set_xlabel("epoch")
    axes[1].legend()
    path = out_dir / "training_curves.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    paths.append(path.as_posix())

    fig, axes = plt.subplots(1, 2, figsize=(10, 4), constrained_layout=True)
    axes[0].hist([row["ml_vector_rmse"] for row in sample_rows], bins=24)
    axes[0].set_title("ML Vector RMSE")
    axes[0].set_xlabel("m/s")
    axes[0].set_ylabel("sample count")
    axes[1].hist([row["vector_rmse_improvement_percent"] for row in sample_rows], bins=24)
    axes[1].set_title("Improvement")
    axes[1].set_xlabel("percent")
    path = out_dir / "sample_distributions.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    paths.append(path.as_posix())

    return paths


def analyze_run(run_dir: Path, out_dir: Path, *, top_n: int = 5) -> dict:
    metrics = load_metrics(run_dir / "metrics.json")
    train_rows = read_csv_rows(run_dir / "train_log.csv")
    sample_rows = read_csv_rows(run_dir / "sample_metrics.csv")
    summary = {
        "overall_metrics": metrics,
        "training": summarize_training(train_rows),
        "samples": summarize_samples(sample_rows, top_n=top_n),
    }
    plot_paths = write_plots(out_dir / "plots", train_rows, sample_rows)
    summary["plots"] = plot_paths
    write_json(out_dir / "analysis_summary.json", summary)
    write_report(out_dir / "analysis_report.md", summary)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Analyze residual U-Net run artifacts.")
    parser.add_argument("--run-dir", required=True, help="Folder containing train/eval artifacts.")
    parser.add_argument("--out", required=True, help="Output analysis folder.")
    parser.add_argument("--top-n", type=int, default=5)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    summary = analyze_run(Path(args.run_dir), Path(args.out), top_n=args.top_n)
    print(json.dumps({
        "analysis_summary": str(Path(args.out) / "analysis_summary.json"),
        "analysis_report": str(Path(args.out) / "analysis_report.md"),
        "ml_vector_rmse": summary["overall_metrics"]["ml_vector_rmse"],
        "improvement_percent": summary["overall_metrics"]["vector_rmse_improvement_percent"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

