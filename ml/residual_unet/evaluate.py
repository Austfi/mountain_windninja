"""Evaluate a residual U-Net checkpoint against WindNinja momentum targets."""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

from .dataset import load_normalization, make_dataloader
from .model_unet import build_unet
from .train import resolve_model_in_channels


VECTOR_ERROR_THRESHOLDS_MPS = (0.5, 1.0, 2.0, 3.0, 5.0, 10.0)
MEANINGFUL_ERROR_DELTA_MPS = 1.0


def _threshold_key(prefix: str, threshold: float, suffix: str) -> str:
    label = str(threshold).replace(".", "p")
    return f"{prefix}_{label}mps_{suffix}"


def _csv_list(values: list[str] | None) -> list[str] | None:
    if not values:
        return None
    parsed: list[str] = []
    for value in values:
        parsed.extend(item.strip() for item in value.split(",") if item.strip())
    return parsed or None


def _require_torch():
    try:
        import torch
    except ImportError as exc:
        raise RuntimeError(
            "PyTorch is required for evaluation. Install ml/residual_unet/requirements.txt."
        ) from exc
    return torch


def _to_device(batch: dict, device):
    return {
        key: value.to(device) if hasattr(value, "to") else value
        for key, value in batch.items()
    }


def _metric_sums(pred_uv, mass_uv, mom_uv, valid_mask) -> dict[str, float]:
    import torch

    mask = valid_mask.bool()
    ml_squared_err = ((pred_uv - mom_uv) ** 2).sum(dim=1)
    mass_squared_err = ((mass_uv - mom_uv) ** 2).sum(dim=1)
    ml_err = torch.sqrt(ml_squared_err + 1e-12)
    mass_err = torch.sqrt(mass_squared_err + 1e-12)
    ml_speed = torch.sqrt((pred_uv ** 2).sum(dim=1) + 1e-6)
    mass_speed = torch.sqrt((mass_uv ** 2).sum(dim=1) + 1e-6)
    mom_speed = torch.sqrt((mom_uv ** 2).sum(dim=1) + 1e-6)
    masked_ml_err = ml_err[mask]
    masked_mass_err = mass_err[mask]
    delta = masked_mass_err - masked_ml_err
    count = int(mask.sum().detach().cpu())
    sums = {
        "count": count,
        "ml_squared_vector_error": float(ml_squared_err[mask].sum().detach().cpu()),
        "mass_squared_vector_error": float(mass_squared_err[mask].sum().detach().cpu()),
        "ml_speed_abs_error": float(torch.abs(ml_speed - mom_speed)[mask].sum().detach().cpu()),
        "mass_speed_abs_error": float(
            torch.abs(mass_speed - mom_speed)[mask].sum().detach().cpu()
        ),
        "ml_better_pixel_count": int((delta > 0.0).sum().detach().cpu()),
        "mass_better_pixel_count": int((delta < 0.0).sum().detach().cpu()),
        "ml_better_by_1mps_pixel_count": int(
            (delta >= MEANINGFUL_ERROR_DELTA_MPS).sum().detach().cpu()
        ),
        "ml_worse_by_1mps_pixel_count": int(
            (delta <= -MEANINGFUL_ERROR_DELTA_MPS).sum().detach().cpu()
        ),
    }
    for threshold in VECTOR_ERROR_THRESHOLDS_MPS:
        sums[_threshold_key("ml_vector_error_le", threshold, "count")] = int(
            (masked_ml_err <= threshold).sum().detach().cpu()
        )
        sums[_threshold_key("mass_vector_error_le", threshold, "count")] = int(
            (masked_mass_err <= threshold).sum().detach().cpu()
        )
    return sums


def _finalize_metrics(totals: dict[str, float]) -> dict[str, float]:
    import math

    count = max(totals["count"], 1)
    mass_rmse = math.sqrt(totals["mass_squared_vector_error"] / count)
    ml_rmse = math.sqrt(totals["ml_squared_vector_error"] / count)
    improvement = 100.0 * (mass_rmse - ml_rmse) / mass_rmse if mass_rmse else 0.0
    metrics = {
        "valid_pixel_count": int(totals["count"]),
        "mass_vector_rmse": mass_rmse,
        "ml_vector_rmse": ml_rmse,
        "vector_rmse_improvement_percent": improvement,
        "mass_speed_mae": totals["mass_speed_abs_error"] / count,
        "ml_speed_mae": totals["ml_speed_abs_error"] / count,
        "ml_better_pixel_count": int(totals.get("ml_better_pixel_count", 0)),
        "mass_better_pixel_count": int(totals.get("mass_better_pixel_count", 0)),
        "ml_better_pixel_fraction": totals.get("ml_better_pixel_count", 0) / count,
        "mass_better_pixel_fraction": totals.get("mass_better_pixel_count", 0) / count,
        "ml_better_by_1mps_pixel_count": int(totals.get("ml_better_by_1mps_pixel_count", 0)),
        "ml_worse_by_1mps_pixel_count": int(totals.get("ml_worse_by_1mps_pixel_count", 0)),
        "ml_better_by_1mps_pixel_fraction": totals.get("ml_better_by_1mps_pixel_count", 0) / count,
        "ml_worse_by_1mps_pixel_fraction": totals.get("ml_worse_by_1mps_pixel_count", 0) / count,
    }
    for threshold in VECTOR_ERROR_THRESHOLDS_MPS:
        ml_count_key = _threshold_key("ml_vector_error_le", threshold, "count")
        mass_count_key = _threshold_key("mass_vector_error_le", threshold, "count")
        ml_fraction_key = _threshold_key("ml_vector_error_le", threshold, "fraction")
        mass_fraction_key = _threshold_key("mass_vector_error_le", threshold, "fraction")
        ml_count = int(totals.get(ml_count_key, 0))
        mass_count = int(totals.get(mass_count_key, 0))
        metrics[ml_count_key] = ml_count
        metrics[mass_count_key] = mass_count
        metrics[ml_fraction_key] = ml_count / count
        metrics[mass_fraction_key] = mass_count / count
    return metrics


def _empty_metric_totals():
    return defaultdict(float)


def _save_example_figure(out_dir: Path, sample_id: str, pred_uv, mass_uv, mom_uv) -> None:
    try:
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        return

    out_dir.mkdir(parents=True, exist_ok=True)
    pred = pred_uv.detach().cpu().numpy()
    mass = mass_uv.detach().cpu().numpy()
    mom = mom_uv.detach().cpu().numpy()
    true_resid = np.sqrt(((mom - mass) ** 2).sum(axis=0))
    pred_resid = np.sqrt(((pred - mass) ** 2).sum(axis=0))
    error_reduction = np.sqrt(((mass - mom) ** 2).sum(axis=0)) - np.sqrt(
        ((pred - mom) ** 2).sum(axis=0)
    )

    fig, axes = plt.subplots(1, 3, figsize=(12, 4), constrained_layout=True)
    for ax, image, title in (
        (axes[0], true_resid, "true residual"),
        (axes[1], pred_resid, "predicted residual"),
        (axes[2], error_reduction, "error reduction"),
    ):
        plot = ax.imshow(image)
        ax.set_title(title)
        ax.set_xticks([])
        ax.set_yticks([])
        fig.colorbar(plot, ax=ax, fraction=0.046, pad=0.04)
    fig.savefig(out_dir / f"{sample_id}.png", dpi=150)
    plt.close(fig)


def evaluate(
    checkpoint_path: Path,
    processed_dir: Path,
    out_dir: Path,
    *,
    split: str = "test",
    batch_size: int = 8,
    source_dataset: str | None = None,
    source_datasets: list[str] | None = None,
    exclude_source_datasets: list[str] | None = None,
    max_samples: int | None = None,
    max_figures: int = 3,
    num_workers: int = 0,
    pin_memory: bool | None = None,
    prefetch_factor: int | None = None,
) -> dict[str, float]:
    torch = _require_torch()
    checkpoint = torch.load(checkpoint_path, map_location="cpu")
    config = checkpoint["config"]
    model_cfg = config["model"]
    normalization = checkpoint.get("normalization") or load_normalization(processed_dir)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = build_unet(
        in_channels=resolve_model_in_channels(model_cfg, normalization),
        out_channels=int(model_cfg.get("out_channels", 2)),
        base_channels=int(model_cfg.get("base_channels", 32)),
    ).to(device)
    model.load_state_dict(checkpoint["model_state_dict"])
    model.eval()

    loader = make_dataloader(
        processed_dir,
        split,
        normalization,
        batch_size=batch_size,
        shuffle=False,
        source_dataset=source_dataset,
        source_datasets=source_datasets,
        exclude_source_datasets=exclude_source_datasets,
        max_samples=max_samples,
        num_workers=num_workers,
        pin_memory=pin_memory,
        prefetch_factor=prefetch_factor,
    )
    print(
        f"device={device} batch_size={batch_size} num_workers={num_workers} "
        f"samples={len(loader.dataset)} batches={len(loader)}",
        flush=True,
    )
    totals = _empty_metric_totals()
    rows = []
    figure_count = 0
    out_dir.mkdir(parents=True, exist_ok=True)

    with torch.no_grad():
        for batch in loader:
            batch = _to_device(batch, device)
            pred_uv = batch["mass_uv"] + model(batch["x"])
            sums = _metric_sums(pred_uv, batch["mass_uv"], batch["mom_uv"], batch["valid_mask"])
            for key, value in sums.items():
                totals[key] += value

            for item_index, sample_id in enumerate(batch["sample_id"]):
                item_sums = _metric_sums(
                    pred_uv[item_index:item_index + 1],
                    batch["mass_uv"][item_index:item_index + 1],
                    batch["mom_uv"][item_index:item_index + 1],
                    batch["valid_mask"][item_index:item_index + 1],
                )
                item_metrics = _finalize_metrics(item_sums)
                source = batch.get("source_dataset", [""])
                rows.append({
                    "sample_id": sample_id,
                    "source_dataset": source[item_index] if source else "",
                    **item_metrics,
                })
                if figure_count < max_figures:
                    _save_example_figure(
                        out_dir / "figures",
                        sample_id,
                        pred_uv[item_index],
                        batch["mass_uv"][item_index],
                        batch["mom_uv"][item_index],
                    )
                    figure_count += 1

    metrics = _finalize_metrics(totals)
    (out_dir / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
    with (out_dir / "sample_metrics.csv").open("w", encoding="utf-8", newline="") as f:
        fieldnames = list(rows[0]) if rows else ["sample_id"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    return metrics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate a residual U-Net checkpoint.")
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--data", required=True, help="Processed dataset directory.")
    parser.add_argument("--out", required=True, help="Evaluation output directory.")
    parser.add_argument("--split", default="test", choices=["train", "val", "test"])
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--num-workers", type=int, default=0)
    parser.add_argument("--prefetch-factor", type=int)
    parser.add_argument("--pin-memory", action="store_true")
    parser.add_argument("--no-pin-memory", action="store_true")
    parser.add_argument(
        "--source-dataset",
        action="append",
        help="Optional source_dataset manifest filter. Repeat or comma-separate values.",
    )
    parser.add_argument(
        "--exclude-source-dataset",
        action="append",
        help="Optional source_dataset exclusion filter. Repeat or comma-separate values.",
    )
    parser.add_argument("--max-samples", type=int)
    parser.add_argument("--max-figures", type=int, default=3)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    metrics = evaluate(
        Path(args.checkpoint),
        Path(args.data),
        Path(args.out),
        split=args.split,
        batch_size=args.batch_size,
        source_datasets=_csv_list(args.source_dataset),
        exclude_source_datasets=_csv_list(args.exclude_source_dataset),
        max_samples=args.max_samples,
        max_figures=args.max_figures,
        num_workers=args.num_workers,
        pin_memory=False if args.no_pin_memory else (True if args.pin_memory else None),
        prefetch_factor=args.prefetch_factor,
    )
    print(json.dumps(metrics, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
