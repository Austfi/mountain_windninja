"""Train the Berthoud residual U-Net."""
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from .config import apply_overrides, load_config
from .dataset import load_normalization, make_dataloader
from .losses import corrected_loss, speed_mae_torch, vector_rmse_torch
from .model_unet import build_unet


def _require_torch():
    try:
        import torch
    except ImportError as exc:
        raise RuntimeError(
            "PyTorch is required for training. Install ml/residual_unet/requirements.txt."
        ) from exc
    return torch


def _to_device(batch: dict, device):
    return {
        key: value.to(device) if hasattr(value, "to") else value
        for key, value in batch.items()
    }


def run_epoch(model, loader, device, *, optimizer=None, speed_weight: float = 0.1) -> dict[str, float]:
    train_mode = optimizer is not None
    model.train(train_mode)
    totals = {
        "loss": 0.0,
        "vec_loss": 0.0,
        "speed_loss": 0.0,
        "ml_vector_rmse": 0.0,
        "mass_vector_rmse": 0.0,
        "ml_speed_mae": 0.0,
        "count": 0,
    }

    torch = _require_torch()
    context = torch.enable_grad() if train_mode else torch.no_grad()
    with context:
        for batch in loader:
            batch = _to_device(batch, device)
            pred_delta = model(batch["x"])
            loss, parts = corrected_loss(
                pred_delta,
                batch["mass_uv"],
                batch["mom_uv"],
                batch["valid_mask"],
                speed_weight=speed_weight,
            )
            if train_mode:
                optimizer.zero_grad(set_to_none=True)
                loss.backward()
                optimizer.step()

            batch_size = int(batch["x"].shape[0])
            pred_uv = batch["mass_uv"] + pred_delta
            totals["loss"] += float(loss.detach().cpu()) * batch_size
            totals["vec_loss"] += parts["vec_loss"] * batch_size
            totals["speed_loss"] += parts["speed_loss"] * batch_size
            totals["ml_vector_rmse"] += vector_rmse_torch(
                pred_uv,
                batch["mom_uv"],
                batch["valid_mask"],
            ) * batch_size
            totals["mass_vector_rmse"] += vector_rmse_torch(
                batch["mass_uv"],
                batch["mom_uv"],
                batch["valid_mask"],
            ) * batch_size
            totals["ml_speed_mae"] += speed_mae_torch(
                pred_uv,
                batch["mom_uv"],
                batch["valid_mask"],
            ) * batch_size
            totals["count"] += batch_size

    count = max(totals.pop("count"), 1)
    return {key: value / count for key, value in totals.items()}


def save_checkpoint(path: Path, payload: dict) -> None:
    torch = _require_torch()
    path.parent.mkdir(parents=True, exist_ok=True)
    torch.save(payload, path)


def append_log(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row))
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def train(config: dict, *, resume: Path | None = None) -> dict:
    torch = _require_torch()
    data_cfg = config["data"]
    model_cfg = config["model"]
    train_cfg = config["training"]

    processed_dir = Path(data_cfg["processed_dir"])
    normalization = load_normalization(processed_dir)
    batch_size = int(data_cfg.get("batch_size", 8))
    num_workers = int(data_cfg.get("num_workers", 0))
    max_train_samples = data_cfg.get("max_train_samples")
    max_val_samples = data_cfg.get("max_val_samples")

    train_loader = make_dataloader(
        processed_dir,
        "train",
        normalization,
        batch_size=batch_size,
        num_workers=num_workers,
        shuffle=True,
        max_samples=max_train_samples,
    )
    val_loader = make_dataloader(
        processed_dir,
        "val",
        normalization,
        batch_size=batch_size,
        num_workers=num_workers,
        shuffle=False,
        max_samples=max_val_samples,
    )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = build_unet(
        in_channels=int(model_cfg.get("in_channels", 5)),
        out_channels=int(model_cfg.get("out_channels", 2)),
        base_channels=int(model_cfg.get("base_channels", 32)),
    ).to(device)
    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=float(train_cfg.get("learning_rate", 1e-3)),
        weight_decay=float(train_cfg.get("weight_decay", 1e-6)),
    )

    start_epoch = 1
    best_val_loss = float("inf")
    if resume:
        checkpoint = torch.load(resume, map_location=device)
        model.load_state_dict(checkpoint["model_state_dict"])
        optimizer.load_state_dict(checkpoint["optimizer_state_dict"])
        start_epoch = int(checkpoint["epoch"]) + 1
        best_val_loss = float(checkpoint.get("best_val_loss", best_val_loss))

    checkpoint_dir = Path(train_cfg["checkpoint_dir"])
    log_csv = Path(train_cfg["log_csv"])
    epochs = int(train_cfg.get("epochs", 30))
    speed_weight = float(train_cfg.get("speed_loss_weight", 0.1))

    latest_metrics = {}
    for epoch in range(start_epoch, epochs + 1):
        train_metrics = run_epoch(
            model,
            train_loader,
            device,
            optimizer=optimizer,
            speed_weight=speed_weight,
        )
        val_metrics = run_epoch(model, val_loader, device, speed_weight=speed_weight)
        is_best = val_metrics["loss"] < best_val_loss
        if is_best:
            best_val_loss = val_metrics["loss"]

        row = {
            "epoch": epoch,
            **{f"train_{key}": value for key, value in train_metrics.items()},
            **{f"val_{key}": value for key, value in val_metrics.items()},
        }
        append_log(log_csv, row)
        payload = {
            "epoch": epoch,
            "best_val_loss": best_val_loss,
            "model_state_dict": model.state_dict(),
            "optimizer_state_dict": optimizer.state_dict(),
            "normalization": normalization,
            "config": config,
            "train_metrics": train_metrics,
            "val_metrics": val_metrics,
        }
        save_checkpoint(checkpoint_dir / "latest.pt", payload)
        if is_best:
            save_checkpoint(checkpoint_dir / "best.pt", payload)
        latest_metrics = row
        print(
            f"epoch={epoch} train_loss={train_metrics['loss']:.4f} "
            f"val_loss={val_metrics['loss']:.4f} "
            f"val_rmse={val_metrics['ml_vector_rmse']:.4f}"
        )

    return latest_metrics


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train the Berthoud residual U-Net.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--resume", help="Optional checkpoint to resume from.")
    parser.add_argument("--data", help="Override processed dataset directory.")
    parser.add_argument("--checkpoint-dir", help="Override checkpoint directory.")
    parser.add_argument("--log-csv", help="Override training log CSV path.")
    parser.add_argument("--epochs", type=int, help="Override configured epoch count.")
    parser.add_argument("--batch-size", type=int, help="Override configured batch size.")
    parser.add_argument("--max-train-samples", type=int)
    parser.add_argument("--max-val-samples", type=int)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    overrides = {}
    if args.data is not None:
        overrides["data.processed_dir"] = args.data
    if args.checkpoint_dir is not None:
        overrides["training.checkpoint_dir"] = args.checkpoint_dir
    if args.log_csv is not None:
        overrides["training.log_csv"] = args.log_csv
    if args.epochs is not None:
        overrides["training.epochs"] = args.epochs
    if args.batch_size is not None:
        overrides["data.batch_size"] = args.batch_size
    if args.max_train_samples is not None:
        overrides["data.max_train_samples"] = args.max_train_samples
    if args.max_val_samples is not None:
        overrides["data.max_val_samples"] = args.max_val_samples

    config = apply_overrides(load_config(args.config), overrides)
    train(config, resume=Path(args.resume) if args.resume else None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
