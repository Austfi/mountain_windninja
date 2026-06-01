"""Train the Berthoud residual U-Net."""
from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path

from .config import apply_overrides, load_config
from .dataset import load_normalization, make_dataloader
from .losses import corrected_loss, speed_mae_torch, vector_rmse_torch
from .model_unet import build_unet


def _csv_list(value) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    items = [item.strip() for item in str(value).split(",")]
    return [item for item in items if item]


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


def run_epoch(
    model,
    loader,
    device,
    *,
    optimizer=None,
    speed_weight: float = 0.1,
    gradient_weight: float = 0.0,
    phase: str,
    progress_every: int = 0,
) -> dict[str, float]:
    train_mode = optimizer is not None
    model.train(train_mode)
    totals = {
        "loss": 0.0,
        "vec_loss": 0.0,
        "speed_loss": 0.0,
        "gradient_loss": 0.0,
        "ml_vector_rmse": 0.0,
        "mass_vector_rmse": 0.0,
        "ml_speed_mae": 0.0,
        "count": 0,
    }

    torch = _require_torch()
    context = torch.enable_grad() if train_mode else torch.no_grad()
    total_batches = len(loader)
    started = time.monotonic()
    with context:
        for batch_idx, batch in enumerate(loader, start=1):
            batch = _to_device(batch, device)
            pred_delta = model(batch["x"])
            loss, parts = corrected_loss(
                pred_delta,
                batch["mass_uv"],
                batch["mom_uv"],
                batch["valid_mask"],
                speed_weight=speed_weight,
                gradient_weight=gradient_weight,
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
            totals["gradient_loss"] += parts["gradient_loss"] * batch_size
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
            if progress_every > 0 and (
                batch_idx == 1 or batch_idx % progress_every == 0 or batch_idx == total_batches
            ):
                elapsed_min = (time.monotonic() - started) / 60.0
                print(
                    f"{phase} batch={batch_idx}/{total_batches} "
                    f"elapsed_min={elapsed_min:.1f}",
                    flush=True,
                )

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


def resolve_model_in_channels(model_cfg: dict, normalization: dict) -> int:
    input_channels = normalization.get("input_channels")
    if not isinstance(input_channels, list) or not input_channels:
        raise ValueError("normalization.json must include non-empty input_channels.")
    expected = len(input_channels)
    configured = model_cfg.get("in_channels")
    if configured is not None and int(configured) != expected:
        raise ValueError(
            f"model.in_channels={configured} does not match dataset input_channels "
            f"length {expected}: {input_channels}"
        )
    return expected


def train(config: dict, *, resume: Path | None = None) -> dict:
    torch = _require_torch()
    data_cfg = config["data"]
    model_cfg = config["model"]
    train_cfg = config["training"]

    processed_dir = Path(data_cfg["processed_dir"])
    normalization = load_normalization(processed_dir)
    batch_size = int(data_cfg.get("batch_size", 8))
    num_workers = int(data_cfg.get("num_workers", 0))
    pin_memory = data_cfg.get("pin_memory")
    persistent_workers = data_cfg.get("persistent_workers")
    prefetch_factor = data_cfg.get("prefetch_factor")
    max_train_samples = data_cfg.get("max_train_samples")
    max_val_samples = data_cfg.get("max_val_samples")
    train_source_datasets = _csv_list(data_cfg.get("train_source_datasets"))
    train_exclude_source_datasets = _csv_list(data_cfg.get("train_exclude_source_datasets"))
    val_source_datasets = _csv_list(data_cfg.get("val_source_datasets"))
    val_exclude_source_datasets = _csv_list(data_cfg.get("val_exclude_source_datasets"))

    train_loader = make_dataloader(
        processed_dir,
        "train",
        normalization,
        batch_size=batch_size,
        num_workers=num_workers,
        shuffle=True,
        source_datasets=train_source_datasets,
        exclude_source_datasets=train_exclude_source_datasets,
        max_samples=max_train_samples,
        pin_memory=pin_memory,
        persistent_workers=persistent_workers,
        prefetch_factor=prefetch_factor,
    )
    val_loader = make_dataloader(
        processed_dir,
        "val",
        normalization,
        batch_size=batch_size,
        num_workers=num_workers,
        shuffle=False,
        source_datasets=val_source_datasets,
        exclude_source_datasets=val_exclude_source_datasets,
        max_samples=max_val_samples,
        pin_memory=pin_memory,
        persistent_workers=persistent_workers,
        prefetch_factor=prefetch_factor,
    )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    if device.type == "cuda":
        device_name = torch.cuda.get_device_name(0)
    else:
        device_name = "cpu"
    print(
        f"device={device} device_name={device_name} "
        f"batch_size={batch_size} num_workers={num_workers} "
        f"train_samples={len(train_loader.dataset)} train_batches={len(train_loader)} "
        f"val_samples={len(val_loader.dataset)} val_batches={len(val_loader)}",
        flush=True,
    )
    model = build_unet(
        in_channels=resolve_model_in_channels(model_cfg, normalization),
        out_channels=int(model_cfg.get("out_channels", 2)),
        base_channels=int(model_cfg.get("base_channels", 32)),
        block_type=str(model_cfg.get("block_type", "conv")),
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
        print(
            f"resume={resume} start_epoch={start_epoch} best_val_loss={best_val_loss:.4f}",
            flush=True,
        )

    checkpoint_dir = Path(train_cfg["checkpoint_dir"])
    log_csv = Path(train_cfg["log_csv"])
    epochs = int(train_cfg.get("epochs", 30))
    speed_weight = float(train_cfg.get("speed_loss_weight", 0.1))
    gradient_weight = float(train_cfg.get("gradient_loss_weight", 0.0))
    progress_every = int(train_cfg.get("progress_every", 0))

    latest_metrics = {}
    for epoch in range(start_epoch, epochs + 1):
        epoch_started = time.monotonic()
        print(f"epoch={epoch}/{epochs} start", flush=True)
        train_metrics = run_epoch(
            model,
            train_loader,
            device,
            optimizer=optimizer,
            speed_weight=speed_weight,
            gradient_weight=gradient_weight,
            phase="train",
            progress_every=progress_every,
        )
        val_metrics = run_epoch(
            model,
            val_loader,
            device,
            speed_weight=speed_weight,
            gradient_weight=gradient_weight,
            phase="val",
            progress_every=progress_every,
        )
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
        elapsed_min = (time.monotonic() - epoch_started) / 60.0
        print(
            f"epoch={epoch} train_loss={train_metrics['loss']:.4f} "
            f"val_loss={val_metrics['loss']:.4f} "
            f"val_rmse={val_metrics['ml_vector_rmse']:.4f} "
            f"elapsed_min={elapsed_min:.1f}",
            flush=True,
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
    parser.add_argument(
        "--base-channels",
        type=int,
        help="Override configured U-Net base channel count.",
    )
    parser.add_argument(
        "--model-block-type",
        choices=("conv", "residual"),
        help="Override U-Net block type.",
    )
    parser.add_argument("--gradient-loss-weight", type=float, help="Override spatial gradient loss weight.")
    parser.add_argument("--batch-size", type=int, help="Override configured batch size.")
    parser.add_argument("--num-workers", type=int, help="Override configured DataLoader workers.")
    parser.add_argument("--progress-every", type=int, help="Print every N batches within each phase.")
    parser.add_argument("--prefetch-factor", type=int, help="DataLoader prefetch factor when workers > 0.")
    parser.add_argument("--pin-memory", action="store_true", help="Force DataLoader pinned memory.")
    parser.add_argument("--no-pin-memory", action="store_true", help="Disable DataLoader pinned memory.")
    parser.add_argument("--max-train-samples", type=int)
    parser.add_argument("--max-val-samples", type=int)
    parser.add_argument("--train-source-datasets", help="Comma-separated source_dataset allow-list for train.")
    parser.add_argument("--train-exclude-source-datasets", help="Comma-separated source_dataset block-list for train.")
    parser.add_argument("--val-source-datasets", help="Comma-separated source_dataset allow-list for validation.")
    parser.add_argument("--val-exclude-source-datasets", help="Comma-separated source_dataset block-list for validation.")
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
    if args.base_channels is not None:
        overrides["model.base_channels"] = args.base_channels
    if args.model_block_type is not None:
        overrides["model.block_type"] = args.model_block_type
    if args.gradient_loss_weight is not None:
        overrides["training.gradient_loss_weight"] = args.gradient_loss_weight
    if args.batch_size is not None:
        overrides["data.batch_size"] = args.batch_size
    if args.num_workers is not None:
        overrides["data.num_workers"] = args.num_workers
    if args.prefetch_factor is not None:
        overrides["data.prefetch_factor"] = args.prefetch_factor
    if args.pin_memory:
        overrides["data.pin_memory"] = True
    if args.no_pin_memory:
        overrides["data.pin_memory"] = False
    if args.progress_every is not None:
        overrides["training.progress_every"] = args.progress_every
    if args.max_train_samples is not None:
        overrides["data.max_train_samples"] = args.max_train_samples
    if args.max_val_samples is not None:
        overrides["data.max_val_samples"] = args.max_val_samples
    if args.train_source_datasets is not None:
        overrides["data.train_source_datasets"] = args.train_source_datasets
    if args.train_exclude_source_datasets is not None:
        overrides["data.train_exclude_source_datasets"] = args.train_exclude_source_datasets
    if args.val_source_datasets is not None:
        overrides["data.val_source_datasets"] = args.val_source_datasets
    if args.val_exclude_source_datasets is not None:
        overrides["data.val_exclude_source_datasets"] = args.val_exclude_source_datasets

    config = apply_overrides(load_config(args.config), overrides)
    train(config, resume=Path(args.resume) if args.resume else None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
