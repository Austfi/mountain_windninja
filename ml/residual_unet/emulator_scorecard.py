"""Decision-oriented evaluation for terrain-specific momentum emulators."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from .dataset import load_normalization, make_dataloader
from .evaluate import _finalize_metrics, _metric_sums
from .model_unet import build_unet
from .train import resolve_model_in_channels


DEFAULT_SPEED_BINS_MPS = (0.0, 5.0, 10.0, 15.0, 20.0, math.inf)
DEFAULT_CANOPY_BINS = (0.0, 10.0, 40.0, 70.0, 101.0)


def _require_torch():
    try:
        import torch
    except ImportError as exc:
        raise RuntimeError(
            "PyTorch is required for the emulator scorecard. "
            "Install ml/residual_unet/requirements.txt."
        ) from exc
    return torch


def _to_device(batch: dict, device):
    return {
        key: value.to(device) if hasattr(value, "to") else value
        for key, value in batch.items()
    }


def _meta_list(batch: dict, key: str, count: int) -> list[str]:
    value = batch.get(key)
    if value is None:
        return [""] * count
    if isinstance(value, str):
        return [value] * count
    return [str(item) for item in value]


def _parse_float(value: str | float | int | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_timestamp(value: str) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def season_for_month(month: int | None) -> str:
    if month is None:
        return "unknown"
    if month in {12, 1, 2}:
        return "winter"
    if month in {3, 4, 5}:
        return "spring"
    if month in {6, 7, 8}:
        return "summer"
    if month in {9, 10, 11}:
        return "fall"
    return "unknown"


def source_kind(source_dataset: str) -> str:
    if "_controlled" in source_dataset:
        return "controlled"
    if "_hrrr" in source_dataset:
        return "hrrr"
    return "other"


def direction_sector_label(direction_deg: float | None, *, sector_size: int = 45) -> str:
    if direction_deg is None or not math.isfinite(direction_deg):
        return "unknown"
    direction = direction_deg % 360.0
    start = int(math.floor(direction / sector_size) * sector_size)
    end = (start + sector_size) % 360
    return f"{start:03d}_{end:03d}"


def range_label(prefix: str, low: float, high: float, unit: str) -> str:
    if math.isinf(high):
        return f"{prefix}_ge_{low:g}{unit}"
    return f"{prefix}_{low:g}_{high:g}{unit}"


def _empty_totals() -> dict[str, float]:
    return defaultdict(float)


def _merge_totals(total: dict[str, float], partial: dict[str, float]) -> None:
    for key, value in partial.items():
        total[key] += value


def _masked_mean_by_sample(values, mask) -> list[float]:
    torch = _require_torch()
    values = values.detach()
    mask = mask.bool()
    sums = (values * mask).flatten(1).sum(dim=1)
    counts = mask.flatten(1).sum(dim=1).clamp_min(1)
    means = sums / counts
    means = torch.where(counts > 0, means, torch.full_like(means, float("nan")))
    return [float(value) for value in means.detach().cpu()]


def _mean_direction_by_sample(mass_uv, valid_mask) -> list[float | None]:
    torch = _require_torch()
    mask = valid_mask.bool()
    counts = mask.flatten(1).sum(dim=1)
    u_mean = (mass_uv[:, 0] * mask).flatten(1).sum(dim=1) / counts.clamp_min(1)
    v_mean = (mass_uv[:, 1] * mask).flatten(1).sum(dim=1) / counts.clamp_min(1)
    direction = (torch.rad2deg(torch.atan2(-u_mean, -v_mean)) + 360.0) % 360.0
    out: list[float | None] = []
    for item_count, item_direction in zip(counts.detach().cpu(), direction.detach().cpu()):
        if int(item_count) <= 0:
            out.append(None)
        else:
            out.append(float(item_direction))
    return out


def _channel_indices(input_channels: list[str]) -> dict[str, int]:
    return {name: index for index, name in enumerate(input_channels)}


def _denormalized_inputs(batch_x, normalization: dict, device):
    torch = _require_torch()
    means = torch.tensor(
        normalization["input_mean"], dtype=batch_x.dtype, device=device
    )[None, :, None, None]
    stds = torch.tensor(
        normalization["input_std"], dtype=batch_x.dtype, device=device
    )[None, :, None, None]
    return batch_x * stds + means


def _add_group(
    groups: dict[tuple[str, str], dict[str, float]],
    group_type: str,
    group: str,
    pred_uv,
    mass_uv,
    mom_uv,
    valid_mask,
) -> None:
    if int(valid_mask.bool().sum().detach().cpu()) <= 0:
        return
    _merge_totals(
        groups[(group_type, group)],
        _metric_sums(pred_uv, mass_uv, mom_uv, valid_mask),
    )


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: Iterable[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        ordered = []
        for row in rows:
            for key in row:
                if key not in ordered:
                    ordered.append(key)
        fieldnames = ordered
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _fmt(value: Any, digits: int = 3) -> str:
    if value in ("", None):
        return ""
    return f"{float(value):.{digits}f}"


def _metrics_row(group_type: str, group: str, totals: dict[str, float]) -> dict[str, Any]:
    metrics = _finalize_metrics(totals)
    return {
        "group_type": group_type,
        "group": group,
        **metrics,
    }


def _sort_metrics_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    order = {
        "overall": 0,
        "source_kind": 1,
        "source_dataset": 2,
        "season": 3,
        "month": 4,
        "direction_sector": 5,
        "target_speed_bin": 6,
        "target_high_wind": 7,
        "canopy_cover": 8,
        "slope_exposure": 9,
    }
    return sorted(rows, key=lambda row: (order.get(str(row["group_type"]), 99), str(row["group"])))


def _top_worst(rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            float(row.get("ml_vector_rmse", 0.0)),
            -float(row.get("vector_rmse_improvement_percent", 0.0)),
        ),
        reverse=True,
    )[:limit]


def _top_regressions(rows: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: float(row.get("vector_rmse_improvement_percent", 0.0)),
    )[:limit]


def _metric_lookup(rows: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    return {(str(row["group_type"]), str(row["group"])): row for row in rows}


def _table_row(label: str, row: dict[str, Any] | None) -> str:
    if row is None:
        return f"| {label} |  |  |  |  |  |  |"
    return (
        f"| {label} | {_fmt(row.get('mass_vector_rmse'))} | "
        f"{_fmt(row.get('ml_vector_rmse'))} | "
        f"{_fmt(row.get('vector_rmse_improvement_percent'), 1)}% | "
        f"{_fmt(row.get('ml_better_pixel_fraction'))} | "
        f"{_fmt(row.get('ml_vector_error_le_1p0mps_fraction'))} | "
        f"{_fmt(row.get('ml_vector_error_le_2p0mps_fraction'))} |"
    )


def _write_report(
    path: Path,
    *,
    checkpoint_path: Path,
    processed_dir: Path,
    split: str,
    metric_rows: list[dict[str, Any]],
    sample_rows: list[dict[str, Any]],
    limit: int,
) -> None:
    lookup = _metric_lookup(metric_rows)
    hrrr_rows = [row for row in metric_rows if row["group_type"] == "source_dataset" and "hrrr" in row["group"]]
    controlled_rows = [
        row for row in metric_rows
        if row["group_type"] == "source_dataset" and "controlled" in row["group"]
    ]
    sector_rows = [row for row in metric_rows if row["group_type"] == "direction_sector"]
    season_rows = [row for row in metric_rows if row["group_type"] == "season"]
    speed_rows = [row for row in metric_rows if row["group_type"] == "target_speed_bin"]
    canopy_rows = [row for row in metric_rows if row["group_type"] == "canopy_cover"]
    exposure_rows = [row for row in metric_rows if row["group_type"] == "slope_exposure"]

    lines = [
        "# Terrain-Specific Momentum Emulator Scorecard",
        "",
        "This report evaluates whether `mass solver + residual U-Net` emulates "
        "WindNinja momentum output on held-out same-terrain samples.",
        "",
        f"- Checkpoint: `{checkpoint_path.as_posix()}`",
        f"- Dataset: `{processed_dir.as_posix()}`",
        f"- Split: `{split}`",
        "",
        "## Primary Metrics",
        "",
        "| Group | Mass RMSE | ML RMSE | Improvement | ML Better Pixels | ML <=1 m/s | ML <=2 m/s |",
        "|---|---:|---:|---:|---:|---:|---:|",
        _table_row("overall", lookup.get(("overall", "all"))),
        _table_row("HRRR only", lookup.get(("source_kind", "hrrr"))),
        _table_row("controlled only", lookup.get(("source_kind", "controlled"))),
        _table_row("target high wind >=10 m/s", lookup.get(("target_high_wind", "target_ge_10mps"))),
        "",
        "## Source Datasets",
        "",
        "| Source | Mass RMSE | ML RMSE | Improvement | ML Better Pixels | ML <=1 m/s | ML <=2 m/s |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in hrrr_rows + controlled_rows:
        lines.append(_table_row(str(row["group"]), row))

    lines += [
        "",
        "## Stratified Checks",
        "",
        "These sections look for regimes where the emulator drifts away from the momentum solve.",
        "",
        "### Season",
        "",
        "| Season | Mass RMSE | ML RMSE | Improvement | ML Better Pixels | ML <=1 m/s | ML <=2 m/s |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in season_rows:
        lines.append(_table_row(str(row["group"]), row))

    lines += [
        "",
        "### Direction Sector",
        "",
        "| Sector | Mass RMSE | ML RMSE | Improvement | ML Better Pixels | ML <=1 m/s | ML <=2 m/s |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in sorted(sector_rows, key=lambda item: str(item["group"])):
        lines.append(_table_row(str(row["group"]), row))

    lines += [
        "",
        "### Target Speed Bin",
        "",
        "| Speed Bin | Mass RMSE | ML RMSE | Improvement | ML Better Pixels | ML <=1 m/s | ML <=2 m/s |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in speed_rows:
        lines.append(_table_row(str(row["group"]), row))

    lines += [
        "",
        "### Canopy Cover",
        "",
        "| Canopy Bin | Mass RMSE | ML RMSE | Improvement | ML Better Pixels | ML <=1 m/s | ML <=2 m/s |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in canopy_rows:
        lines.append(_table_row(str(row["group"]), row))

    lines += [
        "",
        "### Lee/Windward Slope",
        "",
        "| Exposure | Mass RMSE | ML RMSE | Improvement | ML Better Pixels | ML <=1 m/s | ML <=2 m/s |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in exposure_rows:
        lines.append(_table_row(str(row["group"]), row))

    lines += [
        "",
        "## Worst Held-Out Samples",
        "",
        "| Sample | Source | Direction Sector | Mean Target Speed | Mass RMSE | ML RMSE | Improvement | ML Better Pixels |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in _top_worst(sample_rows, limit=limit):
        lines.append(
            f"| {row['sample_id']} | {row['source_dataset']} | {row['direction_sector']} | "
            f"{_fmt(row['mean_momentum_speed_mps'])} | {_fmt(row['mass_vector_rmse'])} | "
            f"{_fmt(row['ml_vector_rmse'])} | "
            f"{_fmt(row['vector_rmse_improvement_percent'], 1)}% | "
            f"{_fmt(row.get('ml_better_pixel_fraction'))} |"
        )

    lines += [
        "",
        "## Worst Regressions",
        "",
        "| Sample | Source | Direction Sector | Mean Target Speed | Mass RMSE | ML RMSE | Improvement | ML Worse By >=1 m/s |",
        "|---|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in _top_regressions(sample_rows, limit=limit):
        lines.append(
            f"| {row['sample_id']} | {row['source_dataset']} | {row['direction_sector']} | "
            f"{_fmt(row['mean_momentum_speed_mps'])} | {_fmt(row['mass_vector_rmse'])} | "
            f"{_fmt(row['ml_vector_rmse'])} | "
            f"{_fmt(row['vector_rmse_improvement_percent'], 1)}% | "
            f"{_fmt(row.get('ml_worse_by_1mps_pixel_fraction'))} |"
        )

    lines += [
        "",
        "## Interpretation Boundary",
        "",
        "- This is a momentum-solver emulation scorecard, not observation validation.",
        "- For the terrain-specific goal, HRRR-only same-terrain held-out performance is the primary operational score.",
        "- Controlled-only performance is a stress test for directions and speeds that may be underrepresented in HRRR.",
        "- Direction, speed, canopy, and lee-side rows identify where more paired data or model changes are needed.",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def write_emulator_scorecard(
    checkpoint_path: Path,
    processed_dir: Path,
    out_dir: Path,
    *,
    split: str = "test",
    batch_size: int = 32,
    num_workers: int = 0,
    pin_memory: bool | None = None,
    prefetch_factor: int | None = None,
    max_samples: int | None = None,
    direction_sector_size: int = 45,
    high_wind_threshold_mps: float = 10.0,
    slope_threshold: float = 0.02,
    worst_case_limit: int = 20,
) -> dict[str, Any]:
    torch = _require_torch()
    checkpoint = torch.load(checkpoint_path, map_location="cpu")
    config = checkpoint["config"]
    model_cfg = config["model"]
    normalization = checkpoint.get("normalization") or load_normalization(processed_dir)
    input_channels = list(normalization.get("input_channels", []))
    channel_indices = _channel_indices(input_channels)

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = build_unet(
        in_channels=resolve_model_in_channels(model_cfg, normalization),
        out_channels=int(model_cfg.get("out_channels", 2)),
        base_channels=int(model_cfg.get("base_channels", 32)),
        block_type=str(model_cfg.get("block_type", "conv")),
    ).to(device)
    model.load_state_dict(checkpoint["model_state_dict"])
    model.eval()

    loader = make_dataloader(
        processed_dir,
        split,
        normalization,
        batch_size=batch_size,
        shuffle=False,
        max_samples=max_samples,
        num_workers=num_workers,
        pin_memory=pin_memory,
        prefetch_factor=prefetch_factor,
    )
    print(
        f"scorecard device={device} batch_size={batch_size} num_workers={num_workers} "
        f"samples={len(loader.dataset)} batches={len(loader)}",
        flush=True,
    )

    groups: dict[tuple[str, str], dict[str, float]] = defaultdict(_empty_totals)
    sample_rows: list[dict[str, Any]] = []

    with torch.no_grad():
        for batch in loader:
            batch = _to_device(batch, device)
            batch_size_actual = int(batch["x"].shape[0])
            pred_uv = batch["mass_uv"] + model(batch["x"])
            mass_uv = batch["mass_uv"]
            mom_uv = batch["mom_uv"]
            valid_mask = batch["valid_mask"].bool()
            x_raw = _denormalized_inputs(batch["x"], normalization, device)

            mass_speed = torch.sqrt((mass_uv ** 2).sum(dim=1) + 1e-6)
            mom_speed = torch.sqrt((mom_uv ** 2).sum(dim=1) + 1e-6)
            mean_mass_speed = _masked_mean_by_sample(mass_speed, valid_mask)
            mean_mom_speed = _masked_mean_by_sample(mom_speed, valid_mask)
            mean_directions = _mean_direction_by_sample(mass_uv, valid_mask)

            sample_ids = _meta_list(batch, "sample_id", batch_size_actual)
            sources = _meta_list(batch, "source_dataset", batch_size_actual)
            timestamps = _meta_list(batch, "timestamp_utc", batch_size_actual)
            case_ids = _meta_list(batch, "case_id", batch_size_actual)
            manifest_speeds = _meta_list(batch, "speed_mps", batch_size_actual)
            manifest_dirs = _meta_list(batch, "direction_deg", batch_size_actual)

            _add_group(groups, "overall", "all", pred_uv, mass_uv, mom_uv, valid_mask)

            for index in range(batch_size_actual):
                sample_mask = torch.zeros_like(valid_mask)
                sample_mask[index] = valid_mask[index]
                source = sources[index]
                kind = source_kind(source)
                timestamp = _parse_timestamp(timestamps[index])
                month = timestamp.month if timestamp else None
                season = season_for_month(month)
                manifest_direction = _parse_float(manifest_dirs[index])
                direction = manifest_direction if manifest_direction is not None else mean_directions[index]
                sector = direction_sector_label(direction, sector_size=direction_sector_size)
                item_sums = _metric_sums(
                    pred_uv[index:index + 1],
                    mass_uv[index:index + 1],
                    mom_uv[index:index + 1],
                    valid_mask[index:index + 1],
                )
                item_metrics = _finalize_metrics(item_sums)

                _add_group(groups, "source_dataset", source, pred_uv, mass_uv, mom_uv, sample_mask)
                _add_group(groups, "source_kind", kind, pred_uv, mass_uv, mom_uv, sample_mask)
                _add_group(groups, "direction_sector", sector, pred_uv, mass_uv, mom_uv, sample_mask)
                if kind == "hrrr":
                    _add_group(groups, "season", season, pred_uv, mass_uv, mom_uv, sample_mask)
                    if timestamp is not None:
                        _add_group(
                            groups,
                            "month",
                            timestamp.strftime("%Y-%m"),
                            pred_uv,
                            mass_uv,
                            mom_uv,
                            sample_mask,
                        )

                sample_rows.append({
                    "sample_id": sample_ids[index],
                    "source_dataset": source,
                    "source_kind": kind,
                    "timestamp_utc": timestamps[index],
                    "case_id": case_ids[index],
                    "manifest_speed_mps": manifest_speeds[index],
                    "manifest_direction_deg": manifest_dirs[index],
                    "mean_mass_speed_mps": mean_mass_speed[index],
                    "mean_momentum_speed_mps": mean_mom_speed[index],
                    "mean_mass_direction_deg": direction if direction is not None else "",
                    "direction_sector": sector,
                    "season": season,
                    **item_metrics,
                })

            for low, high in zip(DEFAULT_SPEED_BINS_MPS[:-1], DEFAULT_SPEED_BINS_MPS[1:]):
                if math.isinf(high):
                    speed_mask = valid_mask & (mom_speed >= low)
                else:
                    speed_mask = valid_mask & (mom_speed >= low) & (mom_speed < high)
                _add_group(
                    groups,
                    "target_speed_bin",
                    range_label("target", low, high, "mps"),
                    pred_uv,
                    mass_uv,
                    mom_uv,
                    speed_mask,
                )
            _add_group(
                groups,
                "target_high_wind",
                range_label("target", high_wind_threshold_mps, math.inf, "mps"),
                pred_uv,
                mass_uv,
                mom_uv,
                valid_mask & (mom_speed >= high_wind_threshold_mps),
            )

            if "canopy_cover" in channel_indices:
                canopy = x_raw[:, channel_indices["canopy_cover"]]
                for low, high in zip(DEFAULT_CANOPY_BINS[:-1], DEFAULT_CANOPY_BINS[1:]):
                    if high >= 101.0:
                        canopy_mask = valid_mask & (canopy >= low)
                    else:
                        canopy_mask = valid_mask & (canopy >= low) & (canopy < high)
                    _add_group(
                        groups,
                        "canopy_cover",
                        range_label("canopy", low, high, "pct"),
                        pred_uv,
                        mass_uv,
                        mom_uv,
                        canopy_mask,
                    )

            if {"dzdx", "dzdy"} <= set(channel_indices):
                dzdx = x_raw[:, channel_indices["dzdx"]]
                dzdy = x_raw[:, channel_indices["dzdy"]]
                speed = mass_speed.clamp_min(1e-6)
                along_flow_slope = dzdx * (mass_uv[:, 0] / speed) + dzdy * (mass_uv[:, 1] / speed)
                lee_mask = valid_mask & (along_flow_slope <= -slope_threshold)
                windward_mask = valid_mask & (along_flow_slope >= slope_threshold)
                cross_flat_mask = valid_mask & ~(lee_mask | windward_mask)
                _add_group(groups, "slope_exposure", "lee", pred_uv, mass_uv, mom_uv, lee_mask)
                _add_group(
                    groups,
                    "slope_exposure",
                    "windward",
                    pred_uv,
                    mass_uv,
                    mom_uv,
                    windward_mask,
                )
                _add_group(
                    groups,
                    "slope_exposure",
                    "cross_or_flat",
                    pred_uv,
                    mass_uv,
                    mom_uv,
                    cross_flat_mask,
                )

    metric_rows = _sort_metrics_rows(
        _metrics_row(group_type, group, totals)
        for (group_type, group), totals in groups.items()
        if int(totals.get("count", 0)) > 0
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    metrics_fields = [
        "group_type",
        "group",
        "valid_pixel_count",
        "mass_vector_rmse",
        "ml_vector_rmse",
        "vector_rmse_improvement_percent",
        "mass_speed_mae",
        "ml_speed_mae",
        "ml_better_pixel_fraction",
        "ml_better_by_1mps_pixel_fraction",
        "ml_worse_by_1mps_pixel_fraction",
        "ml_vector_error_le_0p5mps_fraction",
        "ml_vector_error_le_1p0mps_fraction",
        "ml_vector_error_le_2p0mps_fraction",
        "ml_vector_error_le_3p0mps_fraction",
        "ml_vector_error_le_5p0mps_fraction",
    ]
    _write_csv(out_dir / "scorecard_metrics.csv", metric_rows, metrics_fields)
    _write_csv(out_dir / "scorecard_sample_metrics.csv", sample_rows)
    _write_csv(out_dir / "scorecard_worst_cases.csv", _top_worst(sample_rows, limit=worst_case_limit))
    _write_csv(
        out_dir / "scorecard_worst_regressions.csv",
        _top_regressions(sample_rows, limit=worst_case_limit),
    )
    summary = {
        "checkpoint": checkpoint_path.as_posix(),
        "processed_dir": processed_dir.as_posix(),
        "split": split,
        "sample_count": len(sample_rows),
        "group_count": len(metric_rows),
        "metric_rows": metric_rows,
        "worst_cases": _top_worst(sample_rows, limit=worst_case_limit),
        "worst_regressions": _top_regressions(sample_rows, limit=worst_case_limit),
    }
    (out_dir / "scorecard_summary.json").write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )
    _write_report(
        out_dir / "scorecard_report.md",
        checkpoint_path=checkpoint_path,
        processed_dir=processed_dir,
        split=split,
        metric_rows=metric_rows,
        sample_rows=sample_rows,
        limit=worst_case_limit,
    )
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Write a terrain-specific momentum-emulator validation scorecard."
    )
    parser.add_argument("--checkpoint", required=True)
    parser.add_argument("--data", required=True, help="Processed dataset directory.")
    parser.add_argument("--out", required=True, help="Output scorecard directory.")
    parser.add_argument("--split", default="test", choices=["train", "val", "test"])
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--num-workers", type=int, default=0)
    parser.add_argument("--prefetch-factor", type=int)
    parser.add_argument("--pin-memory", action="store_true")
    parser.add_argument("--no-pin-memory", action="store_true")
    parser.add_argument("--max-samples", type=int)
    parser.add_argument("--direction-sector-size", type=int, default=45)
    parser.add_argument("--high-wind-threshold-mps", type=float, default=10.0)
    parser.add_argument("--slope-threshold", type=float, default=0.02)
    parser.add_argument("--worst-case-limit", type=int, default=20)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    summary = write_emulator_scorecard(
        Path(args.checkpoint),
        Path(args.data),
        Path(args.out),
        split=args.split,
        batch_size=args.batch_size,
        num_workers=args.num_workers,
        pin_memory=False if args.no_pin_memory else (True if args.pin_memory else None),
        prefetch_factor=args.prefetch_factor,
        max_samples=args.max_samples,
        direction_sector_size=args.direction_sector_size,
        high_wind_threshold_mps=args.high_wind_threshold_mps,
        slope_threshold=args.slope_threshold,
        worst_case_limit=args.worst_case_limit,
    )
    print(json.dumps({
        "scorecard_report": str(Path(args.out) / "scorecard_report.md"),
        "sample_count": summary["sample_count"],
        "group_count": summary["group_count"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
