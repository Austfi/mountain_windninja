"""Apply a residual U-Net checkpoint to WindNinja mass-solver rasters."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path

from .build_dataset import (
    CHANNELS,
    build_aligned_terrain_inputs,
    read_uv,
    terrain_features_from_input_channels,
)
from .model_unet import build_unet
from .pairing import DEFAULT_MOMENTUM_DOMAIN, parse_run_label
from .raster_io import (
    center_crop,
    crop_grid_metadata,
    read_ascii_grid,
    same_grid,
    write_ascii_grid,
)
from .train import resolve_model_in_channels
from .wind_math import SpeedUnits, uv_to_speed_direction

SPEED_UNITS = ("mps", "mph", "kph", "kts")
EXCLUDED_RASTER_PREFIXES = ("PASTCAST", "NOMADS", "HEIGHT-HRRR")
TIMESTAMP_RE = re.compile(r"(?P<label>(?:\d{2}-\d{2}-\d{4}|\d{8})_\d{4})")
RUN_DIR_DOMAIN_RE = re.compile(r"^(?P<domain>.+)_\d{8}_\d{4}_reanalysis_\d+h_[A-Za-z0-9_-]+$")


@dataclass(frozen=True)
class InferenceRasterPair:
    sample_id: str
    timestamp: dt.datetime | None
    speed_path: Path
    direction_path: Path


def _require_torch():
    try:
        import torch
    except ImportError as exc:
        raise RuntimeError(
            "PyTorch is required for inference. Install ml/residual_unet/requirements.txt."
        ) from exc
    return torch


def _load_checkpoint(torch, checkpoint_path: Path) -> dict:
    try:
        return torch.load(checkpoint_path, map_location="cpu", weights_only=False)
    except TypeError:
        return torch.load(checkpoint_path, map_location="cpu")


def _resolve_device(torch, requested: str):
    if requested == "auto":
        return torch.device("cuda" if torch.cuda.is_available() else "cpu")
    if requested == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA was requested for inference, but torch.cuda.is_available() is false.")
    return torch.device(requested)


def _normalization_arrays(normalization: dict):
    if not normalization:
        raise ValueError("Checkpoint is missing normalization stats required for inference.")
    if "input_mean" not in normalization or "input_std" not in normalization:
        raise ValueError("Checkpoint normalization must contain input_mean and input_std.")

    import numpy as np

    mean = np.asarray(normalization["input_mean"], dtype=np.float32)
    std = np.asarray(normalization["input_std"], dtype=np.float32)
    if mean.ndim != 1 or std.ndim != 1 or mean.shape != std.shape:
        raise ValueError("Checkpoint normalization input_mean/input_std must be matching 1-D arrays.")
    return mean[:, None, None], np.maximum(std[:, None, None], 1e-6)


def _load_model(checkpoint_path: Path, device_name: str):
    torch = _require_torch()
    checkpoint = _load_checkpoint(torch, checkpoint_path)
    config = checkpoint.get("config", {})
    model_cfg = config.get("model", {})
    normalization = checkpoint.get("normalization")
    _normalization_arrays(normalization)
    device = _resolve_device(torch, device_name)
    model = build_unet(
        in_channels=resolve_model_in_channels(model_cfg, normalization),
        out_channels=int(model_cfg.get("out_channels", 2)),
        base_channels=int(model_cfg.get("base_channels", 32)),
        block_type=str(model_cfg.get("block_type", "conv")),
    ).to(device)
    model.load_state_dict(checkpoint["model_state_dict"])
    model.eval()
    return torch, model, normalization, config, device


def _timestamp_from_base(base: str) -> dt.datetime | None:
    matches = list(TIMESTAMP_RE.finditer(base))
    if not matches:
        return None
    return parse_run_label(matches[-1].group("label"))


def _terrain_domain_from_run_dir(run_dir: Path) -> str:
    match = RUN_DIR_DOMAIN_RE.match(run_dir.name)
    if not match:
        return DEFAULT_MOMENTUM_DOMAIN
    domain = match.group("domain")
    return domain.removesuffix("_mass")


def _display_path(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def _should_ignore_raster(path: Path) -> bool:
    return path.name.startswith(EXCLUDED_RASTER_PREFIXES)


def collect_inference_rasters(run_dir: str | Path) -> list[InferenceRasterPair]:
    """Collect complete WindNinja speed/direction output pairs in one run directory."""
    run_path = Path(run_dir)
    pairs = []
    for speed_path in sorted(run_path.glob("*_vel.asc")):
        if _should_ignore_raster(speed_path):
            continue
        base = speed_path.name.removesuffix("_vel.asc")
        direction_path = speed_path.with_name(f"{base}_ang.asc")
        if not direction_path.exists() or _should_ignore_raster(direction_path):
            continue
        pairs.append(
            InferenceRasterPair(
                sample_id=base,
                timestamp=_timestamp_from_base(base),
                speed_path=speed_path,
                direction_path=direction_path,
            )
        )
    if any(pair.timestamp is not None for pair in pairs):
        pairs = [pair for pair in pairs if pair.timestamp is not None]
    return pairs


def _pair_keys(pair: InferenceRasterPair) -> list[str]:
    keys = []
    if pair.timestamp is not None:
        keys.append(pair.timestamp.isoformat())
    keys.append(pair.sample_id)
    return keys


def _pair_lookup(pairs: list[InferenceRasterPair]) -> dict[str, InferenceRasterPair]:
    lookup = {}
    for pair in pairs:
        for key in _pair_keys(pair):
            lookup.setdefault(key, pair)
    return lookup


def _match_pair(pair: InferenceRasterPair, lookup: dict[str, InferenceRasterPair]) -> InferenceRasterPair | None:
    for key in _pair_keys(pair):
        if key in lookup:
            return lookup[key]
    return None


def _metric_sums(pred_uv, mass_uv, mom_uv, valid_mask) -> dict[str, float]:
    import numpy as np

    mask = np.asarray(valid_mask, dtype=bool)
    count = int(mask.sum())
    if count == 0:
        return {
            "count": 0,
            "ml_squared_vector_error": 0.0,
            "mass_squared_vector_error": 0.0,
            "ml_speed_abs_error": 0.0,
            "mass_speed_abs_error": 0.0,
        }

    ml_err = np.sum((pred_uv - mom_uv) ** 2, axis=0)
    mass_err = np.sum((mass_uv - mom_uv) ** 2, axis=0)
    ml_speed = np.sqrt(np.sum(pred_uv ** 2, axis=0) + 1e-6)
    mass_speed = np.sqrt(np.sum(mass_uv ** 2, axis=0) + 1e-6)
    mom_speed = np.sqrt(np.sum(mom_uv ** 2, axis=0) + 1e-6)
    return {
        "count": count,
        "ml_squared_vector_error": float(ml_err[mask].sum()),
        "mass_squared_vector_error": float(mass_err[mask].sum()),
        "ml_speed_abs_error": float(np.abs(ml_speed - mom_speed)[mask].sum()),
        "mass_speed_abs_error": float(np.abs(mass_speed - mom_speed)[mask].sum()),
    }


def _finalize_metrics(totals: dict[str, float]) -> dict[str, float]:
    count = max(int(totals["count"]), 1)
    mass_rmse = math.sqrt(totals["mass_squared_vector_error"] / count)
    ml_rmse = math.sqrt(totals["ml_squared_vector_error"] / count)
    improvement = 100.0 * (mass_rmse - ml_rmse) / mass_rmse if mass_rmse else 0.0
    return {
        "valid_pixel_count": int(totals["count"]),
        "mass_vector_rmse": mass_rmse,
        "ml_vector_rmse": ml_rmse,
        "vector_rmse_improvement_percent": improvement,
        "mass_speed_mae": totals["mass_speed_abs_error"] / count,
        "ml_speed_mae": totals["ml_speed_abs_error"] / count,
    }


def _add_metric_sums(totals: dict[str, float], sums: dict[str, float]) -> None:
    for key, value in sums.items():
        totals[key] += value


def _masked(values, valid_mask, nodata: float):
    import numpy as np

    out = np.asarray(values, dtype=np.float32).copy()
    out[~valid_mask] = nodata
    return out


def _write_prediction_outputs(
    out_dir: Path,
    sample_id: str,
    reference_grid,
    pred_uv,
    pred_delta,
    valid_mask,
    *,
    crop_size: int,
    output_speed_units: SpeedUnits,
) -> dict[str, str]:
    nodata = reference_grid.nodata
    speed, direction = uv_to_speed_direction(pred_uv[0], pred_uv[1], units=output_speed_units)
    outputs = {
        "corrected_speed_path": out_dir / "corrected" / f"{sample_id}_ml_vel.asc",
        "corrected_direction_path": out_dir / "corrected" / f"{sample_id}_ml_ang.asc",
        "u_path": out_dir / "uv" / f"{sample_id}_ml_u.asc",
        "v_path": out_dir / "uv" / f"{sample_id}_ml_v.asc",
        "delta_u_path": out_dir / "residual" / f"{sample_id}_delta_u.asc",
        "delta_v_path": out_dir / "residual" / f"{sample_id}_delta_v.asc",
    }
    arrays = {
        "corrected_speed_path": speed,
        "corrected_direction_path": direction,
        "u_path": pred_uv[0],
        "v_path": pred_uv[1],
        "delta_u_path": pred_delta[0],
        "delta_v_path": pred_delta[1],
    }
    for key, path in outputs.items():
        values = _masked(arrays[key], valid_mask, nodata)
        output_grid = crop_grid_metadata(reference_grid, crop_size, data=values)
        write_ascii_grid(path, output_grid)
    return {key: path.as_posix() for key, path in outputs.items()}


def _write_sample_metrics(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def infer(
    checkpoint_path: Path,
    mass_run: Path,
    out_dir: Path,
    *,
    source_root: Path,
    speed_units: SpeedUnits = "mph",
    output_speed_units: SpeedUnits | None = None,
    crop_size: int = 96,
    terrain_file: str | Path | None = None,
    terrain_domain: str | None = None,
    momentum_run: Path | None = None,
    max_samples: int | None = None,
    device_name: str = "auto",
) -> dict:
    import numpy as np

    if crop_size % 16 != 0:
        raise ValueError("crop_size must be divisible by 16 for the current U-Net architecture.")
    output_speed_units = output_speed_units or speed_units
    out_dir.mkdir(parents=True, exist_ok=True)

    mass_pairs = collect_inference_rasters(mass_run)
    if max_samples is not None:
        mass_pairs = mass_pairs[:max_samples]
    if not mass_pairs:
        raise ValueError(f"No complete WindNinja *_vel.asc/*_ang.asc pairs found in {mass_run}")

    reference_grid = read_ascii_grid(mass_pairs[0].speed_path)
    resolved_terrain_domain = terrain_domain or _terrain_domain_from_run_dir(mass_run)

    torch, model, normalization, config, device = _load_model(checkpoint_path, device_name)
    input_mean, input_std = _normalization_arrays(normalization)
    input_channels = normalization.get("input_channels", CHANNELS)
    terrain_features = terrain_features_from_input_channels(input_channels)

    terrain_channels, terrain_mask, built_input_channels, resolved_terrain_path = build_aligned_terrain_inputs(
        source_root,
        reference_grid,
        crop_size,
        terrain_file=terrain_file,
        domain=resolved_terrain_domain,
        terrain_features=terrain_features,
    )
    if list(input_channels) != built_input_channels:
        raise ValueError(
            f"Checkpoint input channels {input_channels} do not match inference-built "
            f"channels {built_input_channels}."
        )

    momentum_lookup = {}
    if momentum_run is not None:
        momentum_lookup = _pair_lookup(collect_inference_rasters(momentum_run))

    totals = {
        "count": 0,
        "ml_squared_vector_error": 0.0,
        "mass_squared_vector_error": 0.0,
        "ml_speed_abs_error": 0.0,
        "mass_speed_abs_error": 0.0,
    }
    sample_metric_rows: list[dict[str, object]] = []
    metadata_rows: list[dict[str, object]] = []

    with torch.no_grad():
        for pair in mass_pairs:
            mass_uv_full, mass_mask_full, mass_grid = read_uv(
                pair.speed_path,
                pair.direction_path,
                units=speed_units,
            )
            if not same_grid(reference_grid, mass_grid, tolerance=1e-3):
                raise ValueError(f"Mass raster grid changed within run: {pair.speed_path}")

            mass_uv = center_crop(mass_uv_full, crop_size)
            mass_mask = center_crop(mass_mask_full, crop_size)
            valid_mask = terrain_mask & mass_mask

            x = np.concatenate([terrain_channels, mass_uv], axis=0).astype(np.float32)
            if x.shape[0] != input_mean.shape[0]:
                raise ValueError(
                    f"Checkpoint expects {input_mean.shape[0]} input channels, but inference built {x.shape[0]}."
                )
            x[:, ~valid_mask] = 0.0
            x = (x - input_mean) / input_std

            tensor = torch.from_numpy(x[None, ...]).to(device)
            pred_delta = model(tensor).detach().cpu().numpy()[0].astype(np.float32)
            pred_delta[:, ~valid_mask] = 0.0
            pred_uv = (mass_uv + pred_delta).astype(np.float32)
            pred_uv[:, ~valid_mask] = 0.0

            outputs = _write_prediction_outputs(
                out_dir,
                pair.sample_id,
                mass_grid,
                pred_uv,
                pred_delta,
                valid_mask,
                crop_size=crop_size,
                output_speed_units=output_speed_units,
            )

            row: dict[str, object] = {
                "sample_id": pair.sample_id,
                "timestamp_utc": pair.timestamp.isoformat() if pair.timestamp else "",
                "mass_speed_path": pair.speed_path.as_posix(),
                "mass_direction_path": pair.direction_path.as_posix(),
                "valid_pixel_count": int(valid_mask.sum()),
                **outputs,
            }

            momentum_pair = _match_pair(pair, momentum_lookup) if momentum_lookup else None
            if momentum_pair is not None:
                mom_uv_full, mom_mask_full, mom_grid = read_uv(
                    momentum_pair.speed_path,
                    momentum_pair.direction_path,
                    units=speed_units,
                )
                if not same_grid(mass_grid, mom_grid, tolerance=1e-3):
                    raise ValueError(f"Mass/momentum grids do not match for {pair.sample_id}")
                mom_uv = center_crop(mom_uv_full, crop_size)
                mom_mask = center_crop(mom_mask_full, crop_size)
                compare_mask = valid_mask & mom_mask
                sums = _metric_sums(pred_uv, mass_uv, mom_uv, compare_mask)
                _add_metric_sums(totals, sums)
                item_metrics = _finalize_metrics(sums)
                sample_metric_rows.append({
                    "sample_id": pair.sample_id,
                    "timestamp_utc": row["timestamp_utc"],
                    "momentum_speed_path": momentum_pair.speed_path.as_posix(),
                    **item_metrics,
                })
                row["momentum_speed_path"] = momentum_pair.speed_path.as_posix()
                row["momentum_direction_path"] = momentum_pair.direction_path.as_posix()
            metadata_rows.append(row)

    metadata = {
        "created_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "checkpoint": checkpoint_path.as_posix(),
        "mass_run": mass_run.as_posix(),
        "momentum_run": momentum_run.as_posix() if momentum_run is not None else None,
        "source_root": source_root.as_posix(),
        "terrain_file": (
            Path(terrain_file).as_posix()
            if terrain_file is not None
            else _display_path(resolved_terrain_path, source_root)
        ),
        "terrain_domain": resolved_terrain_domain,
        "terrain_features": terrain_features,
        "device": str(device),
        "speed_units": speed_units,
        "output_speed_units": output_speed_units,
        "crop_size": crop_size,
        "input_channels": input_channels,
        "model_config": config.get("model", {}),
        "sample_count": len(metadata_rows),
        "samples": metadata_rows,
    }
    (out_dir / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")

    summary = {
        "sample_count": len(metadata_rows),
        "out_dir": out_dir.as_posix(),
        "metadata_path": (out_dir / "metadata.json").as_posix(),
    }
    if sample_metric_rows:
        metrics = _finalize_metrics(totals)
        (out_dir / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
        _write_sample_metrics(out_dir / "sample_metrics.csv", sample_metric_rows)
        summary["metrics"] = metrics
        summary["metrics_path"] = (out_dir / "metrics.json").as_posix()
        summary["sample_metrics_path"] = (out_dir / "sample_metrics.csv").as_posix()
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Apply a residual U-Net checkpoint to mass-solver rasters.")
    parser.add_argument("--checkpoint", required=True, help="Path to best.pt or another residual U-Net checkpoint.")
    parser.add_argument("--mass-run", required=True, help="WindNinja mass-solver output directory.")
    parser.add_argument("--out", required=True, help="Output directory for ML-corrected rasters.")
    parser.add_argument("--source-root", default=".", help="Mountain WindNinja repo root containing static_data/.")
    parser.add_argument("--terrain-file", help="Terrain file path. Relative paths are resolved from repo root or static_data/.")
    parser.add_argument("--terrain-domain", help="Domain key used to find static_data/<domain>.tif or .lcp.")
    parser.add_argument("--speed-units", default="mph", choices=SPEED_UNITS, help="Units in the input WindNinja speed rasters.")
    parser.add_argument(
        "--output-speed-units",
        choices=SPEED_UNITS,
        help="Units for corrected speed rasters. Defaults to --speed-units.",
    )
    parser.add_argument("--crop-size", type=int, default=96)
    parser.add_argument("--momentum-run", help="Optional momentum-solver output directory for comparison metrics.")
    parser.add_argument("--max-samples", type=int, help="Limit the number of raster pairs for smoke tests.")
    parser.add_argument("--device", default="auto", choices=["auto", "cpu", "cuda"])
    return parser


def main() -> int:
    args = build_parser().parse_args()
    summary = infer(
        Path(args.checkpoint),
        Path(args.mass_run),
        Path(args.out),
        source_root=Path(args.source_root),
        speed_units=args.speed_units,
        output_speed_units=args.output_speed_units,
        crop_size=args.crop_size,
        terrain_file=args.terrain_file,
        terrain_domain=args.terrain_domain,
        momentum_run=Path(args.momentum_run) if args.momentum_run else None,
        max_samples=args.max_samples,
        device_name=args.device,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
