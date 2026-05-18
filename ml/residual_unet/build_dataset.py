"""Build NPZ shards for the Berthoud Pass residual U-Net v0 dataset."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Iterable

from .pairing import (
    DEFAULT_MASS_DOMAIN,
    DEFAULT_MOMENTUM_DOMAIN,
    SolverPair,
    blocked_day_split,
    pair_mass_momentum,
)
from .raster_io import AsciiGrid, center_crop, read_ascii_grid, same_grid
from .wind_math import speed_direction_to_uv

CHANNELS = ["z_rel", "dzdx", "dzdy", "u_mass", "v_mass"]
TARGETS = ["delta_u", "delta_v"]


def _run_gdal(command: list[str]) -> None:
    result = subprocess.run(command, check=False, text=True, capture_output=True)
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"GDAL command failed: {' '.join(command)}\n{detail}")


DEFAULT_TERRAIN_DOMAIN = DEFAULT_MOMENTUM_DOMAIN


def _candidate_repo_paths(source_root: Path, raw_path: str | Path) -> list[Path]:
    path = Path(raw_path)
    if path.is_absolute():
        return [path]
    return [
        source_root / path,
        source_root / "static_data" / path,
    ]


def _find_terrain(
    source_root: Path,
    *,
    terrain_file: str | Path | None = None,
    domain: str = DEFAULT_TERRAIN_DOMAIN,
) -> Path:
    if terrain_file is not None:
        for candidate in _candidate_repo_paths(source_root, terrain_file):
            if candidate.exists():
                return candidate
        raise FileNotFoundError(f"Could not find terrain file: {terrain_file}")

    candidates = [
        source_root / "static_data" / f"{domain}.tif",
        source_root / "static_data" / f"{domain}.lcp",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        f"Could not find terrain for {domain}. Expected static_data/{domain}.tif or .lcp"
    )


def align_terrain_to_reference(
    source_root: Path,
    reference: AsciiGrid,
    *,
    terrain_file: str | Path | None = None,
    domain: str = DEFAULT_TERRAIN_DOMAIN,
) -> AsciiGrid:
    """Warp terrain band 1 onto a WindNinja output grid."""
    terrain_path = _find_terrain(source_root, terrain_file=terrain_file, domain=domain)
    xmin, ymin, xmax, ymax = reference.bounds
    with tempfile.TemporaryDirectory(prefix="mwn-ml-terrain-") as temp:
        temp_path = Path(temp)
        band_path = temp_path / "terrain_band.tif"
        aligned_path = temp_path / "terrain_aligned.asc"
        _run_gdal([
            "gdal_translate",
            "-q",
            "-b",
            "1",
            str(terrain_path),
            str(band_path),
        ])
        _run_gdal([
            "gdalwarp",
            "-q",
            "-overwrite",
            "-of",
            "AAIGrid",
            "-r",
            "bilinear",
            "-te",
            str(xmin),
            str(ymin),
            str(xmax),
            str(ymax),
            "-tr",
            str(reference.cellsize),
            str(reference.cellsize),
            "-dstnodata",
            str(reference.nodata),
            str(band_path),
            str(aligned_path),
        ])
        terrain = read_ascii_grid(aligned_path)
    if not same_grid(reference, terrain, tolerance=1e-3):
        raise ValueError("Aligned terrain grid does not match WindNinja output grid.")
    return terrain


def build_terrain_channels(terrain: AsciiGrid, crop_size: int):
    import numpy as np

    z = np.asarray(terrain.data, dtype=np.float32)
    valid = z > terrain.nodata + 1
    z_work = np.where(valid, z, np.nan)
    z_rel = z_work - np.nanmean(z_work)
    grad_rows, grad_cols = np.gradient(z_work, terrain.cellsize, terrain.cellsize)
    dzdx = grad_cols
    dzdy = -grad_rows
    terrain_stack = np.stack([z_rel, dzdx, dzdy]).astype(np.float32)
    terrain_stack = np.nan_to_num(terrain_stack, nan=0.0, posinf=0.0, neginf=0.0)
    return center_crop(terrain_stack, crop_size), center_crop(valid, crop_size)


def read_uv(speed_path: Path, direction_path: Path, *, units: str = "mph"):
    speed_grid = read_ascii_grid(speed_path)
    direction_grid = read_ascii_grid(direction_path)
    if not same_grid(speed_grid, direction_grid):
        raise ValueError(f"Speed/direction grids do not match: {speed_path} {direction_path}")

    import numpy as np

    speed = np.asarray(speed_grid.data, dtype=np.float32)
    direction = np.asarray(direction_grid.data, dtype=np.float32)
    valid = (
        (speed > speed_grid.nodata + 1)
        & (direction > direction_grid.nodata + 1)
        & np.isfinite(speed)
        & np.isfinite(direction)
    )
    u, v = speed_direction_to_uv(speed, direction, units=units)
    uv = np.stack([u, v]).astype(np.float32)
    uv[:, ~valid] = 0.0
    return uv, valid, speed_grid


def _relative(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def write_sample(
    solver_pair: SolverPair,
    sample_index: int,
    split: str,
    out_dir: Path,
    source_root: Path,
    terrain_channels,
    terrain_mask,
    crop_size: int,
    sample_prefix: str,
    source_dataset: str,
    domain: str,
    mass_domain: str,
    terrain_file: str | None,
) -> dict[str, str]:
    import numpy as np

    mass_uv, mass_mask, mass_grid = read_uv(
        solver_pair.mass.speed_path,
        solver_pair.mass.direction_path,
    )
    mom_uv, mom_mask, mom_grid = read_uv(
        solver_pair.momentum.speed_path,
        solver_pair.momentum.direction_path,
    )
    if not same_grid(mass_grid, mom_grid):
        raise ValueError(f"Mass/momentum grids do not match at {solver_pair.timestamp.isoformat()}")

    mass_uv = center_crop(mass_uv, crop_size)
    mom_uv = center_crop(mom_uv, crop_size)
    mass_mask = center_crop(mass_mask, crop_size)
    mom_mask = center_crop(mom_mask, crop_size)
    valid_mask = terrain_mask & mass_mask & mom_mask

    delta_uv = mom_uv - mass_uv
    x = np.concatenate([terrain_channels, mass_uv], axis=0).astype(np.float32)
    y = delta_uv.astype(np.float32)
    x[:, ~valid_mask] = 0.0
    y[:, ~valid_mask] = 0.0

    sample_id = f"{sample_prefix}_{sample_index:06d}"
    sample_path = out_dir / "samples" / f"{sample_id}.npz"
    sample_path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(
        sample_path,
        x=x,
        y=y,
        mass_uv=mass_uv.astype(np.float32),
        mom_uv=mom_uv.astype(np.float32),
        valid_mask=valid_mask.astype(np.bool_),
    )

    return {
        "sample_id": sample_id,
        "source_dataset": source_dataset,
        "timestamp_utc": solver_pair.timestamp.isoformat(),
        "date": solver_pair.timestamp.date().isoformat(),
        "domain": domain,
        "mass_domain": mass_domain,
        "terrain_file": terrain_file or "",
        "split": split,
        "npz_path": sample_path.relative_to(out_dir).as_posix(),
        "mass_speed_path": _relative(solver_pair.mass.speed_path, source_root),
        "mass_direction_path": _relative(solver_pair.mass.direction_path, source_root),
        "momentum_speed_path": _relative(solver_pair.momentum.speed_path, source_root),
        "momentum_direction_path": _relative(solver_pair.momentum.direction_path, source_root),
    }


def compute_input_normalization(rows: Iterable[dict[str, str]], out_dir: Path) -> dict:
    import numpy as np

    sums = np.zeros(len(CHANNELS), dtype=np.float64)
    squares = np.zeros(len(CHANNELS), dtype=np.float64)
    counts = np.zeros(len(CHANNELS), dtype=np.int64)

    for row in rows:
        if row["split"] != "train":
            continue
        with np.load(out_dir / row["npz_path"]) as sample:
            x = sample["x"].astype(np.float32)
            valid = sample["valid_mask"].astype(bool)
        for channel_index in range(x.shape[0]):
            values = x[channel_index][valid]
            values = values[np.isfinite(values)]
            sums[channel_index] += float(values.sum(dtype=np.float64))
            squares[channel_index] += float((values.astype(np.float64) ** 2).sum())
            counts[channel_index] += int(values.size)

    if (counts == 0).any():
        raise ValueError("Cannot compute normalization: at least one channel has no train values.")

    means = sums / counts
    variances = np.maximum(squares / counts - means ** 2, 1e-12)
    stds = np.sqrt(variances)
    return {
        "input_channels": CHANNELS,
        "input_mean": [float(value) for value in means],
        "input_std": [float(value) for value in stds],
        "target_channels": TARGETS,
    }


def write_manifest(rows: list[dict[str, str]], path: Path) -> None:
    if not rows:
        raise ValueError("No samples to write.")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def build_dataset(
    source_root: Path,
    out_dir: Path,
    crop_size: int,
    *,
    force: bool = False,
    max_samples: int | None = None,
    momentum_domain: str = DEFAULT_MOMENTUM_DOMAIN,
    mass_domain: str = DEFAULT_MASS_DOMAIN,
    terrain_file: str | Path | None = None,
    source_dataset: str | None = None,
    sample_prefix: str | None = None,
) -> dict:
    if out_dir.exists() and not force and (out_dir / "manifest.csv").exists():
        raise FileExistsError(f"Dataset already exists. Use --force to rebuild: {out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "samples").mkdir(parents=True, exist_ok=True)

    pairs = pair_mass_momentum(
        source_root,
        momentum_domain=momentum_domain,
        mass_domain=mass_domain,
    )
    if not pairs:
        raise ValueError("No paired Berthoud mass/momentum raster samples found.")
    if max_samples is not None:
        pairs = pairs[:max_samples]

    reference = read_ascii_grid(pairs[0].mass.speed_path)
    terrain = align_terrain_to_reference(
        source_root,
        reference,
        terrain_file=terrain_file,
        domain=momentum_domain,
    )
    terrain_channels, terrain_mask = build_terrain_channels(terrain, crop_size)

    split_by_day = blocked_day_split([pair.timestamp for pair in pairs])
    source_dataset = source_dataset or out_dir.name
    sample_prefix = sample_prefix or source_dataset
    terrain_label = Path(terrain_file).as_posix() if terrain_file is not None else ""
    rows = []
    for index, pair in enumerate(pairs):
        split = split_by_day[pair.timestamp.date()]
        rows.append(
            write_sample(
                pair,
                index,
                split,
                out_dir,
                source_root,
                terrain_channels,
                terrain_mask,
                crop_size,
                sample_prefix,
                source_dataset,
                momentum_domain,
                mass_domain,
                terrain_label,
            )
        )

    normalization = compute_input_normalization(rows, out_dir)
    write_manifest(rows, out_dir / "manifest.csv")
    (out_dir / "normalization.json").write_text(
        json.dumps(normalization, indent=2) + "\n",
        encoding="utf-8",
    )

    split_counts = {name: sum(1 for row in rows if row["split"] == name) for name in ("train", "val", "test")}
    summary = {
        "created_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source_root": source_root.as_posix(),
        "sample_count": len(rows),
        "crop_size": crop_size,
        "momentum_domain": momentum_domain,
        "mass_domain": mass_domain,
        "terrain_file": terrain_label,
        "source_dataset": source_dataset,
        "input_channels": CHANNELS,
        "target_channels": TARGETS,
        "split_counts": split_counts,
    }
    (out_dir / "dataset_summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the Berthoud residual U-Net v0 dataset.")
    parser.add_argument("--source-root", default=".", help="Mountain WindNinja repo root to read.")
    parser.add_argument("--out", required=True, help="Output processed dataset directory.")
    parser.add_argument("--crop-size", type=int, default=96)
    parser.add_argument("--max-samples", type=int, help="Optional smoke-test sample limit.")
    parser.add_argument("--momentum-domain", default=DEFAULT_MOMENTUM_DOMAIN)
    parser.add_argument("--mass-domain", default=DEFAULT_MASS_DOMAIN)
    parser.add_argument("--terrain-file", help="Terrain file path. Relative paths are resolved from repo root or static_data/.")
    parser.add_argument("--source-dataset", help="Source label to write into manifest rows.")
    parser.add_argument("--sample-prefix", help="Prefix for generated sample_id values.")
    parser.add_argument("--force", action="store_true", help="Overwrite an existing processed dataset.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    summary = build_dataset(
        Path(args.source_root).resolve(),
        Path(args.out).resolve(),
        args.crop_size,
        force=args.force,
        max_samples=args.max_samples,
        momentum_domain=args.momentum_domain,
        mass_domain=args.mass_domain,
        terrain_file=args.terrain_file,
        source_dataset=args.source_dataset,
        sample_prefix=args.sample_prefix,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
