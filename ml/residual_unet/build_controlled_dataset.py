"""Build NPZ shards from controlled WindNinja mass/momentum pair outputs."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
from pathlib import Path

from .build_dataset import (
    TARGETS,
    build_aligned_terrain_inputs,
    compute_input_normalization,
    parse_terrain_features,
    read_uv,
    write_manifest,
)
from .raster_io import center_crop, read_ascii_grid, same_grid


DEFAULT_CONTROLLED_SOURCE = "controlled_berthoud_training"
DEFAULT_VAL_DIRECTIONS = (30.0, 120.0, 210.0, 300.0)
DEFAULT_TEST_DIRECTIONS = (60.0, 150.0, 240.0, 330.0)
DEFAULT_SPLIT_TOLERANCE_DEG = 0.25


def safe_label(value: str) -> str:
    safe = "".join(char if char.isalnum() or char in {"_", "-"} else "_" for char in value)
    return safe.strip("_") or DEFAULT_CONTROLLED_SOURCE


def load_controlled_manifest(raw_root: Path) -> list[dict[str, str]]:
    manifest_path = raw_root / "manifest.csv"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Missing controlled manifest: {manifest_path}")
    with manifest_path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def unique_nonempty_value(rows: list[dict[str, str]], field: str) -> str | None:
    values = {row.get(field, "").strip() for row in rows if row.get(field, "").strip()}
    if not values:
        return None
    if len(values) > 1:
        raise ValueError(f"Controlled manifest has multiple {field} values: {sorted(values)}")
    return next(iter(values))


def infer_controlled_metadata(
    rows: list[dict[str, str]],
    raw_root: Path,
    *,
    terrain_file: str | Path | None = None,
    terrain_domain: str | None = None,
    source_dataset: str | None = None,
) -> dict[str, str]:
    domain_label = terrain_domain or unique_nonempty_value(rows, "domain_label") or ""
    source_label = source_dataset or (
        f"controlled_{safe_label(domain_label)}" if domain_label else safe_label(raw_root.name)
    )

    terrain_label = ""
    if terrain_file is not None:
        terrain_label = Path(terrain_file).as_posix()
    elif not domain_label:
        manifest_terrain = unique_nonempty_value(rows, "terrain_file")
        if manifest_terrain:
            terrain_label = manifest_terrain

    return {
        "source_dataset": safe_label(source_label),
        "terrain_domain": domain_label,
        "terrain_file": terrain_label,
    }


def find_windninja_ascii_pair(output_dir: Path) -> tuple[Path, Path]:
    speed_paths = sorted(output_dir.glob("*_vel.asc"))
    for speed_path in speed_paths:
        if speed_path.name.startswith(("NOMADS-", "PASTCAST-")):
            continue
        direction_path = speed_path.with_name(speed_path.name.replace("_vel.asc", "_ang.asc"))
        if direction_path.exists():
            return speed_path, direction_path
    raise FileNotFoundError(f"No complete WindNinja *_vel.asc/*_ang.asc pair in {output_dir}")


def _direction_distance_deg(a: float, b: float) -> float:
    return abs((a - b + 180.0) % 360.0 - 180.0)


def _parse_direction_values(values: list[str] | None) -> list[float] | None:
    if values is None:
        return None
    parsed: list[float] = []
    for value in values:
        parsed.extend(float(item.strip()) for item in value.split(",") if item.strip())
    return parsed


def controlled_split(
    direction_deg: float,
    *,
    val_directions: list[float] | tuple[float, ...] | None = None,
    test_directions: list[float] | tuple[float, ...] | None = None,
    tolerance_deg: float = DEFAULT_SPLIT_TOLERANCE_DEG,
) -> str:
    """Hold out full direction sectors for a simple generalization check."""
    direction = direction_deg % 360.0
    test_values = test_directions or DEFAULT_TEST_DIRECTIONS
    val_values = val_directions or DEFAULT_VAL_DIRECTIONS
    if any(_direction_distance_deg(direction, candidate) <= tolerance_deg for candidate in test_values):
        return "test"
    if any(_direction_distance_deg(direction, candidate) <= tolerance_deg for candidate in val_values):
        return "val"
    return "train"


def pair_manifest_rows(rows: list[dict[str, str]]) -> list[tuple[dict[str, str], dict[str, str]]]:
    grouped: dict[str, dict[str, dict[str, str]]] = {}
    for row in rows:
        grouped.setdefault(row["case_id"], {})[row["solver"]] = row
    paired = []
    for case_id in sorted(grouped):
        solvers = grouped[case_id]
        if {"mass", "momentum"} <= set(solvers):
            paired.append((solvers["mass"], solvers["momentum"]))
    return paired


def write_controlled_sample(
    *,
    sample_index: int,
    mass_row: dict[str, str],
    momentum_row: dict[str, str],
    out_dir: Path,
    source_dataset: str,
    terrain_domain: str,
    terrain_file: str,
    terrain_channels,
    terrain_mask,
    crop_size: int,
    val_directions: list[float] | tuple[float, ...] | None,
    test_directions: list[float] | tuple[float, ...] | None,
    split_tolerance_deg: float,
) -> dict[str, str]:
    import numpy as np

    mass_speed, mass_direction = find_windninja_ascii_pair(Path(mass_row["output_dir"]))
    mom_speed, mom_direction = find_windninja_ascii_pair(Path(momentum_row["output_dir"]))
    mass_uv, mass_mask, mass_grid = read_uv(mass_speed, mass_direction, units="mps")
    mom_uv, mom_mask, mom_grid = read_uv(mom_speed, mom_direction, units="mps")
    if not same_grid(mass_grid, mom_grid):
        raise ValueError(f"Mass/momentum grids do not match for {mass_row['case_id']}")

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

    case_id = mass_row["case_id"]
    sample_id = f"{safe_label(source_dataset)}_{sample_index:05d}_{case_id}"
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
    direction_deg = float(mass_row["direction_deg"])
    return {
        "sample_id": sample_id,
        "source_dataset": source_dataset,
        "case_id": case_id,
        "speed_mps": mass_row["speed_mps"],
        "direction_deg": mass_row["direction_deg"],
        "domain": terrain_domain,
        "mass_domain": f"{terrain_domain}_mass" if terrain_domain else "",
        "momentum_domain": terrain_domain,
        "terrain_file": terrain_file,
        "split": controlled_split(
            direction_deg,
            val_directions=val_directions,
            test_directions=test_directions,
            tolerance_deg=split_tolerance_deg,
        ),
        "npz_path": sample_path.relative_to(out_dir).as_posix(),
        "mass_speed_path": mass_speed.as_posix(),
        "mass_direction_path": mass_direction.as_posix(),
        "momentum_speed_path": mom_speed.as_posix(),
        "momentum_direction_path": mom_direction.as_posix(),
    }


def build_controlled_dataset(
    raw_root: Path,
    out_dir: Path,
    crop_size: int,
    *,
    force: bool = False,
    terrain_file: str | Path | None = None,
    terrain_domain: str | None = None,
    terrain_features: list[str] | None = None,
    source_dataset: str | None = None,
    val_directions: list[float] | tuple[float, ...] | None = None,
    test_directions: list[float] | tuple[float, ...] | None = None,
    split_tolerance_deg: float = DEFAULT_SPLIT_TOLERANCE_DEG,
) -> dict:
    if out_dir.exists() and not force and (out_dir / "manifest.csv").exists():
        raise FileExistsError(f"Dataset already exists. Use --force to rebuild: {out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "samples").mkdir(parents=True, exist_ok=True)

    rows = load_controlled_manifest(raw_root)
    paired_rows = pair_manifest_rows(rows)
    if not paired_rows:
        raise ValueError(f"No complete mass/momentum rows found in {raw_root / 'manifest.csv'}")
    metadata = infer_controlled_metadata(
        rows,
        raw_root,
        terrain_file=terrain_file,
        terrain_domain=terrain_domain,
        source_dataset=source_dataset,
    )

    first_mass_speed, _first_mass_direction = find_windninja_ascii_pair(
        Path(paired_rows[0][0]["output_dir"])
    )
    reference = read_ascii_grid(first_mass_speed)
    source_root = Path.cwd()
    terrain_channels, terrain_mask, input_channels, resolved_terrain_path = build_aligned_terrain_inputs(
        source_root,
        reference,
        crop_size,
        terrain_file=metadata["terrain_file"] or None,
        domain=metadata["terrain_domain"],
        terrain_features=terrain_features,
    )
    features = parse_terrain_features(terrain_features)
    if features and not metadata["terrain_file"]:
        metadata["terrain_file"] = resolved_terrain_path.relative_to(source_root).as_posix()

    out_rows = []
    for index, (mass_row, momentum_row) in enumerate(paired_rows):
        out_rows.append(
            write_controlled_sample(
                sample_index=index,
                mass_row=mass_row,
                momentum_row=momentum_row,
                out_dir=out_dir,
                source_dataset=metadata["source_dataset"],
                terrain_domain=metadata["terrain_domain"],
                terrain_file=metadata["terrain_file"],
                terrain_channels=terrain_channels,
                terrain_mask=terrain_mask,
                crop_size=crop_size,
                val_directions=val_directions,
                test_directions=test_directions,
                split_tolerance_deg=split_tolerance_deg,
            )
        )

    normalization = compute_input_normalization(out_rows, out_dir, input_channels)
    write_manifest(out_rows, out_dir / "manifest.csv")
    (out_dir / "normalization.json").write_text(
        json.dumps(normalization, indent=2) + "\n",
        encoding="utf-8",
    )
    split_counts = {
        name: sum(1 for row in out_rows if row["split"] == name)
        for name in ("train", "val", "test")
    }
    summary = {
        "created_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "raw_root": raw_root.as_posix(),
        "sample_count": len(out_rows),
        "crop_size": crop_size,
        "source_dataset": metadata["source_dataset"],
        "terrain_domain": metadata["terrain_domain"],
        "terrain_file": metadata["terrain_file"],
        "terrain_features": features,
        "input_channels": input_channels,
        "target_channels": TARGETS,
        "split_policy": {
            "val_directions": list(val_directions or DEFAULT_VAL_DIRECTIONS),
            "test_directions": list(test_directions or DEFAULT_TEST_DIRECTIONS),
            "tolerance_deg": split_tolerance_deg,
        },
        "split_counts": split_counts,
    }
    (out_dir / "dataset_summary.json").write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build a controlled residual U-Net dataset.")
    parser.add_argument("--raw-root", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--crop-size", type=int, default=96)
    parser.add_argument("--terrain-file", help="Terrain file path. Relative paths resolve from repo root or static_data/.")
    parser.add_argument("--terrain-domain", help="Domain key used to find static_data/<domain>.tif or .lcp.")
    parser.add_argument(
        "--terrain-feature",
        action="append",
        help=(
            "Optional extra terrain/LCP feature channel. Supported: canopy_cover. "
            "Repeat or comma-separate values."
        ),
    )
    parser.add_argument("--source-dataset", help="Source label to write into manifest rows and sample IDs.")
    parser.add_argument(
        "--val-direction",
        action="append",
        help="Validation holdout direction in degrees. Repeat or comma-separate values.",
    )
    parser.add_argument(
        "--test-direction",
        action="append",
        help="Test holdout direction in degrees. Repeat or comma-separate values.",
    )
    parser.add_argument(
        "--split-tolerance-deg",
        type=float,
        default=DEFAULT_SPLIT_TOLERANCE_DEG,
        help="Direction matching tolerance for controlled val/test splits.",
    )
    parser.add_argument("--force", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    summary = build_controlled_dataset(
        Path(args.raw_root),
        Path(args.out),
        args.crop_size,
        force=args.force,
        terrain_file=args.terrain_file,
        terrain_domain=args.terrain_domain,
        terrain_features=args.terrain_feature,
        source_dataset=args.source_dataset,
        val_directions=_parse_direction_values(args.val_direction),
        test_directions=_parse_direction_values(args.test_direction),
        split_tolerance_deg=args.split_tolerance_deg,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
