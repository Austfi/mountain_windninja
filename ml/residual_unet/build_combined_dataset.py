"""Merge processed residual U-Net datasets into one training set."""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import shutil
from pathlib import Path

from .build_dataset import CHANNELS, TARGETS, compute_input_normalization


DEFAULT_HRRR_DIR = Path("ml/residual_unet/data/processed/berthoud_v0")
DEFAULT_CONTROLLED_DIR = Path("ml/residual_unet/data/processed/controlled_berthoud_training")
DEFAULT_OUT_DIR = Path("ml/residual_unet/data/processed/berthoud_combined_v1")

MANIFEST_FIELDS = [
    "sample_id",
    "source_dataset",
    "source_sample_id",
    "timestamp_utc",
    "date",
    "case_id",
    "speed_mps",
    "direction_deg",
    "domain",
    "mass_domain",
    "momentum_domain",
    "terrain_file",
    "split",
    "npz_path",
    "mass_speed_path",
    "mass_direction_path",
    "momentum_speed_path",
    "momentum_direction_path",
]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def load_summary(processed_dir: Path) -> dict:
    return json.loads((processed_dir / "dataset_summary.json").read_text(encoding="utf-8"))


def _split_counts(rows: list[dict[str, str]]) -> dict[str, int]:
    counts = {"train": 0, "val": 0, "test": 0}
    for row in rows:
        counts[row["split"]] = counts.get(row["split"], 0) + 1
    return counts


def _validate_source(name: str, processed_dir: Path) -> dict:
    manifest = processed_dir / "manifest.csv"
    if not manifest.exists():
        raise FileNotFoundError(f"Missing manifest for {name}: {manifest}")
    summary = load_summary(processed_dir)
    if summary.get("input_channels") != CHANNELS:
        raise ValueError(f"{name} input channels do not match {CHANNELS}: {summary}")
    if summary.get("target_channels") != TARGETS:
        raise ValueError(f"{name} target channels do not match {TARGETS}: {summary}")
    return summary


def write_manifest(rows: list[dict[str, str]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def merge_source(
    *,
    source_name: str,
    processed_dir: Path,
    out_dir: Path,
) -> list[dict[str, str]]:
    rows = read_csv(processed_dir / "manifest.csv")
    merged_rows: list[dict[str, str]] = []
    for row in rows:
        source_sample_id = row["sample_id"]
        sample_id = f"{source_name}__{source_sample_id}"
        source_npz = processed_dir / row["npz_path"]
        if not source_npz.exists():
            raise FileNotFoundError(f"Missing source sample: {source_npz}")

        dest_npz = out_dir / "samples" / source_name / source_npz.name
        dest_npz.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_npz, dest_npz)

        merged = {field: "" for field in MANIFEST_FIELDS}
        for key, value in row.items():
            if key in merged:
                merged[key] = value
        merged.update({
            "sample_id": sample_id,
            "source_dataset": source_name,
            "source_sample_id": source_sample_id,
            "npz_path": dest_npz.relative_to(out_dir).as_posix(),
        })
        merged_rows.append(merged)
    return merged_rows


def build_combined_dataset(
    hrrr_dir: Path,
    controlled_dir: Path,
    out_dir: Path,
    *,
    force: bool = False,
    sources: list[tuple[str, Path]] | None = None,
) -> dict:
    sources = sources or [
        ("berthoud_v0", hrrr_dir),
        ("controlled_berthoud_training", controlled_dir),
    ]
    if out_dir.exists():
        if not force:
            raise FileExistsError(f"Combined dataset already exists. Use --force: {out_dir}")
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    source_summaries = {
        source_name: _validate_source(source_name, processed_dir)
        for source_name, processed_dir in sources
    }
    rows: list[dict[str, str]] = []
    for source_name, processed_dir in sources:
        rows.extend(merge_source(
            source_name=source_name,
            processed_dir=processed_dir,
            out_dir=out_dir,
        ))

    rows.sort(key=lambda row: (row["source_dataset"], row["source_sample_id"]))
    normalization = compute_input_normalization(rows, out_dir)
    write_manifest(rows, out_dir / "manifest.csv")
    (out_dir / "normalization.json").write_text(
        json.dumps(normalization, indent=2) + "\n",
        encoding="utf-8",
    )
    summary = {
        "created_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "sample_count": len(rows),
        "crop_size": next(iter(source_summaries.values())).get("crop_size"),
        "input_channels": CHANNELS,
        "target_channels": TARGETS,
        "split_counts": _split_counts(rows),
        "source_datasets": {
            source_name: {
                "processed_dir": processed_dir.as_posix(),
                "sample_count": int(source_summaries[source_name].get("sample_count", 0)),
                "split_counts": source_summaries[source_name].get("split_counts", {}),
            }
            for source_name, processed_dir in sources
        },
    }
    (out_dir / "dataset_summary.json").write_text(
        json.dumps(summary, indent=2) + "\n",
        encoding="utf-8",
    )
    return summary


def parse_source(raw: str) -> tuple[str, Path]:
    if "=" not in raw:
        raise argparse.ArgumentTypeError("--source must be NAME=PATH")
    name, path = raw.split("=", 1)
    name = name.strip()
    path = path.strip()
    if not name or not path:
        raise argparse.ArgumentTypeError("--source must be NAME=PATH")
    return name, Path(path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Merge HRRR and controlled residual U-Net datasets.")
    parser.add_argument("--hrrr-dir", default=DEFAULT_HRRR_DIR.as_posix())
    parser.add_argument("--controlled-dir", default=DEFAULT_CONTROLLED_DIR.as_posix())
    parser.add_argument("--out", default=DEFAULT_OUT_DIR.as_posix())
    parser.add_argument(
        "--source",
        action="append",
        type=parse_source,
        help="Processed dataset source as NAME=PATH. Repeat to override the default sources.",
    )
    parser.add_argument("--force", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    summary = build_combined_dataset(
        Path(args.hrrr_dir),
        Path(args.controlled_dir),
        Path(args.out),
        force=args.force,
        sources=args.source,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
