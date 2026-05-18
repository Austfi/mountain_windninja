"""Build or package residual U-Net ZIP artifacts for Colab training."""
from __future__ import annotations

import argparse
import zipfile
from pathlib import Path

from .build_controlled_dataset import build_controlled_dataset


DEFAULT_RAW_ROOT = Path("runtime/ml/residual_unet/raw/controlled_berthoud_training")
DEFAULT_PROCESSED_DIR = Path("ml/residual_unet/data/processed/controlled_berthoud_training")
DEFAULT_UPLOAD_DIR = Path("ml/residual_unet/outputs/drive_upload")


def _should_include_code(path: Path) -> bool:
    parts = set(path.parts)
    if "__pycache__" in parts:
        return False
    if path.name in {".DS_Store"}:
        return False
    blocked_parts = {"colab", "outputs"}
    if parts & blocked_parts:
        return False
    if "data" in parts and "processed" in parts:
        return False
    return path.is_file()


def write_code_zip(out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(Path("ml").rglob("*")):
            if _should_include_code(path):
                archive.write(path, path.as_posix())


def write_dataset_zip(processed_dir: Path, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    root_name = processed_dir.name
    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(processed_dir.rglob("*")):
            if path.is_file():
                archive.write(path, (Path(root_name) / path.relative_to(processed_dir)).as_posix())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare residual U-Net code and dataset ZIP artifacts for Colab."
    )
    parser.add_argument("--raw-root", default=DEFAULT_RAW_ROOT.as_posix())
    parser.add_argument("--processed-dir", default=DEFAULT_PROCESSED_DIR.as_posix())
    parser.add_argument("--upload-dir", default=DEFAULT_UPLOAD_DIR.as_posix())
    parser.add_argument("--crop-size", type=int, default=96)
    parser.add_argument("--terrain-file", help="Terrain file path passed to controlled dataset build.")
    parser.add_argument("--terrain-domain", help="Domain key passed to controlled dataset build.")
    parser.add_argument("--source-dataset", help="Source label passed to controlled dataset build.")
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Only write ZIPs from an existing processed dataset.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    raw_root = Path(args.raw_root)
    processed_dir = Path(args.processed_dir)
    upload_dir = Path(args.upload_dir)

    if not args.skip_build:
        summary = build_controlled_dataset(
            raw_root,
            processed_dir,
            args.crop_size,
            force=args.force,
            terrain_file=args.terrain_file,
            terrain_domain=args.terrain_domain,
            source_dataset=args.source_dataset,
        )
        print(f"Built dataset: {summary}")
    elif not (processed_dir / "manifest.csv").exists():
        raise FileNotFoundError(f"Missing processed dataset: {processed_dir}")

    code_zip = upload_dir / "residual_unet_code.zip"
    dataset_zip = upload_dir / f"{processed_dir.name}_dataset.zip"
    write_code_zip(code_zip)
    write_dataset_zip(processed_dir, dataset_zip)
    print(f"Wrote {code_zip}")
    print(f"Wrote {dataset_zip}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
