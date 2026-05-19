"""Build or package residual U-Net ZIP artifacts for Colab training."""
from __future__ import annotations

import argparse
import subprocess
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


def upload_paths_to_gcs(paths: list[Path], bucket: str, prefix: str) -> None:
    prefix = prefix.strip("/")
    for path in paths:
        destination = f"gs://{bucket}/{prefix}/{path.name}" if prefix else f"gs://{bucket}/{path.name}"
        print(f"Uploading {path} -> {destination}")
        subprocess.run(["gcloud", "storage", "cp", str(path), destination], check=True)


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
        "--code-only",
        action="store_true",
        help="Write/upload only residual_unet_code.zip. Useful when the dataset ZIP is already in GCS.",
    )
    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Only write ZIPs from an existing processed dataset.",
    )
    parser.add_argument("--gcs-bucket", help="Optional Cloud Storage bucket to upload artifacts to.")
    parser.add_argument("--gcs-prefix", default="drive_upload", help="Bucket prefix for uploaded artifacts.")
    parser.add_argument(
        "--notebook",
        action="append",
        type=Path,
        help="Notebook path to upload beside the ZIP artifacts. Repeat for multiple notebooks.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    raw_root = Path(args.raw_root)
    processed_dir = Path(args.processed_dir)
    upload_dir = Path(args.upload_dir)

    if args.code_only:
        print("Code-only mode: skipping dataset build and dataset ZIP.")
    elif not args.skip_build:
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
    write_code_zip(code_zip)
    upload_paths = [code_zip]
    dataset_zip = None
    if not args.code_only:
        dataset_zip = upload_dir / f"{processed_dir.name}_dataset.zip"
        write_dataset_zip(processed_dir, dataset_zip)
        upload_paths.append(dataset_zip)
    print(f"Wrote {code_zip}")
    if dataset_zip is not None:
        print(f"Wrote {dataset_zip}")
    if args.notebook:
        for notebook in args.notebook:
            if not notebook.exists():
                raise FileNotFoundError(f"Missing notebook: {notebook}")
            upload_paths.append(notebook)
    if args.gcs_bucket:
        upload_paths_to_gcs(upload_paths, args.gcs_bucket, args.gcs_prefix)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
