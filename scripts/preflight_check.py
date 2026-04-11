#!/usr/bin/env python3
"""Validate that everything is set up correctly before running WindNinja."""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import config_loader

try:
    from google.cloud import storage as gcs_storage
except ImportError:
    gcs_storage = None


def _path_status(path: Path, *, must_exist: bool = True, executable: bool = False) -> tuple[bool, str]:
    if must_exist and not path.exists():
        return False, f"missing: {path}"
    if executable and not path.is_file():
        return False, f"not an executable file: {path}"
    if executable and not os.access(path, os.X_OK):
        return False, f"not executable: {path}"
    return True, str(path)


def _command_status(command: str) -> tuple[bool, str]:
    resolved = shutil.which(command)
    if not resolved:
        return False, f"not found in PATH: {command}"
    return True, resolved


def _surface_metadata_status(path: Path) -> tuple[bool, str]:
    result = subprocess.run(
        ["gdalinfo", "-json", str(path)],
        capture_output=True, text=True, check=False,
    )
    if result.returncode != 0:
        return False, result.stderr.strip() or "gdalinfo failed"

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        return False, f"could not parse gdalinfo output: {exc}"

    size = payload.get("size") or [0, 0]
    geo_transform = payload.get("geoTransform") or [0, 0, 0, 0, 0, 0]
    bands = payload.get("bands") or []
    wkt = ((payload.get("coordinateSystem") or {}).get("wkt")) or ""
    wkt_lower = wkt.lower()
    is_lcp = path.suffix.lower() == ".lcp"

    warnings = []

    has_nodata = any(band.get("noDataValue") is not None for band in bands)
    if has_nodata:
        warnings.append("has NO_DATA values (WindNinja will attempt to fill them)")

    if is_lcp and len(bands) < 5:
        return False, "LCP file does not have enough bands"

    if geo_transform[2] != 0 or geo_transform[4] != 0:
        return False, "surface file is not north-up"

    is_geographic = ("projcs[" not in wkt_lower and "projcrs[" not in wkt_lower)
    if is_geographic:
        warnings.append("geographic CRS (WindNinja will auto-convert to UTM)")

    if not is_geographic:
        if "metre" not in wkt_lower and "meter" not in wkt_lower:
            return False, "surface file horizontal units are not meters"

        width_km = abs(float(geo_transform[1])) * float(size[0]) / 1000 if size[0] else 0.0
        height_km = abs(float(geo_transform[5])) * float(size[1]) / 1000 if size[1] else 0.0
        if width_km > 50 or height_km > 50:
            return False, f"extent is {width_km:.1f} x {height_km:.1f} km; keep under 50x50 km"
        size_info = f", extent {width_km:.1f} x {height_km:.1f} km"
    else:
        size_info = ""

    kind = "LCP landscape file" if is_lcp else "DEM"
    detail = f"{kind}{size_info}"
    if warnings:
        detail += " [" + "; ".join(warnings) + "]"
    return True, detail


def _lcp_prj_status(path: Path) -> tuple[bool, str]:
    prj_path = path.with_suffix(".prj")
    if not prj_path.exists():
        return False, f"missing projection sidecar for LCP file: {prj_path}"
    return True, str(prj_path)


def _write_status(path: Path) -> tuple[bool, str]:
    try:
        path.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=path, delete=True):
            pass
        return True, str(path)
    except OSError as exc:
        return False, f"{path}: {exc}"


def _gcs_status(bucket_name: str) -> tuple[bool, str]:
    if gcs_storage is None:
        return False, "google-cloud-storage not installed"
    try:
        client = gcs_storage.Client()
        bucket = client.bucket(bucket_name)
        if bucket.exists():
            return True, f"connected to bucket {bucket_name}"
        return False, f"bucket not found: {bucket_name}"
    except Exception as exc:
        return False, str(exc)


def build_report(check_gcs: bool) -> dict:
    domain = config_loader.get_domain_config()
    runtime_env = config_loader.BASE_DIR / "config" / "runtime.env"
    windninja_cli = Path(config_loader.WINDNINJA_CLI)
    openfoam_bashrc = Path(config_loader.OPENFOAM_BASHRC)

    report = {
        "domain_id": config_loader.DEFAULT_DOMAIN,
        "bucket": config_loader.GCS_BUCKET,
        "checks": [],
    }

    def add(name, passed, detail):
        report["checks"].append({"name": name, "passed": passed, "detail": detail})

    add("runtime_env", *_path_status(runtime_env))
    add("domain_template", *_path_status(domain.template_path))
    add("domain_surface_file", *_path_status(domain.elevation_file))
    if domain.elevation_file.suffix.lower() == ".lcp":
        add("domain_surface_prj", *_lcp_prj_status(domain.elevation_file))
    add("runtime_dir_writable", *_write_status(config_loader.RUNTIME_DIR))
    add("static_data_dir_writable", *_write_status(config_loader.STATIC_DATA_DIR))
    add("windninja_cli", *_path_status(windninja_cli, executable=True))
    add("openfoam_bashrc", *_path_status(openfoam_bashrc))
    add("gdalinfo", *_command_status("gdalinfo"))
    add("gdallocationinfo", *_command_status("gdallocationinfo"))
    if domain.elevation_file.exists() and shutil.which("gdalinfo"):
        add("domain_surface_metadata", *_surface_metadata_status(domain.elevation_file))

    if config_loader.GCS_UPLOAD_ENABLED:
        if check_gcs:
            add("gcs_access", *_gcs_status(config_loader.GCS_BUCKET))
        else:
            add("gcs_access", True, "skipped; rerun with --check-gcs to verify")
    else:
        add("gcs_access", True, "upload disabled")

    return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate Mountain WindNinja runtime prerequisites."
    )
    parser.add_argument("--check-gcs", action="store_true",
                        help="Attempt a live Cloud Storage access check.")
    parser.add_argument("--json", action="store_true",
                        help="Emit JSON instead of human-readable output.")
    args = parser.parse_args()

    report = build_report(check_gcs=args.check_gcs)
    failed = [c for c in report["checks"] if not c["passed"]]

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(f"Domain: {report['domain_id']}")
        print(f"Bucket: {report['bucket']}")
        print()
        for c in report["checks"]:
            marker = "PASS" if c["passed"] else "FAIL"
            print(f"[{marker}] {c['name']}: {c['detail']}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
