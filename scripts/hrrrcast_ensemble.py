"""Postprocess HRRRCast member WindNinja outputs into ensemble speed products."""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
import re
from typing import Any

try:
    from . import utils
except ImportError:
    import utils


logger = utils.setup_logging("hrrrcast_ensemble")

_TIMESTAMP_RE = re.compile(r"(\d{8})[-_]?(\d{4})")
_EXCLUDED_SPEED_PREFIXES = ("GENERIC-", "NOMADS-", "PASTCAST-", "windninja_generic")


def write_ensemble_summary(
    *,
    member_output_dirs: dict[str, str | Path],
    summary_dir: str | Path,
    domain_key: str,
    start_time: dt.datetime,
    stop_time: dt.datetime,
    dry_run: bool = False,
) -> Path:
    """Write JSON metadata and compatible ASCII speed ensemble products."""
    output_dir = Path(summary_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {
        "source": "hrrrcast",
        "domain": domain_key,
        "start_utc": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stop_utc": stop_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "members": list(member_output_dirs),
        "member_output_dirs": {
            member: str(Path(path)) for member, path in member_output_dirs.items()
        },
        "dry_run": dry_run,
        "products": [],
        "skipped": [],
    }

    if not dry_run:
        summary["products"] = _write_speed_products(member_output_dirs, output_dir)
        if not summary["products"]:
            summary["skipped"].append(
                "No compatible member speed rasters were found for ensemble products."
            )

    summary_path = output_dir / "hrrrcast_ensemble_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    logger.info(f"HRRRCast ensemble summary: {summary_path}")
    return summary_path


def _write_speed_products(member_output_dirs: dict[str, str | Path], output_dir: Path) -> list[dict]:
    import numpy as np

    grouped = _group_speed_rasters(member_output_dirs)
    products: list[dict] = []
    for timestamp, member_paths in sorted(grouped.items()):
        if len(member_paths) < 1:
            continue
        grids = []
        compatible_members: list[str] = []
        header: list[str] | None = None
        nodata = -9999.0
        for member, path in member_paths:
            try:
                grid_header, grid, grid_nodata = _read_ascii_grid(path)
            except Exception as exc:
                logger.warning(f"Skipping HRRRCast ensemble raster {path}: {exc}")
                continue
            if header is None:
                header = grid_header
                nodata = grid_nodata
            elif _normalized_header(grid_header) != _normalized_header(header):
                logger.warning(f"Skipping incompatible HRRRCast ensemble raster: {path}")
                continue
            grids.append(np.where(grid == grid_nodata, np.nan, grid))
            compatible_members.append(member)

        if not grids or header is None:
            continue

        stack = np.stack(grids, axis=0)
        stat_arrays = {
            "mean": np.nanmean(stack, axis=0),
            "p10": np.nanpercentile(stack, 10, axis=0),
            "p50": np.nanpercentile(stack, 50, axis=0),
            "p90": np.nanpercentile(stack, 90, axis=0),
            "spread": np.nanpercentile(stack, 90, axis=0) - np.nanpercentile(stack, 10, axis=0),
        }
        stat_files: dict[str, str] = {}
        for stat_name, values in stat_arrays.items():
            out_path = output_dir / f"hrrrcast_speed_{stat_name}_{timestamp}.asc"
            _write_ascii_grid(out_path, header, values, nodata=nodata)
            stat_files[stat_name] = str(out_path)
        products.append(
            {
                "timestamp": timestamp,
                "members": compatible_members,
                "files": stat_files,
            }
        )
    return products


def _group_speed_rasters(member_output_dirs: dict[str, str | Path]) -> dict[str, list[tuple[str, Path]]]:
    grouped: dict[str, list[tuple[str, Path]]] = {}
    for member, output_dir in member_output_dirs.items():
        for path in sorted(Path(output_dir).glob("*_vel.asc")):
            if path.name.startswith(_EXCLUDED_SPEED_PREFIXES):
                continue
            key = _timestamp_key(path)
            grouped.setdefault(key, []).append((member, path))
    return grouped


def _timestamp_key(path: Path) -> str:
    match = _TIMESTAMP_RE.search(path.name)
    if match:
        return f"{match.group(1)}_{match.group(2)}"
    return path.stem.removesuffix("_vel")


def _read_ascii_grid(path: Path) -> tuple[list[str], Any, float]:
    import numpy as np

    with path.open("r", encoding="utf-8") as f:
        header = [next(f).rstrip("\n") for _ in range(6)]
    values = np.loadtxt(path, skiprows=6, dtype="float32")
    return header, values, _nodata_value(header)


def _write_ascii_grid(path: Path, header: list[str], values, *, nodata: float) -> None:
    import numpy as np

    filled = np.where(np.isfinite(values), values, nodata).astype("float32")
    with path.open("w", encoding="utf-8") as f:
        for line in header:
            f.write(f"{line}\n")
        np.savetxt(f, filled, fmt="%.6f")


def _nodata_value(header: list[str]) -> float:
    for line in header:
        parts = line.split()
        if parts and parts[0].lower() == "nodata_value" and len(parts) >= 2:
            try:
                return float(parts[1])
            except ValueError:
                return -9999.0
    return -9999.0


def _normalized_header(header: list[str]) -> list[str]:
    return [" ".join(line.lower().split()) for line in header]
