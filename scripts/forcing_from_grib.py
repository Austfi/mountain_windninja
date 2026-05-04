#!/usr/bin/env python3
"""Convert one GRIB/NetCDF U/V timestep into WindNinja speed/direction grids."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
from dataclasses import dataclass
from pathlib import Path
import re
import subprocess
import tempfile

try:
    from . import config_loader, utils
    from .daily_run import parse_utc_timestamp
    from .wind_math import (
        asc_nodata_value,
        asc_shape,
        iter_asc_data_rows,
        read_asc_header,
        speed_direction_from_uv,
    )
except ImportError:
    import config_loader
    import utils
    from daily_run import parse_utc_timestamp
    from wind_math import (
        asc_nodata_value,
        asc_shape,
        iter_asc_data_rows,
        read_asc_header,
        speed_direction_from_uv,
    )


logger = utils.setup_logging("forcing_from_grib")
OUTPUT_NODATA = -9999.0


class ForcingError(ValueError):
    """Raised for operator-fixable forcing conversion problems."""


@dataclass(frozen=True)
class DatasetCandidate:
    source: str
    band: int | None
    description: str
    metadata: dict[str, str]

    def haystack(self) -> str:
        metadata_text = " ".join(f"{key}={value}" for key, value in self.metadata.items())
        return f"{self.source} {self.description} {metadata_text}"

    def summary(self) -> str:
        band_text = f" band {self.band}" if self.band is not None else ""
        return f"{self.source}{band_text} :: {self.description}".strip()


@dataclass(frozen=True)
class TerrainGrid:
    path: Path
    size: tuple[int, int]
    geo_transform: tuple[float, float, float, float, float, float]
    wkt: str


def resolve_repo_path(raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path.resolve()
    return (Path(os.fspath(config_loader.BASE_DIR)) / path).resolve()


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def require_mounted_path(path: Path, label: str, *, must_exist: bool) -> None:
    roots = (Path(config_loader.RUNTIME_DIR), Path(config_loader.STATIC_DATA_DIR))
    if not any(_is_relative_to(path, root) for root in roots):
        roots_text = ", ".join(str(root) for root in roots)
        raise ForcingError(f"{label} must be under a mounted repo path: {roots_text}")
    if must_exist and not path.exists():
        raise ForcingError(f"{label} does not exist: {path}")


def _gdalinfo_json(dataset: str | Path) -> dict:
    command = ["gdalinfo", "-json", str(dataset)]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise ForcingError(f"gdalinfo failed for {dataset}: {detail}")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise ForcingError(f"Could not parse gdalinfo JSON for {dataset}") from exc


def _metadata_as_strings(raw_metadata: dict | None) -> dict[str, str]:
    values: dict[str, str] = {}
    for domain, payload in (raw_metadata or {}).items():
        if isinstance(payload, dict):
            for key, value in payload.items():
                values[f"{domain}:{key}"] = str(value)
        else:
            values[str(domain)] = str(payload)
    return values


def collect_candidates(input_path: Path, payload: dict) -> list[DatasetCandidate]:
    """Collect possible GDAL sources/bands from gdalinfo JSON output."""
    candidates: list[DatasetCandidate] = []

    for item in payload.get("subdatasets") or []:
        name = item.get("name")
        if not name:
            continue
        description = item.get("description") or item.get("desc") or name
        candidates.append(DatasetCandidate(name, None, description, {}))

    subdatasets = (payload.get("metadata") or {}).get("SUBDATASETS") or {}
    for key, name in sorted(subdatasets.items()):
        match = re.match(r"SUBDATASET_(\d+)_NAME", key)
        if not match:
            continue
        description = subdatasets.get(f"SUBDATASET_{match.group(1)}_DESC", name)
        candidates.append(DatasetCandidate(str(name), None, str(description), {}))

    for idx, band in enumerate(payload.get("bands") or [], start=1):
        description = band.get("description") or f"Band {idx}"
        metadata = _metadata_as_strings(band.get("metadata"))
        candidates.append(
            DatasetCandidate(
                str(input_path),
                idx,
                str(description),
                metadata,
            )
        )

    if not candidates:
        candidates.append(DatasetCandidate(str(input_path), None, str(input_path), {}))
    return candidates


def _compact(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _digits(value: str) -> str:
    return re.sub(r"\D+", "", value)


def _time_tokens(raw_time: str) -> set[str]:
    tokens = {_compact(raw_time), _digits(raw_time)}
    try:
        parsed = parse_utc_timestamp(raw_time)
    except ValueError:
        return {token for token in tokens if token}

    tokens.update({
        parsed.strftime("%Y%m%d%H%M"),
        parsed.strftime("%Y-%m-%dT%H:%M"),
        parsed.strftime("%Y-%m-%d %H:%M"),
    })
    return {_compact(token) for token in tokens if token} | {
        _digits(token) for token in tokens if token
    }


def _matches_token(candidate: DatasetCandidate, token: str) -> bool:
    return _compact(token) in _compact(candidate.haystack())


def _matches_time(candidate: DatasetCandidate, raw_time: str) -> bool:
    compact_haystack = _compact(candidate.haystack())
    digit_haystack = _digits(candidate.haystack())
    for token in _time_tokens(raw_time):
        if token and (token in compact_haystack or token in digit_haystack):
            return True
    return False


def _candidate_list_message(candidates: list[DatasetCandidate]) -> str:
    return "\n".join(f"  - {candidate.summary()}" for candidate in candidates[:30])


def select_candidate(
    candidates: list[DatasetCandidate],
    *,
    var_name: str,
    level: str,
    raw_time: str,
    exact_source: str | None = None,
) -> DatasetCandidate:
    """Pick a single candidate by variable, level, and optionally time."""
    if exact_source:
        return DatasetCandidate(exact_source, None, exact_source, {})

    matches = [
        candidate
        for candidate in candidates
        if _matches_token(candidate, var_name) and _matches_token(candidate, level)
    ]
    if len(matches) > 1:
        time_matches = [candidate for candidate in matches if _matches_time(candidate, raw_time)]
        if time_matches:
            matches = time_matches

    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise ForcingError(
            f"No GDAL band/subdataset matched var={var_name!r}, level={level!r}.\n"
            f"Candidates:\n{_candidate_list_message(candidates)}"
        )
    raise ForcingError(
        f"Ambiguous GDAL band/subdataset for var={var_name!r}, level={level!r}, "
        f"time={raw_time!r}. Use --u-source/--v-source with an exact GDAL dataset.\n"
        f"Candidates:\n{_candidate_list_message(matches)}"
    )


def _terrain_grid(path: Path) -> TerrainGrid:
    payload = _gdalinfo_json(path)
    size = payload.get("size") or []
    transform = payload.get("geoTransform") or []
    wkt = ((payload.get("coordinateSystem") or {}).get("wkt")) or ""
    if len(size) != 2:
        raise ForcingError(f"Could not determine terrain dimensions: {path}")
    if len(transform) != 6:
        raise ForcingError(f"Missing terrain geotransform: {path}")
    if not wkt.strip():
        raise ForcingError(f"Missing terrain CRS/projection: {path}")
    if float(transform[2]) != 0 or float(transform[4]) != 0:
        raise ForcingError(f"Terrain must be north-up: {path}")
    return TerrainGrid(
        path=path,
        size=(int(size[0]), int(size[1])),
        geo_transform=tuple(float(value) for value in transform),
        wkt=wkt,
    )


def _extent(grid: TerrainGrid) -> tuple[float, float, float, float]:
    gt = grid.geo_transform
    width, height = grid.size
    xmin = gt[0]
    ymax = gt[3]
    xmax = gt[0] + gt[1] * width
    ymin = gt[3] + gt[5] * height
    return min(xmin, xmax), min(ymin, ymax), max(xmin, xmax), max(ymin, ymax)


def warp_candidate_to_terrain(
    candidate: DatasetCandidate,
    terrain: TerrainGrid,
    output_path: Path,
) -> None:
    xmin, ymin, xmax, ymax = _extent(terrain)
    command = [
        "gdalwarp",
        "-overwrite",
        "-of",
        "AAIGrid",
        "-r",
        "bilinear",
        "-dstnodata",
        str(OUTPUT_NODATA),
        "-t_srs",
        terrain.wkt,
        "-te",
        str(xmin),
        str(ymin),
        str(xmax),
        str(ymax),
        "-ts",
        str(terrain.size[0]),
        str(terrain.size[1]),
    ]
    if candidate.band is not None:
        command.extend(["-b", str(candidate.band)])
    command.extend([candidate.source, str(output_path)])

    logger.info(f"Running: {' '.join(command)}")
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise ForcingError(f"gdalwarp failed for {candidate.summary()}: {detail}")


def _header_with_nodata(header_lines: list[str], nodata: float) -> list[str]:
    out_lines: list[str] = []
    replaced = False
    for line in header_lines:
        if line.strip().lower().startswith("nodata_value"):
            out_lines.append(f"NODATA_value {nodata:g}")
            replaced = True
        else:
            out_lines.append(line)
    if not replaced:
        out_lines.append(f"NODATA_value {nodata:g}")
    return out_lines


def _format_grid_value(value: float) -> str:
    if math.isfinite(value):
        return f"{value:.6f}"
    return f"{OUTPUT_NODATA:g}"


def write_speed_direction_grids(
    u_grid: Path,
    v_grid: Path,
    speed_grid: Path,
    direction_grid: Path,
) -> None:
    """Compute WindNinja griddedInitialization inputs from aligned U/V AAIGrids."""
    if asc_shape(u_grid) != asc_shape(v_grid):
        raise ForcingError("Aligned U and V grids have different dimensions.")

    u_header_lines, u_header = read_asc_header(u_grid)
    _v_header_lines, v_header = read_asc_header(v_grid)
    u_nodata = asc_nodata_value(u_header, OUTPUT_NODATA)
    v_nodata = asc_nodata_value(v_header, OUTPUT_NODATA)

    header_lines = _header_with_nodata(u_header_lines, OUTPUT_NODATA)
    with speed_grid.open("w", encoding="utf-8") as speed_out, direction_grid.open(
        "w", encoding="utf-8"
    ) as direction_out:
        speed_out.write("\n".join(header_lines) + "\n")
        direction_out.write("\n".join(header_lines) + "\n")

        for u_row, v_row in zip(iter_asc_data_rows(u_grid), iter_asc_data_rows(v_grid)):
            if len(u_row) != len(v_row):
                raise ForcingError("Aligned U and V grids have mismatched row lengths.")
            speed_values: list[str] = []
            direction_values: list[str] = []
            for u_value, v_value in zip(u_row, v_row):
                if (
                    (u_nodata is not None and u_value == u_nodata)
                    or (v_nodata is not None and v_value == v_nodata)
                    or math.isnan(u_value)
                    or math.isnan(v_value)
                ):
                    speed_values.append(f"{OUTPUT_NODATA:g}")
                    direction_values.append(f"{OUTPUT_NODATA:g}")
                    continue
                speed, direction = speed_direction_from_uv(u_value, v_value)
                speed_values.append(_format_grid_value(speed if speed is not None else OUTPUT_NODATA))
                direction_values.append(
                    _format_grid_value(direction if direction is not None else OUTPUT_NODATA)
                )
            speed_out.write(" ".join(speed_values) + "\n")
            direction_out.write(" ".join(direction_values) + "\n")


def write_prj_sidecars(speed_grid: Path, direction_grid: Path, wkt: str) -> None:
    for grid_path in (speed_grid, direction_grid):
        grid_path.with_suffix(".prj").write_text(wkt.rstrip() + "\n", encoding="utf-8")


def main() -> int:
    config_loader.init_directories()
    parser = argparse.ArgumentParser(
        description="Convert one local GRIB/NetCDF U/V timestep into WindNinja AAIGrids."
    )
    parser.add_argument("input", help="Local GRIB/NetCDF file under runtime/ or static_data/.")
    parser.add_argument("--domain", required=True, choices=config_loader.list_domains())
    parser.add_argument("--time", required=True, help="UTC timestep to select.")
    parser.add_argument("--u-var", required=True, help="U-wind variable token, e.g. UGRD.")
    parser.add_argument("--v-var", required=True, help="V-wind variable token, e.g. VGRD.")
    parser.add_argument("--level", required=True, help="Vertical level token, e.g. 10m.")
    parser.add_argument("--out", required=True, help="Output directory under runtime/ or static_data/.")
    parser.add_argument("--u-source", help="Exact GDAL dataset/subdataset override for U.")
    parser.add_argument("--v-source", help="Exact GDAL dataset/subdataset override for V.")
    args = parser.parse_args()

    try:
        run_time = parse_utc_timestamp(args.time)
    except ValueError as exc:
        parser.error(str(exc))

    input_path = resolve_repo_path(args.input)
    output_dir = resolve_repo_path(args.out)
    require_mounted_path(input_path, "INPUT", must_exist=True)
    require_mounted_path(output_dir, "--out", must_exist=False)
    output_dir.mkdir(parents=True, exist_ok=True)

    domain_config = config_loader.get_domain_config(args.domain)
    if domain_config.elevation_file.suffix.lower() == ".lcp":
        raise ForcingError(
            "forcing-from-grib requires a DEM domain in v1. Register/use the "
            "matching .tif terrain before creating gridded forcing."
        )
    terrain = _terrain_grid(domain_config.elevation_file)

    source_payload = _gdalinfo_json(input_path)
    candidates = collect_candidates(input_path, source_payload)
    u_candidate = select_candidate(
        candidates,
        var_name=args.u_var,
        level=args.level,
        raw_time=args.time,
        exact_source=args.u_source,
    )
    v_candidate = select_candidate(
        candidates,
        var_name=args.v_var,
        level=args.level,
        raw_time=args.time,
        exact_source=args.v_source,
    )

    speed_grid = output_dir / "speed.asc"
    direction_grid = output_dir / "direction.asc"

    with tempfile.TemporaryDirectory(prefix="mwn-forcing-", dir=output_dir) as tmp_dir:
        u_aligned = Path(tmp_dir) / "u.asc"
        v_aligned = Path(tmp_dir) / "v.asc"
        warp_candidate_to_terrain(u_candidate, terrain, u_aligned)
        warp_candidate_to_terrain(v_candidate, terrain, v_aligned)
        write_speed_direction_grids(u_aligned, v_aligned, speed_grid, direction_grid)

    write_prj_sidecars(speed_grid, direction_grid, terrain.wkt)
    metadata = {
        "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "input": str(input_path),
        "domain": domain_config.key,
        "terrain": str(domain_config.elevation_file),
        "time_utc": run_time.isoformat() + "Z",
        "u_var": args.u_var,
        "v_var": args.v_var,
        "level": args.level,
        "u_source": u_candidate.summary(),
        "v_source": v_candidate.summary(),
        "speed_grid": str(speed_grid),
        "direction_grid": str(direction_grid),
        "speed_units": "mps",
        "direction_formula": "(270 - atan2(v, u) * 180/pi) % 360",
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2) + "\n",
        encoding="utf-8",
    )

    logger.info(f"Wrote {speed_grid}")
    logger.info(f"Wrote {direction_grid}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
