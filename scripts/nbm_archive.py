#!/usr/bin/env python3
"""Create WindNinja gridded forcing from archived NBM wind fields."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import tempfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path

try:
    from . import config_loader, forcing_from_grib as forcing, utils
    from .daily_run import parse_utc_timestamp
except ImportError:
    import config_loader
    import forcing_from_grib as forcing
    import utils
    from daily_run import parse_utc_timestamp


logger = utils.setup_logging("nbm_archive")
NBM_BASE_URL = "https://noaa-nbm-grib2-pds.s3.amazonaws.com"


@dataclass(frozen=True)
class NbmRecord:
    index: int
    offset: int
    cycle: str
    variable: str
    level: str
    description: str
    raw: str


def ymdhm(value: dt.datetime) -> str:
    return value.strftime("%Y%m%d%H%M")


def nbm_grib_url(cycle_time: dt.datetime, lead_hours: int, product: str) -> str:
    cycle_date = cycle_time.strftime("%Y%m%d")
    cycle_hour = cycle_time.strftime("%H")
    return (
        f"{NBM_BASE_URL}/blend.{cycle_date}/{cycle_hour}/core/"
        f"blend.t{cycle_hour}z.core.f{lead_hours:03d}.{product}.grib2"
    )


def read_url(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "mountain-windninja"})
    with urllib.request.urlopen(request, timeout=90) as response:
        return response.read()


def read_url_range(url: str, start: int, end: int | None) -> bytes:
    range_value = f"bytes={start}-" if end is None else f"bytes={start}-{end}"
    request = urllib.request.Request(
        url,
        headers={"Range": range_value, "User-Agent": "mountain-windninja"},
    )
    with urllib.request.urlopen(request, timeout=90) as response:
        return response.read()


def parse_index(index_text: str) -> list[NbmRecord]:
    records: list[NbmRecord] = []
    for raw_line in index_text.splitlines():
        if not raw_line.strip():
            continue
        parts = raw_line.split(":")
        if len(parts) < 6:
            continue
        records.append(
            NbmRecord(
                index=int(parts[0]),
                offset=int(parts[1]),
                cycle=parts[2],
                variable=parts[3],
                level=parts[4],
                description=":".join(parts[5:]),
                raw=raw_line,
            )
        )
    return records


def find_record(records: list[NbmRecord], variable: str, level: str) -> NbmRecord:
    matches = [
        record
        for record in records
        if record.variable == variable
        and record.level == level
        and "ens std dev" not in record.description.lower()
    ]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise ValueError(f"NBM index does not contain {variable}:{level}")
    raise ValueError(f"NBM index has ambiguous {variable}:{level} records")


def record_end(records: list[NbmRecord], record: NbmRecord) -> int | None:
    for candidate in records:
        if candidate.offset > record.offset:
            return candidate.offset - 1
    return None


def download_selected_records(
    url: str,
    all_records: list[NbmRecord],
    selected_records: list[NbmRecord],
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("wb") as handle:
        for record in selected_records:
            end = record_end(all_records, record)
            logger.info(f"Fetching NBM {record.variable}:{record.level} bytes {record.offset}-{end or ''}")
            handle.write(read_url_range(url, record.offset, end))


def build_forcing(
    *,
    valid_time: dt.datetime,
    domain: str,
    lead_hours: int,
    product: str,
    output_dir: Path,
    parent_speed_units: str,
    keep_raw: bool,
) -> dict:
    if lead_hours < 1:
        raise ValueError("NBM does not publish f000; use --lead-hours >= 1.")

    cycle_time = valid_time - dt.timedelta(hours=lead_hours)
    grib_url = nbm_grib_url(cycle_time, lead_hours, product)
    index_url = f"{grib_url}.idx"
    logger.info(f"NBM cycle: {ymdhm(cycle_time)} f{lead_hours:03d}")
    logger.info(f"NBM index: {index_url}")

    index_text = read_url(index_url).decode("utf-8")
    index_records = parse_index(index_text)
    speed_record = find_record(index_records, "WIND", "10 m above ground")
    direction_record = find_record(index_records, "WDIR", "10 m above ground")

    output_dir.mkdir(parents=True, exist_ok=True)
    raw_grib = output_dir / f"nbm_{ymdhm(valid_time)}_f{lead_hours:03d}.grib2"
    download_selected_records(
        grib_url,
        index_records,
        [direction_record, speed_record],
        raw_grib,
    )

    domain_config = config_loader.get_gridded_domain_config(domain)
    if domain_config.elevation_file.suffix.lower() == ".lcp":
        raise ValueError("NBM archive forcing requires a DEM-backed domain, not .lcp.")
    terrain = forcing._terrain_grid(domain_config.elevation_file)

    payload = forcing._gdalinfo_json(raw_grib)
    bands = payload.get("bands") or []
    if len(bands) < 2:
        raise ValueError(f"Extracted NBM GRIB did not contain two wind bands: {raw_grib}")
    direction_candidate = forcing.DatasetCandidate(
        str(raw_grib),
        1,
        str(bands[0].get("description") or "NBM WDIR 10 m"),
        forcing._metadata_as_strings(bands[0].get("metadata")),
    )
    speed_candidate = forcing.DatasetCandidate(
        str(raw_grib),
        2,
        str(bands[1].get("description") or "NBM WIND 10 m"),
        forcing._metadata_as_strings(bands[1].get("metadata")),
    )

    speed_grid = output_dir / "speed.asc"
    direction_grid = output_dir / "direction.asc"
    parent_speed_grid = output_dir / "parent_vel.asc"
    parent_direction_grid = output_dir / "parent_ang.asc"

    with tempfile.TemporaryDirectory(prefix="mwn-nbm-", dir=output_dir) as tmp_dir:
        source_speed = Path(tmp_dir) / "source_speed.asc"
        source_direction = Path(tmp_dir) / "source_direction.asc"
        forcing.warp_candidate_to_terrain(speed_candidate, terrain, source_speed)
        forcing.warp_candidate_to_terrain(direction_candidate, terrain, source_direction)
        forcing.write_speed_direction_pair_grids(
            source_speed,
            source_direction,
            speed_grid,
            direction_grid,
            input_speed_units="mps",
            output_speed_units="mps",
        )
        forcing.write_speed_direction_pair_grids(
            source_speed,
            source_direction,
            parent_speed_grid,
            parent_direction_grid,
            input_speed_units="mps",
            output_speed_units=parent_speed_units,
        )

    forcing.write_prj_sidecars(speed_grid, direction_grid, terrain.wkt)
    forcing.write_prj_sidecars(parent_speed_grid, parent_direction_grid, terrain.wkt)

    if not keep_raw:
        raw_grib.unlink(missing_ok=True)

    metadata = {
        "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source": "NBM archive",
        "valid_time_utc": valid_time.isoformat() + "Z",
        "cycle_time_utc": cycle_time.isoformat() + "Z",
        "lead_hours": lead_hours,
        "product": product,
        "grib_url": grib_url,
        "index_url": index_url,
        "speed_record": speed_record.raw,
        "direction_record": direction_record.raw,
        "domain": domain_config.key,
        "terrain": str(domain_config.elevation_file),
        "speed_grid": str(speed_grid),
        "direction_grid": str(direction_grid),
        "speed_units": "mps",
        "parent_speed_grid": str(parent_speed_grid),
        "parent_direction_grid": str(parent_direction_grid),
        "parent_speed_units": parent_speed_units,
    }
    (output_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2) + "\n",
        encoding="utf-8",
    )
    return metadata


def main() -> int:
    config_loader.init_directories()
    parser = argparse.ArgumentParser(
        description="Fetch archived NBM 10 m wind and create WindNinja grid forcing."
    )
    parser.add_argument("--time", required=True, help="Valid UTC time.")
    parser.add_argument("--domain", required=True, choices=config_loader.list_domains())
    parser.add_argument("--lead-hours", type=int, default=1,
                        help="Forecast lead to validate, default 1 because NBM has no f000.")
    parser.add_argument("--product", default="co", help="NBM product/domain, default co.")
    parser.add_argument("--out", required=True, help="Output directory under runtime/ or static_data/.")
    parser.add_argument("--parent-speed-units", choices=["mph", "mps", "kph", "kts"],
                        default="mph")
    parser.add_argument("--keep-raw", action="store_true", help="Keep extracted GRIB records.")
    args = parser.parse_args()

    try:
        valid_time = parse_utc_timestamp(args.time)
    except ValueError as exc:
        parser.error(str(exc))

    output_dir = forcing.resolve_repo_path(args.out)
    forcing.require_mounted_path(output_dir, "--out", must_exist=False)
    metadata = build_forcing(
        valid_time=valid_time,
        domain=args.domain,
        lead_hours=args.lead_hours,
        product=args.product,
        output_dir=output_dir,
        parent_speed_units=args.parent_speed_units,
        keep_raw=args.keep_raw,
    )
    logger.info(f"Wrote {metadata['speed_grid']}")
    logger.info(f"Wrote {metadata['direction_grid']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
