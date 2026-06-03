#!/usr/bin/env python3
"""Build a local WindNinja weather-model NetCDF from HRRRCast GRIB data."""
from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
import os
from pathlib import Path
import tempfile
from typing import Callable
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

try:
    from . import config_loader, utils
    from .weather_netcdf import (
        buffered_bbox,
        clip_dataset_to_bbox,
        domain_bbox_latlon,
        fields_from_datasets,
        forecast_hours_for_window,
        resolve_cycle_candidates,
        validate_windninja_generic_netcdf,
        write_windninja_generic_netcdf,
    )
except ImportError:
    import config_loader
    import utils
    from weather_netcdf import (
        buffered_bbox,
        clip_dataset_to_bbox,
        domain_bbox_latlon,
        fields_from_datasets,
        forecast_hours_for_window,
        resolve_cycle_candidates,
        validate_windninja_generic_netcdf,
        write_windninja_generic_netcdf,
    )


logger = utils.setup_logging("hrrrcast_wx_model")

HRRRCAST_MODEL_NAME = "HRRRCAST"
HRRRCAST_DETERMINISTIC_MEMBER = "avg"
HRRRCAST_ENSEMBLE_MEMBERS = tuple(f"m{idx:02d}" for idx in range(9))
HRRRCAST_VALID_MEMBERS = (HRRRCAST_DETERMINISTIC_MEMBER, *HRRRCAST_ENSEMBLE_MEMBERS)
HRRRCAST_REQUIRED_FIELDS = ("u10", "v10", "t2m", "tcc")

_HRRRCAST_FIELD_SELECTORS = {
    "u10": ("UGRD", "10 m"),
    "v10": ("VGRD", "10 m"),
    "t2m": ("TMP", "2 m"),
    "tcc": ("TCDC", "entire atmosphere"),
}
_HRRRCAST_FIELD_FILTERS = {
    "u10": (
        {"typeOfLevel": "heightAboveGround", "level": 10, "shortName": "10u"},
        {"typeOfLevel": "heightAboveGround", "level": 10, "shortName": "u"},
    ),
    "v10": (
        {"typeOfLevel": "heightAboveGround", "level": 10, "shortName": "10v"},
        {"typeOfLevel": "heightAboveGround", "level": 10, "shortName": "v"},
    ),
    "t2m": (
        {"typeOfLevel": "heightAboveGround", "level": 2, "shortName": "2t"},
        {"typeOfLevel": "heightAboveGround", "level": 2, "shortName": "t"},
    ),
    "tcc": (
        {"typeOfLevel": "atmosphere", "shortName": "tcc"},
        {"shortName": "tcc"},
    ),
}
_FIELD_SPEC = type(
    "HrrrCastFieldSpec",
    (),
    {
        "name": HRRRCAST_MODEL_NAME,
        "wind_input": "uv",
        "variable_aliases": {
            "u10": ("u10", "u", "ugrd", "10u"),
            "v10": ("v10", "v", "vgrd", "10v"),
            "t2m": ("t2m", "t", "tmp", "2t"),
            "tcc": ("tcc", "tcdc", "total_cloud_cover"),
        },
    },
)()


class HrrrCastWeatherError(RuntimeError):
    """Raised for HRRRCast weather-file preparation failures."""


@dataclass(frozen=True)
class HrrrCastIndexRecord:
    message_number: int
    byte_start: int
    byte_end: int | None
    variable: str
    level: str
    line: str


@dataclass(frozen=True)
class HrrrCastCyclePlan:
    cycle: dt.datetime
    forecast_hours: tuple[int, ...]
    idx_text_by_hour: dict[int, str]


def normalize_hrrrcast_member(raw_member: str | None, *, default: str = "avg") -> str:
    member = (raw_member or default).strip().lower()
    if member not in HRRRCAST_VALID_MEMBERS:
        raise HrrrCastWeatherError(
            "HRRRCast member must be avg or m00..m08."
        )
    return member


def expand_hrrrcast_members(raw_members: str) -> list[str]:
    """Parse --hrrrcast-members, preserving order and removing duplicates."""
    values = [item.strip().lower() for item in raw_members.split(",") if item.strip()]
    if len(values) == 1 and values[0] == "all":
        return list(HRRRCAST_ENSEMBLE_MEMBERS)
    if not values:
        raise HrrrCastWeatherError("--hrrrcast-members must list at least one member.")

    members: list[str] = []
    seen: set[str] = set()
    for value in values:
        member = normalize_hrrrcast_member(value)
        if member not in seen:
            members.append(member)
            seen.add(member)
    return members


def prepare_hrrrcast_wx_model(
    *,
    model_name: str,
    start_time: dt.datetime,
    stop_time: dt.datetime,
    domain_config,
    member: str | None = None,
    cache_root: str | Path | None = None,
    base_url: str | None = None,
    max_cycle_rewind: int | None = None,
    cycle: dt.datetime | None = None,
) -> Path:
    if model_name.upper() != HRRRCAST_MODEL_NAME:
        raise HrrrCastWeatherError("HRRRCast source only supports --model HRRRCAST.")

    selected_member = normalize_hrrrcast_member(member)
    root = Path(cache_root or config_loader.HRRRCAST_CACHE_DIR)
    source_base_url = base_url or config_loader.HRRRCAST_BASE_URL
    rewind_count = (
        config_loader.HRRRCAST_MAX_CYCLE_REWIND
        if max_cycle_rewind is None
        else max_cycle_rewind
    )

    bbox = buffered_bbox(domain_bbox_latlon(domain_config), resolution_km=3.0)
    plan = resolve_hrrrcast_cycle_plan(
        start_time=start_time,
        stop_time=stop_time,
        member=selected_member,
        base_url=source_base_url,
        max_cycle_rewind=rewind_count,
        cycle=cycle,
    )
    output_path = hrrrcast_output_path(root, plan.cycle, selected_member, domain_config.key)
    if output_path.exists():
        try:
            validate_windninja_generic_netcdf(output_path)
            logger.info(f"Using cached HRRRCast WindNinja file: {output_path}")
            return output_path
        except Exception as exc:
            logger.warning(
                f"Cached HRRRCast WindNinja file is invalid and will be rebuilt: {exc}"
            )
            output_path.unlink(missing_ok=True)

    logger.info(
        "Preparing HRRRCast weather file: "
        f"member={selected_member} cycle={plan.cycle:%Y-%m-%d %H:%M}Z "
        f"fxx={plan.forecast_hours[0]}..{plan.forecast_hours[-1]}"
    )
    hourly = [
        download_hrrrcast_hour_fields(
            plan.cycle,
            fxx,
            selected_member,
            bbox=bbox,
            cache_root=root,
            base_url=source_base_url,
            idx_text=plan.idx_text_by_hour[fxx],
        )
        for fxx in plan.forecast_hours
    ]
    tmp_output_path = output_path.with_name(
        f".{output_path.name}.{os.getpid()}.tmp"
    )
    tmp_output_path.unlink(missing_ok=True)
    try:
        write_windninja_generic_netcdf(hourly, tmp_output_path)
        tmp_output_path.replace(output_path)
        validate_windninja_generic_netcdf(output_path)
    except Exception:
        tmp_output_path.unlink(missing_ok=True)
        raise
    logger.info(f"Wrote HRRRCast WindNinja file: {output_path}")
    return output_path


def resolve_hrrrcast_cycle_plan(
    *,
    start_time: dt.datetime,
    stop_time: dt.datetime,
    member: str,
    base_url: str | None = None,
    max_cycle_rewind: int | None = None,
    cycle: dt.datetime | None = None,
    fetch_idx_text: Callable[[str], str] | None = None,
    log_failures: bool = True,
) -> HrrrCastCyclePlan:
    selected_member = normalize_hrrrcast_member(member)
    source_base_url = base_url or config_loader.HRRRCAST_BASE_URL
    fetcher = fetch_idx_text or _fetch_text
    rewind_count = (
        config_loader.HRRRCAST_MAX_CYCLE_REWIND
        if max_cycle_rewind is None
        else max_cycle_rewind
    )
    errors: list[str] = []

    candidate_cycles = _hrrrcast_cycle_candidates(
        start_time=start_time,
        max_cycle_rewind=rewind_count,
        base_url=source_base_url,
        explicit_cycle=cycle,
        fetch_text=fetcher,
    )

    for candidate_cycle in candidate_cycles:
        try:
            fxx_values = tuple(forecast_hours_for_window(candidate_cycle, start_time, stop_time))
            max_fxx = hrrrcast_max_forecast_hour(candidate_cycle)
            if fxx_values[-1] > max_fxx:
                raise HrrrCastWeatherError(
                    f"cycle only covers through f{max_fxx:02d}, "
                    f"but the run needs f{fxx_values[-1]:02d}"
                )

            idx_by_hour: dict[int, str] = {}
            for fxx in fxx_values:
                idx_url = hrrrcast_idx_url(source_base_url, candidate_cycle, selected_member, fxx)
                idx_text = fetcher(idx_url)
                required_message_ranges(idx_text)
                idx_by_hour[fxx] = idx_text
            return HrrrCastCyclePlan(
                cycle=candidate_cycle,
                forecast_hours=fxx_values,
                idx_text_by_hour=idx_by_hour,
            )
        except Exception as exc:
            errors.append(f"{candidate_cycle:%Y-%m-%d %H:%M}Z: {exc}")
            if log_failures:
                logger.warning(f"HRRRCast cycle failed: {errors[-1]}")

    detail = "\n  - ".join(errors)
    raise HrrrCastWeatherError(f"Could not prepare HRRRCast weather file:\n  - {detail}")


def _hrrrcast_cycle_candidates(
    *,
    start_time: dt.datetime,
    max_cycle_rewind: int,
    base_url: str,
    explicit_cycle: dt.datetime | None,
    fetch_text: Callable[[str], str],
) -> tuple[dt.datetime, ...]:
    if explicit_cycle is not None:
        return tuple(
            resolve_cycle_candidates(
                start_time,
                cycle_interval_hours=1,
                max_cycle_rewind=max_cycle_rewind,
                explicit_cycle=explicit_cycle,
            )
        )

    try:
        candidates = discover_hrrrcast_cycle_candidates(
            start_time=start_time,
            max_cycle_rewind=max_cycle_rewind,
            base_url=base_url,
            fetch_text=fetch_text,
        )
        if candidates:
            return candidates
    except Exception as exc:
        logger.warning(
            f"Could not list HRRRCast cycle prefixes; falling back to hourly probing: {exc}"
        )

    return tuple(
        resolve_cycle_candidates(
            start_time,
            cycle_interval_hours=1,
            max_cycle_rewind=max_cycle_rewind,
            explicit_cycle=None,
        )
    )


def discover_hrrrcast_cycle_candidates(
    *,
    start_time: dt.datetime,
    max_cycle_rewind: int,
    base_url: str | None = None,
    fetch_text: Callable[[str], str] | None = None,
) -> tuple[dt.datetime, ...]:
    """List actual HRRRCast cycle prefixes in the S3 bucket, newest first."""
    source_base_url = base_url or config_loader.HRRRCAST_BASE_URL
    fetcher = fetch_text or _fetch_text
    earliest = start_time - dt.timedelta(hours=max(0, max_cycle_rewind))
    cycles: set[dt.datetime] = set()

    day = earliest.date()
    while day <= start_time.date():
        for prefix in list_hrrrcast_cycle_prefixes(
            source_base_url,
            day,
            fetch_text=fetcher,
        ):
            cycle = _parse_hrrrcast_cycle_prefix(prefix)
            if cycle is None:
                continue
            if earliest <= cycle <= start_time:
                cycles.add(cycle)
        day += dt.timedelta(days=1)

    return tuple(sorted(cycles, reverse=True))


def list_hrrrcast_cycle_prefixes(
    base_url: str,
    day: dt.date,
    *,
    fetch_text: Callable[[str], str] | None = None,
) -> tuple[str, ...]:
    fetcher = fetch_text or _fetch_text
    url = hrrrcast_s3_list_url(base_url, prefix=f"HRRRCast/{day:%Y%m%d}/", delimiter="/")
    text = fetcher(url)
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        raise HrrrCastWeatherError(f"failed to parse HRRRCast bucket listing {url}: {exc}") from exc

    prefixes: list[str] = []
    for element in root.findall(".//{*}CommonPrefixes/{*}Prefix"):
        if element.text:
            prefixes.append(element.text)
    return tuple(prefixes)


def hrrrcast_s3_list_url(base_url: str, *, prefix: str, delimiter: str = "/") -> str:
    query = urlencode({"list-type": "2", "prefix": prefix, "delimiter": delimiter})
    return f"{base_url.rstrip('/')}/?{query}"


def _parse_hrrrcast_cycle_prefix(prefix: str) -> dt.datetime | None:
    parts = prefix.strip("/").split("/")
    if len(parts) != 3 or parts[0] != "HRRRCast":
        return None
    try:
        return dt.datetime.strptime(f"{parts[1]}{parts[2]}", "%Y%m%d%H")
    except ValueError:
        return None


def hrrrcast_max_forecast_hour(cycle: dt.datetime) -> int:
    return 48 if cycle.hour in {0, 6, 12, 18} else 18


def hrrrcast_output_path(root: Path, cycle: dt.datetime, member: str, domain_key: str) -> Path:
    # WindNinja 3.12.2's generic NetCDF height reader only honors the caller's
    # height variable when the forecast filename contains "GFS".
    return root / cycle.strftime("%Y%m%d%H") / member / domain_key / "windninja_generic_GFS.nc"


def hrrrcast_key(cycle: dt.datetime, member: str, fxx: int) -> str:
    selected_member = normalize_hrrrcast_member(member)
    return (
        f"HRRRCast/{cycle:%Y%m%d}/{cycle:%H}/"
        f"hrrrcast.{selected_member}.t{cycle:%H}z.pgrb2.f{fxx:02d}"
    )


def hrrrcast_grib_url(base_url: str, cycle: dt.datetime, member: str, fxx: int) -> str:
    return f"{base_url.rstrip('/')}/{hrrrcast_key(cycle, member, fxx)}"


def hrrrcast_idx_url(base_url: str, cycle: dt.datetime, member: str, fxx: int) -> str:
    return f"{hrrrcast_grib_url(base_url, cycle, member, fxx)}.idx"


def parse_idx_records(idx_text: str) -> list[HrrrCastIndexRecord]:
    raw_records: list[tuple[int, int, str, str, str]] = []
    for raw_line in idx_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = line.split(":")
        if len(parts) < 6:
            continue
        try:
            message_number = int(parts[0])
            byte_start = int(parts[1])
        except ValueError:
            continue
        raw_records.append((message_number, byte_start, parts[3], parts[4], line))

    records: list[HrrrCastIndexRecord] = []
    for index, raw_record in enumerate(raw_records):
        message_number, byte_start, variable, level, line = raw_record
        byte_end = raw_records[index + 1][1] - 1 if index + 1 < len(raw_records) else None
        records.append(
            HrrrCastIndexRecord(
                message_number=message_number,
                byte_start=byte_start,
                byte_end=byte_end,
                variable=variable,
                level=level,
                line=line,
            )
        )
    return records


def required_message_ranges(idx_text: str) -> dict[str, HrrrCastIndexRecord]:
    records = parse_idx_records(idx_text)
    selected: dict[str, HrrrCastIndexRecord] = {}
    for field, (variable, level) in _HRRRCAST_FIELD_SELECTORS.items():
        record = _select_record(records, variable=variable, level=level)
        if record is None:
            raise HrrrCastWeatherError(
                f"HRRRCast index missing {variable}:{level}."
            )
        selected[field] = record
    return selected


def _select_record(
    records: list[HrrrCastIndexRecord],
    *,
    variable: str,
    level: str,
) -> HrrrCastIndexRecord | None:
    level_token = level.lower()
    for record in records:
        if record.variable != variable:
            continue
        if level_token in record.level.lower():
            return record
    return None


def download_hrrrcast_hour_fields(
    cycle: dt.datetime,
    fxx: int,
    member: str,
    *,
    bbox: tuple[float, float, float, float],
    cache_root: Path,
    base_url: str,
    idx_text: str,
) -> dict:
    subset_path = hrrrcast_subset_path(cache_root, cycle, member, fxx)
    if not subset_path.exists() or subset_path.stat().st_size == 0:
        _download_hrrrcast_subset(
            subset_path,
            grib_url=hrrrcast_grib_url(base_url, cycle, member, fxx),
            idx_text=idx_text,
        )
    datasets = open_hrrrcast_datasets(subset_path, bbox=bbox)
    try:
        return fields_from_datasets(
            datasets,
            valid_time=cycle + dt.timedelta(hours=fxx),
            spec=_FIELD_SPEC,
        )
    finally:
        for ds in datasets.values():
            try:
                ds.close()
            except Exception:
                pass


def hrrrcast_subset_path(root: Path, cycle: dt.datetime, member: str, fxx: int) -> Path:
    selected_member = normalize_hrrrcast_member(member)
    return (
        root
        / "grib"
        / cycle.strftime("%Y%m%d%H")
        / selected_member
        / f"hrrrcast.{selected_member}.t{cycle:%H}z.pgrb2.f{fxx:02d}.windninja.grib2"
    )


def _download_hrrrcast_subset(subset_path: Path, *, grib_url: str, idx_text: str) -> Path:
    ranges = required_message_ranges(idx_text)
    subset_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=subset_path.parent, delete=False) as tmp:
        tmp_path = Path(tmp.name)
        try:
            for field in HRRRCAST_REQUIRED_FIELDS:
                record = ranges[field]
                chunk = _fetch_range(grib_url, record.byte_start, record.byte_end)
                tmp.write(chunk)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise
    tmp_path.replace(subset_path)
    if subset_path.stat().st_size == 0:
        subset_path.unlink(missing_ok=True)
        raise HrrrCastWeatherError(f"Downloaded HRRRCast subset is empty: {subset_path}")
    return subset_path


def open_hrrrcast_datasets(
    grib_path: Path,
    *,
    bbox: tuple[float, float, float, float],
) -> dict:
    try:
        import xarray as xr
    except ImportError as exc:
        raise HrrrCastWeatherError(
            "HRRRCast support requires the Docker image with cfgrib/xarray installed. "
            "Rebuild with ./deploy/gcp/mwn.sh build-local."
        ) from exc

    datasets: dict[str, object] = {}
    for field in HRRRCAST_REQUIRED_FIELDS:
        last_error: Exception | None = None
        for filter_keys in _HRRRCAST_FIELD_FILTERS[field]:
            try:
                ds = xr.open_dataset(
                    grib_path,
                    engine="cfgrib",
                    backend_kwargs={
                        "indexpath": "",
                        "filter_by_keys": filter_keys,
                    },
                )
                datasets[field] = clip_dataset_to_bbox(ds, bbox)
                break
            except Exception as exc:
                last_error = exc
        if field not in datasets:
            raise HrrrCastWeatherError(
                f"Could not open HRRRCast {field} from {grib_path}: {last_error}"
            )
    return datasets


def _fetch_text(url: str, *, timeout: int = 60) -> str:
    try:
        with urlopen(Request(url, headers={"User-Agent": "mountain-windninja"}), timeout=timeout) as response:
            return response.read().decode("utf-8")
    except (HTTPError, URLError, TimeoutError) as exc:
        raise HrrrCastWeatherError(f"failed to fetch {url}: {exc}") from exc


def _fetch_range(url: str, start: int, end: int | None, *, timeout: int = 120) -> bytes:
    range_header = f"bytes={start}-" if end is None else f"bytes={start}-{end}"
    request = Request(
        url,
        headers={
            "Range": range_header,
            "User-Agent": "mountain-windninja",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            data = response.read()
    except (HTTPError, URLError, TimeoutError) as exc:
        raise HrrrCastWeatherError(
            f"failed to fetch {url} range {range_header}: {exc}"
        ) from exc
    if not data:
        raise HrrrCastWeatherError(f"empty HRRRCast range {range_header} from {url}")
    return data
