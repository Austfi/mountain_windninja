#!/usr/bin/env python3
"""Build a local WindNinja weather-model NetCDF from Herbie data."""
from __future__ import annotations

import datetime as dt
import json
import math
from pathlib import Path
import re
import subprocess
from typing import Any

try:
    from . import config_loader, utils
    from .weather_models import HerbieModelSpec, resolve_herbie_model
except ImportError:
    import config_loader
    import utils
    from weather_models import HerbieModelSpec, resolve_herbie_model


logger = utils.setup_logging("herbie_wx_model")

REQUIRED_OUTPUT_VARIABLES = (
    "U-component_of_wind_height_above_ground",
    "V-component_of_wind_height_above_ground",
    "Temperature_height_above_ground",
    "Total_cloud_cover",
)
WGS84_WKT = (
    'GEOGCS["WGS 84",DATUM["WGS_1984",'
    'SPHEROID["WGS 84",6378137,298.257223563]],'
    'PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433],'
    'AUTHORITY["EPSG","4326"]]'
)


class HerbieWeatherError(RuntimeError):
    """Raised for Herbie weather-file preparation failures."""


def parse_extra_values(raw_values: list[str] | None) -> dict[str, str | int | float | bool]:
    extras: dict[str, str | int | float | bool] = {}
    for raw in raw_values or []:
        if "=" not in raw:
            raise HerbieWeatherError(f"--herbie-extra must be KEY=VALUE, got: {raw!r}")
        key, value = raw.split("=", 1)
        key = key.strip()
        if not key:
            raise HerbieWeatherError(f"--herbie-extra has an empty key: {raw!r}")
        extras[key] = _parse_scalar(value.strip())
    return extras


def _parse_scalar(value: str) -> str | int | float | bool:
    lower = value.lower()
    if lower in {"true", "false"}:
        return lower == "true"
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def parse_priority(raw_priority: str | None) -> list[str] | None:
    if not raw_priority:
        return None
    values = [item.strip() for item in raw_priority.split(",") if item.strip()]
    return values or None


def domain_bbox_latlon(domain_config) -> tuple[float, float, float, float]:
    """Return west, south, east, north for a domain elevation file."""
    command = ["gdalinfo", "-json", str(domain_config.elevation_file)]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise HerbieWeatherError(f"gdalinfo failed for {domain_config.elevation_file}: {detail}")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise HerbieWeatherError(f"Could not parse gdalinfo JSON for {domain_config.elevation_file}") from exc

    coordinates = (((payload.get("wgs84Extent") or {}).get("coordinates") or [[]])[0])
    if coordinates:
        lons = [float(point[0]) for point in coordinates]
        lats = [float(point[1]) for point in coordinates]
        return min(lons), min(lats), max(lons), max(lats)

    corners = payload.get("cornerCoordinates") or {}
    wgs84_corners = [corners.get(name) for name in ("upperLeft", "lowerLeft", "lowerRight", "upperRight")]
    if all(point and len(point) >= 2 for point in wgs84_corners):
        lons = [float(point[0]) for point in wgs84_corners]
        lats = [float(point[1]) for point in wgs84_corners]
        return min(lons), min(lats), max(lons), max(lats)

    raise HerbieWeatherError(
        f"gdalinfo JSON did not include a WGS84 extent for {domain_config.elevation_file}"
    )


def buffered_bbox(
    bbox: tuple[float, float, float, float],
    *,
    resolution_km: float,
    cells: int = 3,
) -> tuple[float, float, float, float]:
    west, south, east, north = bbox
    lat_mid = (south + north) / 2.0
    lat_buffer = max(0.1, resolution_km * cells / 111.0)
    lon_scale = max(0.2, math.cos(math.radians(lat_mid)))
    lon_buffer = max(0.1, resolution_km * cells / (111.0 * lon_scale))
    return west - lon_buffer, south - lat_buffer, east + lon_buffer, north + lat_buffer


def resolve_cycle_candidates(
    start_time: dt.datetime,
    *,
    cycle_interval_hours: int,
    max_cycle_rewind: int,
    explicit_cycle: dt.datetime | None = None,
) -> list[dt.datetime]:
    if explicit_cycle:
        return [_strip_tz(explicit_cycle)]

    start_time = _strip_tz(start_time)
    interval = max(1, int(cycle_interval_hours))
    cycle_hour = (start_time.hour // interval) * interval
    first = start_time.replace(hour=cycle_hour, minute=0, second=0, microsecond=0)
    return [
        first - dt.timedelta(hours=interval * offset)
        for offset in range(max(0, max_cycle_rewind) + 1)
    ]


def _strip_tz(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(dt.timezone.utc).replace(tzinfo=None)


def forecast_hours_for_window(
    cycle: dt.datetime,
    start_time: dt.datetime,
    stop_time: dt.datetime,
) -> list[int]:
    start_time = _strip_tz(start_time)
    stop_time = _strip_tz(stop_time)
    if stop_time <= start_time:
        raise HerbieWeatherError("Herbie forecast stop time must be after start time.")

    first = int((start_time - cycle).total_seconds() / 3600)
    last = int((stop_time - cycle).total_seconds() / 3600)
    if first < 0:
        raise HerbieWeatherError(
            f"Resolved Herbie cycle {cycle:%Y-%m-%d %H:%M} UTC is after the run start."
        )
    return list(range(first, last + 1))


def prepare_herbie_wx_model(
    *,
    model_name: str,
    start_time: dt.datetime,
    stop_time: dt.datetime,
    domain_config,
    cache_root: str | Path | None = None,
    priority: str | list[str] | None = None,
    max_cycle_rewind: int | None = None,
    cycle: dt.datetime | None = None,
    product: str | None = None,
    member: str | int | None = None,
    domain: str | None = None,
    extra: dict[str, str | int | float | bool] | None = None,
) -> Path:
    spec = resolve_herbie_model(model_name)
    root = Path(cache_root or config_loader.HERBIE_CACHE_DIR)
    priority_value = priority if isinstance(priority, list) else parse_priority(priority)
    if priority_value is None:
        priority_value = parse_priority(config_loader.HERBIE_PRIORITY)
    rewind_count = (
        config_loader.HERBIE_MAX_CYCLE_REWIND
        if max_cycle_rewind is None
        else max_cycle_rewind
    )

    bbox = buffered_bbox(domain_bbox_latlon(domain_config), resolution_km=spec.resolution_km)
    errors: list[str] = []
    for candidate_cycle in resolve_cycle_candidates(
        start_time,
        cycle_interval_hours=spec.cycle_interval_hours,
        max_cycle_rewind=rewind_count,
        explicit_cycle=cycle,
    ):
        try:
            fxx_values = forecast_hours_for_window(candidate_cycle, start_time, stop_time)
            output_path = _output_path(root, spec, candidate_cycle, domain_config.key)
            if output_path.exists():
                try:
                    validate_windninja_generic_netcdf(output_path)
                    logger.info(f"Using cached Herbie WindNinja file: {output_path}")
                    return output_path
                except HerbieWeatherError as exc:
                    logger.warning(
                        f"Cached Herbie WindNinja file is invalid and will be rebuilt: {exc}"
                    )
                    output_path.unlink(missing_ok=True)

            logger.info(
                "Preparing Herbie weather file: "
                f"{spec.name} cycle={candidate_cycle:%Y-%m-%d %H:%M}Z "
                f"fxx={fxx_values[0]}..{fxx_values[-1]}"
            )
            hourly = [
                fetch_hour_fields(
                    spec,
                    candidate_cycle,
                    fxx,
                    bbox=bbox,
                    cache_root=root,
                    priority=priority_value,
                    product=product,
                    member=member,
                    domain=domain,
                    extra=extra,
                )
                for fxx in fxx_values
            ]
            write_windninja_generic_netcdf(hourly, output_path)
            return output_path
        except Exception as exc:
            errors.append(f"{candidate_cycle:%Y-%m-%d %H:%M}Z: {exc}")
            logger.warning(f"Herbie cycle failed: {errors[-1]}")

    detail = "\n  - ".join(errors)
    raise HerbieWeatherError(f"Could not prepare Herbie weather file:\n  - {detail}")


def _output_path(root: Path, spec: HerbieModelSpec, cycle: dt.datetime, domain_key: str) -> Path:
    # WindNinja 3.12.2's generic NetCDF height reader only honors the caller's
    # height variable when the forecast filename contains "GFS".
    return (
        root
        / spec.name.lower()
        / cycle.strftime("%Y%m%d%H")
        / domain_key
        / "windninja_generic_GFS.nc"
    )


def fetch_hour_fields(
    spec: HerbieModelSpec,
    cycle: dt.datetime,
    fxx: int,
    *,
    bbox: tuple[float, float, float, float],
    cache_root: Path,
    priority: list[str] | None,
    product: str | None,
    member: str | int | None,
    domain: str | None,
    extra: dict[str, str | int | float | bool] | None,
) -> dict[str, Any]:
    try:
        from herbie import Herbie
    except ImportError as exc:
        raise HerbieWeatherError(
            "Herbie support requires the Docker image with herbie-data/cfgrib/xarray installed. "
            "Rebuild with ./deploy/gcp/mwn.sh build-local."
        ) from exc

    datasets: dict[str, Any] = {}
    base_kwargs = spec.herbie_kwargs(product=product, member=member, domain=domain, extra=extra)
    for field in ("u10", "v10", "t2m", "tcc"):
        field_errors: list[str] = []
        fetches = _field_fetches(spec, field)
        for search, field_kwargs in fetches:
            try:
                kwargs = dict(base_kwargs)
                kwargs.update(field_kwargs)
                herbie = Herbie(
                    cycle,
                    fxx=fxx,
                    priority=priority,
                    save_dir=cache_root,
                    verbose=False,
                    **kwargs,
                )
                _apply_source_overrides(herbie, spec, cycle, fxx, kwargs)
                ds = _open_herbie_dataset(
                    herbie,
                    spec,
                    field,
                    search=search,
                    cache_root=cache_root,
                )
                datasets[field] = clip_dataset_to_bbox(ds, bbox)
                break
            except Exception as exc:
                label = search if search is not None else _field_fetch_label(field_kwargs)
                field_errors.append(f"{label}: {exc}")

        if field not in datasets and field == "tcc":
            logger.warning(
                f"{spec.name} f{fxx:03d}: cloud cover unavailable; using 0 percent cloud cover."
            )
            datasets[field] = None
        elif field not in datasets:
            detail = "; ".join(field_errors)
            raise HerbieWeatherError(f"{spec.name} f{fxx:03d}: missing {field} ({detail})")

    valid_time = cycle + dt.timedelta(hours=fxx)
    return fields_from_datasets(datasets, valid_time=valid_time, spec=spec)


def _field_fetches(
    spec: HerbieModelSpec,
    field: str,
) -> tuple[tuple[str | None, dict[str, str | int | float | bool]], ...]:
    if spec.fetch_strategy == "indexed":
        return tuple((search, {}) for search in _search_patterns(spec, field))
    if spec.fetch_strategy == "single_message":
        field_kwargs = (spec.field_extra or {}).get(field)
        if not field_kwargs:
            return ((None, {}),)
        return ((None, dict(field_kwargs)),)
    raise HerbieWeatherError(
        f"Unsupported Herbie fetch strategy for {spec.name}: {spec.fetch_strategy}"
    )


def _field_fetch_label(field_kwargs: dict[str, str | int | float | bool]) -> str:
    if not field_kwargs:
        return "full-field file"
    return ",".join(f"{key}={value}" for key, value in sorted(field_kwargs.items()))


def _search_patterns(spec: HerbieModelSpec, field: str) -> tuple[str | None, ...]:
    patterns = (spec.search_patterns or {}).get(field)
    if not patterns:
        raise HerbieWeatherError(f"No Herbie search pattern configured for {spec.name} {field}")
    return patterns


def _open_herbie_dataset(
    herbie,
    spec: HerbieModelSpec,
    field: str,
    *,
    search: str | None,
    cache_root: Path,
):
    if spec.fetch_strategy == "indexed":
        if search is None:
            raise HerbieWeatherError(f"{spec.name} {field}: indexed fetch requires a search regex.")
        _validate_inventory_match(herbie, spec, field, search)
        downloaded = herbie.download(
            search=search,
            save_dir=cache_root,
            errors="raise",
            verbose=False,
        )
        _validate_downloaded_path(downloaded, spec, field, search)
        return herbie.xarray(
            search=search,
            remove_grib=False,
            save_dir=cache_root,
            errors="raise",
            verbose=False,
        )

    if spec.fetch_strategy == "single_message":
        return herbie.xarray(
            remove_grib=False,
            save_dir=cache_root,
            errors="raise",
            verbose=False,
        )

    raise HerbieWeatherError(
        f"Unsupported Herbie fetch strategy for {spec.name}: {spec.fetch_strategy}"
    )


def _validate_inventory_match(herbie, spec: HerbieModelSpec, field: str, search: str) -> None:
    inventory = herbie.inventory(search=search, verbose=False)
    count = len(inventory)
    if count < 1:
        raise HerbieWeatherError(
            f"{spec.name} {field}: inventory had no matches for regex {search!r}."
        )
    if count > 1:
        logger.warning(
            f"{spec.name} {field}: inventory regex {search!r} matched {count} messages: "
            f"{'; '.join(_inventory_search_samples(inventory))}"
        )


def _inventory_search_samples(inventory, limit: int = 5) -> list[str]:
    columns = getattr(inventory, "columns", ())
    if "search_this" not in columns:
        return []
    try:
        values = inventory["search_this"].head(limit).tolist()
    except AttributeError:
        values = list(inventory["search_this"])[:limit]
    return [str(value) for value in values]


def _validate_downloaded_path(
    downloaded: Any,
    spec: HerbieModelSpec,
    field: str,
    search: str,
) -> None:
    if downloaded is None:
        raise HerbieWeatherError(
            f"{spec.name} {field}: Herbie did not return a downloaded file for {search!r}."
        )
    path = Path(str(downloaded))
    if not path.exists():
        raise HerbieWeatherError(
            f"{spec.name} {field}: downloaded subset is missing for {search!r}: {path}"
        )


def _apply_source_overrides(
    herbie,
    spec: HerbieModelSpec,
    cycle: dt.datetime,
    fxx: int,
    kwargs: dict[str, str | int | float | bool],
) -> None:
    if spec.name != "RRFS":
        return

    domain = str(kwargs.get("domain") or "conus")
    url = _current_rrfs_aws_url(cycle, fxx, domain=domain)
    herbie.product = "2dfld"
    herbie.SOURCES = {"aws": url}
    herbie.LOCALFILE = f"{cycle:%Y%m%d%H}/rrfs.t{cycle:%H}z.2dfld.{_rrfs_grid_label(domain)}.f{fxx:03d}.{_rrfs_domain_label(domain)}.grib2"
    herbie.grib, herbie.grib_source = herbie.find_grib()
    herbie.idx, herbie.idx_source = herbie.find_idx()
    herbie.__dict__.pop("index_as_dataframe", None)


def _current_rrfs_aws_url(cycle: dt.datetime, fxx: int, *, domain: str = "conus") -> str:
    domain_label = _rrfs_domain_label(domain)
    grid = _rrfs_grid_label(domain)
    return (
        "https://noaa-rrfs-pds.s3.amazonaws.com/"
        f"rrfs_a/rrfs.{cycle:%Y%m%d}/{cycle:%H}/"
        f"rrfs.t{cycle:%H}z.2dfld.{grid}.f{fxx:03d}.{domain_label}.grib2"
    )


def _rrfs_domain_label(domain: str) -> str:
    normalized = domain.strip().lower().replace("_", " ").replace("-", " ")
    aliases = {
        "conus": "conus",
        "ak": "ak",
        "alaska": "ak",
        "hi": "hi",
        "hawaii": "hi",
        "pr": "pr",
        "puerto rico": "pr",
    }
    try:
        return aliases[normalized]
    except KeyError as exc:
        raise HerbieWeatherError(
            "RRFS Herbie source override only supports conus, alaska, hawaii, and puerto rico domains."
        ) from exc


def _rrfs_grid_label(domain: str) -> str:
    domain_label = _rrfs_domain_label(domain)
    return "2p5km" if domain_label in {"hi", "pr"} else "3km"


def clip_dataset_to_bbox(ds, bbox: tuple[float, float, float, float]):
    west, south, east, north = bbox
    if "latitude" not in ds.coords or "longitude" not in ds.coords:
        return ds

    lat = ds["latitude"]
    lon = ds["longitude"]
    if lat.ndim == 1 and lon.ndim == 1:
        lat_values = lat.values
        lon_values = _normalize_longitudes(lon.values)
        y_idx = _indices_between(lat_values, south, north)
        x_idx = _indices_between(lon_values, west, east)
        if y_idx and x_idx:
            return ds.isel({lat.dims[0]: slice(min(y_idx), max(y_idx) + 1),
                            lon.dims[0]: slice(min(x_idx), max(x_idx) + 1)})
        raise HerbieWeatherError(
            "Herbie source grid does not overlap the domain bbox "
            f"west={west:.4f}, south={south:.4f}, east={east:.4f}, north={north:.4f}."
        )

    if lat.ndim == 2 and lon.ndim == 2:
        import numpy as np

        lon_values = _normalize_longitudes(lon.values)
        mask = (
            (lat.values >= south)
            & (lat.values <= north)
            & (lon_values >= west)
            & (lon_values <= east)
        )
        rows, cols = np.where(mask)
        if rows.size and cols.size:
            y_dim, x_dim = lat.dims
            return ds.isel({
                y_dim: slice(int(rows.min()), int(rows.max()) + 1),
                x_dim: slice(int(cols.min()), int(cols.max()) + 1),
            })
        raise HerbieWeatherError(
            "Herbie source grid does not overlap the domain bbox "
            f"west={west:.4f}, south={south:.4f}, east={east:.4f}, north={north:.4f}."
        )
    return ds


def _normalize_longitudes(values):
    import numpy as np

    arr = np.asarray(values, dtype="float64")
    return ((arr + 180.0) % 360.0) - 180.0


def _indices_between(values, lower: float, upper: float) -> list[int]:
    return [idx for idx, value in enumerate(values) if lower <= float(value) <= upper]


def fields_from_datasets(
    datasets: dict[str, Any],
    *,
    valid_time: dt.datetime,
    spec: HerbieModelSpec,
) -> dict[str, Any]:
    import numpy as np

    u = _extract_array(datasets["u10"], spec, "u10", valid_time=valid_time)
    v = _extract_array(datasets["v10"], spec, "v10", valid_time=valid_time)
    t = _extract_array(datasets["t2m"], spec, "t2m", valid_time=valid_time)
    if datasets["tcc"] is None:
        cloud = np.zeros_like(u["values"], dtype="float32")
        lat = u["latitude"]
        lon = u["longitude"]
    else:
        cloud_values = _extract_array(datasets["tcc"], spec, "tcc", valid_time=valid_time)
        cloud = cloud_values["values"]
        lat = cloud_values["latitude"]
        lon = cloud_values["longitude"]
        if float(np.nanmax(cloud)) <= 1.0:
            cloud = cloud * 100.0

    return {
        "time": valid_time,
        "u10": u["values"].astype("float32"),
        "v10": v["values"].astype("float32"),
        "t2m": t["values"].astype("float32"),
        "tcc": cloud.astype("float32"),
        "latitude": lat.astype("float32"),
        "longitude": lon.astype("float32"),
    }


def _extract_array(
    ds,
    spec: HerbieModelSpec,
    field: str,
    *,
    valid_time: dt.datetime | None = None,
) -> dict[str, Any]:
    if ds is None:
        raise HerbieWeatherError(f"{field} dataset is missing")
    data_var = _select_data_var(ds, (spec.variable_aliases or {}).get(field, (field,)))
    arr = _select_spatial_slice(data_var, ds, field, valid_time=valid_time)
    dims = _spatial_dims_for(arr, ds)
    values = arr.transpose(*dims).values
    lat, lon = _latitude_longitude_for(ds, dims)
    return {"values": values, "latitude": lat, "longitude": lon}


def _select_spatial_slice(arr, ds, field: str, *, valid_time: dt.datetime | None):
    arr = arr.squeeze(drop=True)
    spatial_dims = set(_spatial_dims_for(arr, ds))
    for dim in list(arr.dims):
        if dim in spatial_dims:
            continue
        size = int(arr.sizes[dim])
        if size == 1:
            arr = arr.isel({dim: 0}, drop=True)
            spatial_dims = set(_spatial_dims_for(arr, ds))
            continue
        index = _valid_time_index(ds, dim, valid_time)
        if index is not None:
            arr = arr.isel({dim: index}, drop=True)
            spatial_dims = set(_spatial_dims_for(arr, ds))
            continue
        raise HerbieWeatherError(
            f"{field} dataset has non-spatial dimension {dim!r} with {size} values "
            "and no matching valid_time coordinate."
        )

    arr = arr.squeeze(drop=True)
    dims = _spatial_dims_for(arr, ds)
    if any(dim not in arr.dims for dim in dims) or len(arr.dims) != 2:
        raise HerbieWeatherError(
            f"{field} dataset could not be reduced to one spatial grid. Dims: {arr.dims}"
        )
    return arr


def _spatial_dims_for(arr, ds) -> tuple[str, str]:
    if "latitude" in ds.coords and "longitude" in ds.coords:
        lat = ds["latitude"]
        lon = ds["longitude"]
        if lat.ndim == 2 and lon.ndim == 2 and set(lat.dims).issubset(arr.dims):
            return lat.dims
        if lat.ndim == 1 and lon.ndim == 1:
            dims = (lat.dims[0], lon.dims[0])
            if set(dims).issubset(arr.dims):
                return dims
    return arr.dims[-2:]


def _valid_time_index(ds, dim: str, valid_time: dt.datetime | None) -> int | None:
    if valid_time is None:
        return None
    if "valid_time" in ds.coords:
        coord = ds["valid_time"]
        if dim in coord.dims and int(coord.sizes[dim]) > 1:
            index = _datetime_index(coord.values, valid_time)
            if index is not None:
                return index
    if dim in ds.coords:
        coord = ds[dim]
        index = _datetime_index(coord.values, valid_time)
        if index is not None:
            return index
    if dim == "step" and "time" in ds.coords and dim in ds.coords:
        return _step_index(ds, dim, valid_time)
    return None


def _datetime_index(values, valid_time: dt.datetime) -> int | None:
    import numpy as np

    arr = np.asarray(values)
    if not np.issubdtype(arr.dtype, np.datetime64):
        return None
    target = np.datetime64(_strip_tz(valid_time), "s")
    flattened = arr.astype("datetime64[s]").reshape(-1)
    matches = np.where(flattened == target)[0]
    if matches.size:
        return int(matches[0])
    return None


def _step_index(ds, dim: str, valid_time: dt.datetime) -> int | None:
    import numpy as np

    steps = np.asarray(ds[dim].values)
    if not np.issubdtype(steps.dtype, np.timedelta64):
        return None
    reference_values = np.asarray(ds["time"].values)
    if not np.issubdtype(reference_values.dtype, np.datetime64):
        return None
    reference = reference_values.reshape(-1)[0].astype("datetime64[s]")
    target = np.datetime64(_strip_tz(valid_time), "s")
    wanted_step = target - reference
    flattened = steps.astype("timedelta64[s]").reshape(-1)
    matches = np.where(flattened == wanted_step.astype("timedelta64[s]"))[0]
    if matches.size:
        return int(matches[0])
    return None


def _select_data_var(ds, aliases: tuple[str, ...]):
    alias_tokens = {_compact(alias) for alias in aliases}
    candidates = []
    for name, value in ds.data_vars.items():
        haystack = " ".join(
            str(item)
            for item in (
                name,
                value.attrs.get("GRIB_shortName", ""),
                value.attrs.get("GRIB_name", ""),
                value.attrs.get("long_name", ""),
                value.attrs.get("standard_name", ""),
            )
        )
        compact = _compact(haystack)
        if any(alias in compact for alias in alias_tokens):
            candidates.append(value)
    if len(candidates) == 1:
        return candidates[0]
    if not candidates and len(ds.data_vars) == 1:
        return next(iter(ds.data_vars.values()))
    names = ", ".join(ds.data_vars)
    raise HerbieWeatherError(f"Could not select one variable from dataset. Found: {names}")


def _compact(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _latitude_longitude_for(ds, dims: tuple[str, str]):
    import numpy as np

    if "latitude" in ds.coords and "longitude" in ds.coords:
        lat = ds["latitude"]
        lon = ds["longitude"]
        if lat.ndim == 2 and lon.ndim == 2:
            return lat.transpose(*dims).values, _normalize_longitudes(lon.transpose(*dims).values)
        if lat.ndim == 1 and lon.ndim == 1:
            lon2, lat2 = np.meshgrid(_normalize_longitudes(lon.values), lat.values)
            return lat2, lon2

    y_size = int(ds.sizes[dims[0]])
    x_size = int(ds.sizes[dims[1]])
    y = np.arange(y_size, dtype="float32")
    x = np.arange(x_size, dtype="float32")
    lon2, lat2 = np.meshgrid(x, y)
    return lat2, lon2


def write_windninja_generic_netcdf(hourly: list[dict[str, Any]], output_path: Path) -> Path:
    if not hourly:
        raise HerbieWeatherError("No hourly Herbie fields were provided.")

    import numpy as np
    import xarray as xr

    output_path.parent.mkdir(parents=True, exist_ok=True)
    valid_times = [_strip_tz(item["time"]) for item in hourly]
    base_time = valid_times[0]
    times = np.array(
        [(valid_time - base_time).total_seconds() / 3600.0 for valid_time in valid_times],
        dtype="float64",
    )

    u = np.stack([item["u10"] for item in hourly], axis=0)
    v = np.stack([item["v10"] for item in hourly], axis=0)
    t = np.stack([item["t2m"] for item in hourly], axis=0)
    cloud = np.stack([item["tcc"] for item in hourly], axis=0)
    latitude = hourly[0]["latitude"]
    longitude = hourly[0]["longitude"]
    latitude_axis, longitude_axis = _regular_latlon_axes(latitude, longitude)
    latitude_axis, longitude_axis, u, v, t, cloud = _align_spatial_axes(
        latitude_axis,
        longitude_axis,
        u,
        v,
        t,
        cloud,
    )
    weather_attrs = {"grid_mapping": "crs"}

    ds = xr.Dataset(
        data_vars={
            "U-component_of_wind_height_above_ground": (
                ("time", "height_above_ground1", "lat", "lon"),
                u[:, np.newaxis, :, :],
                {
                    "units": "m s-1",
                    "long_name": "10 m U-component of wind",
                    **weather_attrs,
                },
            ),
            "V-component_of_wind_height_above_ground": (
                ("time", "height_above_ground1", "lat", "lon"),
                v[:, np.newaxis, :, :],
                {
                    "units": "m s-1",
                    "long_name": "10 m V-component of wind",
                    **weather_attrs,
                },
            ),
            "Temperature_height_above_ground": (
                ("time", "height_above_ground", "lat", "lon"),
                t[:, np.newaxis, :, :],
                {"units": "K", "long_name": "2 m temperature", **weather_attrs},
            ),
            "Total_cloud_cover": (
                ("time", "lat", "lon"),
                cloud,
                {"units": "%", "long_name": "Total cloud cover", **weather_attrs},
            ),
            "crs": (
                (),
                np.int32(0),
                {
                    "grid_mapping_name": "latitude_longitude",
                    "epsg_code": "EPSG:4326",
                    "spatial_ref": WGS84_WKT,
                },
            ),
        },
        coords={
            "time": (
                "time",
                times,
                {
                    "standard_name": "time",
                    "long_name": "time",
                    "units": f"hours since {base_time:%Y-%m-%dT%H:%M:%SZ}",
                    "calendar": "standard",
                },
            ),
            "height_above_ground": (
                "height_above_ground",
                np.array([2.0], dtype="float32"),
                {"units": "m", "positive": "up", "long_name": "height above ground"},
            ),
            "height_above_ground1": (
                "height_above_ground1",
                np.array([10.0], dtype="float32"),
                {"units": "m", "positive": "up", "long_name": "height above ground"},
            ),
            "lat": (
                "lat",
                latitude_axis.astype("float32"),
                {
                    "standard_name": "latitude",
                    "long_name": "latitude",
                    "units": "degrees_north",
                    "axis": "Y",
                },
            ),
            "lon": (
                "lon",
                longitude_axis.astype("float32"),
                {
                    "standard_name": "longitude",
                    "long_name": "longitude",
                    "units": "degrees_east",
                    "axis": "X",
                },
            ),
        },
        attrs={
            "Conventions": "CF-1.8",
            "title": "WindNinja generic weather model file generated from Herbie",
        },
    )
    encoding = {
        name: {"_FillValue": np.float32(-9999.0)}
        for name in REQUIRED_OUTPUT_VARIABLES
    }
    for coord_name in (
        "time",
        "height_above_ground",
        "height_above_ground1",
        "lat",
        "lon",
        "crs",
    ):
        encoding[coord_name] = {"_FillValue": None}
    ds.to_netcdf(output_path, encoding=encoding)
    validate_windninja_generic_netcdf(output_path)
    logger.info(f"Wrote Herbie WindNinja file: {output_path}")
    return output_path


def _regular_latlon_axes(latitude, longitude):
    import numpy as np

    lat = np.asarray(latitude, dtype="float64")
    lon = np.asarray(longitude, dtype="float64")
    if lat.ndim == 2:
        lat_axis = np.nanmean(lat, axis=1)
    elif lat.ndim == 1:
        lat_axis = lat
    else:
        raise HerbieWeatherError("Latitude coordinate must be one- or two-dimensional.")

    if lon.ndim == 2:
        lon_axis = np.nanmean(lon, axis=0)
    elif lon.ndim == 1:
        lon_axis = _normalize_longitudes(lon)
    else:
        raise HerbieWeatherError("Longitude coordinate must be one- or two-dimensional.")

    if lat_axis.size < 2 or lon_axis.size < 2:
        raise HerbieWeatherError("Herbie clip is too small to write a georeferenced weather grid.")
    return lat_axis, lon_axis


def _align_spatial_axes(latitude_axis, longitude_axis, *arrays):
    import numpy as np

    lat = np.asarray(latitude_axis, dtype="float64")
    lon = _normalize_longitudes(longitude_axis)
    aligned = [np.asarray(array) for array in arrays]

    if lat[0] < lat[-1]:
        lat = lat[::-1]
        aligned = [array[..., ::-1, :] for array in aligned]
    if lon[0] > lon[-1]:
        lon = lon[::-1]
        aligned = [array[..., ::-1] for array in aligned]

    return lat, lon, *aligned


def validate_windninja_generic_netcdf(path: Path) -> None:
    import xarray as xr

    ds = xr.open_dataset(path, decode_times=False, mask_and_scale=False)
    try:
        missing = [name for name in REQUIRED_OUTPUT_VARIABLES if name not in ds.variables]
        if missing:
            raise HerbieWeatherError(f"NetCDF missing required variables: {', '.join(missing)}")
        if "time" not in ds.coords and "time" not in ds.variables:
            raise HerbieWeatherError("NetCDF missing time coordinate.")
        if ds["time"].attrs.get("standard_name") != "time":
            raise HerbieWeatherError("NetCDF time coordinate must have standard_name=time.")
        if "units" not in ds["time"].attrs:
            raise HerbieWeatherError("NetCDF time coordinate must include units.")
        time_units = str(ds["time"].attrs.get("units", ""))
        if "hours since " not in time_units or "T" not in time_units.split("hours since ", 1)[1]:
            raise HerbieWeatherError("NetCDF time units must use ISO T form for WindNinja.")
        if "height_above_ground" not in ds.variables:
            raise HerbieWeatherError("NetCDF missing 10 m height dimension.")
        if "height_above_ground1" not in ds.variables:
            raise HerbieWeatherError("NetCDF missing 2 m height dimension.")
        for height_name in ("height_above_ground", "height_above_ground1"):
            units = str(ds[height_name].attrs.get("units", "")).lower()
            if units not in {"m", "meter", "meters", "metre", "metres"}:
                raise HerbieWeatherError(f"NetCDF {height_name} must have meter units.")
        expected_coord_names = {"latitude": "lat", "longitude": "lon"}
        for standard_name, expected_name in expected_coord_names.items():
            coord_name = _coordinate_with_standard_name(ds, standard_name)
            if coord_name is None:
                raise HerbieWeatherError(f"NetCDF missing {standard_name} coordinate.")
            if coord_name != expected_name:
                raise HerbieWeatherError(
                    f"NetCDF {standard_name} coordinate must be named {expected_name}."
                )
            if ds[coord_name].attrs.get("standard_name") != standard_name:
                raise HerbieWeatherError(
                    f"NetCDF {coord_name} coordinate must have standard_name={standard_name}."
                )
        if "crs" not in ds.variables:
            raise HerbieWeatherError("NetCDF missing WGS84 grid mapping variable.")
        if ds["crs"].attrs.get("grid_mapping_name") != "latitude_longitude":
            raise HerbieWeatherError("NetCDF grid mapping must be latitude_longitude.")
        for name in REQUIRED_OUTPUT_VARIABLES:
            if ds[name].attrs.get("grid_mapping") != "crs":
                raise HerbieWeatherError(f"NetCDF {name} must reference grid_mapping=crs.")
            fill_value = ds[name].attrs.get("_FillValue")
            if fill_value is None:
                raise HerbieWeatherError(f"NetCDF {name} must define a finite fill value.")
            if math.isnan(float(fill_value)):
                raise HerbieWeatherError(f"NetCDF {name} fill value must not be NaN.")
    finally:
        ds.close()


def _coordinate_with_standard_name(ds, standard_name: str) -> str | None:
    for name in ds.variables:
        if ds[name].attrs.get("standard_name") == standard_name:
            return name
    return None
