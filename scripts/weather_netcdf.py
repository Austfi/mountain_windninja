"""Shared WindNinja generic weather NetCDF helpers.

The implementation currently delegates to the Herbie adapter's vetted writer
and grid helpers so new weather sources produce the same file shape.
"""
from __future__ import annotations

try:
    from .herbie_wx_model import (
        REQUIRED_OUTPUT_VARIABLES,
        HerbieWeatherError as WxModelNetcdfError,
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
    from herbie_wx_model import (
        REQUIRED_OUTPUT_VARIABLES,
        HerbieWeatherError as WxModelNetcdfError,
        buffered_bbox,
        clip_dataset_to_bbox,
        domain_bbox_latlon,
        fields_from_datasets,
        forecast_hours_for_window,
        resolve_cycle_candidates,
        validate_windninja_generic_netcdf,
        write_windninja_generic_netcdf,
    )

__all__ = [
    "REQUIRED_OUTPUT_VARIABLES",
    "WxModelNetcdfError",
    "buffered_bbox",
    "clip_dataset_to_bbox",
    "domain_bbox_latlon",
    "fields_from_datasets",
    "forecast_hours_for_window",
    "resolve_cycle_candidates",
    "validate_windninja_generic_netcdf",
    "write_windninja_generic_netcdf",
]
