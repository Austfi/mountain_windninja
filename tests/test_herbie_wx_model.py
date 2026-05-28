from __future__ import annotations

import datetime as dt

import numpy as np
import pytest
import xarray as xr

from scripts import herbie_wx_model


def _hour(valid_time: dt.datetime, offset: float = 0.0) -> dict:
    y, x = np.mgrid[0:3, 0:4]
    return {
        "time": valid_time,
        "u10": (x + offset).astype("float32"),
        "v10": (y + offset).astype("float32"),
        "t2m": np.full((3, 4), 273.15 + offset, dtype="float32"),
        "tcc": np.full((3, 4), 25.0 + offset, dtype="float32"),
        "latitude": np.full((3, 4), 39.0, dtype="float32") + y * 0.01,
        "longitude": np.full((3, 4), -106.0, dtype="float32") + x * 0.01,
    }


def test_write_windninja_generic_netcdf_contains_required_variables(tmp_path):
    output = tmp_path / "windninja_generic.nc"
    herbie_wx_model.write_windninja_generic_netcdf(
        [
            _hour(dt.datetime(2026, 1, 1, 0, 0)),
            _hour(dt.datetime(2026, 1, 1, 1, 0), offset=1.0),
        ],
        output,
    )

    ds = xr.open_dataset(output)
    try:
        for name in herbie_wx_model.REQUIRED_OUTPUT_VARIABLES:
            assert name in ds.variables
            assert ds[name].attrs["grid_mapping"] == "crs"
        assert "time" in ds.variables
        assert "height_above_ground" in ds.variables
        assert "height_above_ground1" in ds.variables
        assert ds["U-component_of_wind_height_above_ground"].shape == (2, 1, 3, 4)
        assert ds["Temperature_height_above_ground"].shape == (2, 1, 3, 4)
        assert ds["Total_cloud_cover"].shape == (2, 3, 4)
    finally:
        ds.close()

    raw = xr.open_dataset(output, decode_times=False, mask_and_scale=False)
    try:
        assert raw["time"].attrs["standard_name"] == "time"
        assert raw["time"].attrs["units"] == "hours since 2026-01-01T00:00:00Z"
        assert raw["height_above_ground"].attrs["units"] == "m"
        assert raw["height_above_ground1"].attrs["units"] == "m"
        assert raw["height_above_ground"].values.tolist() == [2.0]
        assert raw["height_above_ground1"].values.tolist() == [10.0]
        assert raw["lat"].attrs["standard_name"] == "latitude"
        assert raw["lon"].attrs["standard_name"] == "longitude"
        assert raw["crs"].attrs["grid_mapping_name"] == "latitude_longitude"
        assert raw["lat"].values[0] > raw["lat"].values[-1]
        for name in herbie_wx_model.REQUIRED_OUTPUT_VARIABLES:
            assert raw[name].attrs["_FillValue"] == -9999.0
    finally:
        raw.close()


def test_output_path_uses_windninja_gfs_compat_filename(tmp_path):
    spec = herbie_wx_model.resolve_herbie_model("HRRR")

    output = herbie_wx_model._output_path(
        tmp_path,
        spec,
        dt.datetime(2026, 1, 1, 0, 0),
        "test_domain",
    )

    assert output.name == "windninja_generic_GFS.nc"


def test_validate_windninja_generic_netcdf_rejects_missing_variables(tmp_path):
    output = tmp_path / "bad.nc"
    xr.Dataset({"time": ("time", [np.datetime64("2026-01-01T00:00")])}).to_netcdf(output)

    with pytest.raises(herbie_wx_model.HerbieWeatherError, match="missing required variables"):
        herbie_wx_model.validate_windninja_generic_netcdf(output)


def test_resolve_cycle_candidates_rewinds_by_model_interval():
    start = dt.datetime(2026, 1, 1, 13, 0)

    candidates = herbie_wx_model.resolve_cycle_candidates(
        start,
        cycle_interval_hours=6,
        max_cycle_rewind=2,
    )

    assert candidates == [
        dt.datetime(2026, 1, 1, 12, 0),
        dt.datetime(2026, 1, 1, 6, 0),
        dt.datetime(2026, 1, 1, 0, 0),
    ]


def test_rrfs_current_aws_url_uses_2dfld_current_layout():
    url = herbie_wx_model._current_rrfs_aws_url(
        dt.datetime(2026, 5, 28, 3),
        4,
        domain="conus",
    )

    assert url == (
        "https://noaa-rrfs-pds.s3.amazonaws.com/"
        "rrfs_a/rrfs.20260528/03/rrfs.t03z.2dfld.3km.f004.conus.grib2"
    )


def test_rrfs_current_aws_url_maps_regional_domains():
    assert ".3km.f001.ak.grib2" in herbie_wx_model._current_rrfs_aws_url(
        dt.datetime(2026, 5, 28, 0),
        1,
        domain="alaska",
    )
    assert ".2p5km.f001.hi.grib2" in herbie_wx_model._current_rrfs_aws_url(
        dt.datetime(2026, 5, 28, 0),
        1,
        domain="hawaii",
    )


def test_forecast_hours_for_window_includes_start_and_stop():
    cycle = dt.datetime(2026, 1, 1, 0, 0)

    assert herbie_wx_model.forecast_hours_for_window(
        cycle,
        dt.datetime(2026, 1, 1, 3, 0),
        dt.datetime(2026, 1, 1, 6, 0),
    ) == [3, 4, 5, 6]


def test_parse_extra_values_coerces_scalars():
    assert herbie_wx_model.parse_extra_values([
        "member=2",
        "resolution=0.25",
        "flag=true",
        "name=control",
    ]) == {
        "member": 2,
        "resolution": 0.25,
        "flag": True,
        "name": "control",
    }
