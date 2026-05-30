from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from scripts import herbie_wx_model
from scripts.weather_models import HerbieModelSpec


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


def test_forecast_hours_for_window_brackets_coarse_models():
    cycle = dt.datetime(2026, 1, 1, 12, 0)

    assert herbie_wx_model.forecast_hours_for_window(
        cycle,
        dt.datetime(2026, 1, 1, 17, 0),
        dt.datetime(2026, 1, 1, 18, 0),
        interval_hours=3,
    ) == [3, 6]


def test_forecast_hours_for_window_rejects_unavailable_f000():
    cycle = dt.datetime(2026, 1, 1, 12, 0)

    with pytest.raises(herbie_wx_model.HerbieWeatherError, match="minimum forecast hour"):
        herbie_wx_model.forecast_hours_for_window(
            cycle,
            dt.datetime(2026, 1, 1, 12, 0),
            dt.datetime(2026, 1, 1, 13, 0),
            min_forecast_hour=1,
        )


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


def test_ncep_search_patterns_are_inventory_regexes():
    spec = herbie_wx_model.resolve_herbie_model("HRRR")

    assert spec.search_patterns["u10"][0] == r":UGRD:10 m above ground:"
    assert spec.search_patterns["v10"][0] == r":VGRD:10 m above ground:"
    assert spec.search_patterns["t2m"][0] == r":TMP:2 m above ground:"
    assert spec.search_patterns["tcc"][0] == r":TCDC:entire atmosphere[^:]*:(?!.*ave)"


def test_inventory_validation_rejects_zero_matches():
    class FakeHerbie:
        def inventory(self, *, search, verbose):
            assert search == r":UGRD:10 m above ground:"
            assert verbose is False
            return pd.DataFrame({"search_this": []})

    spec = herbie_wx_model.resolve_herbie_model("HRRR")

    with pytest.raises(herbie_wx_model.HerbieWeatherError, match="inventory had no matches"):
        herbie_wx_model._validate_inventory_match(
            FakeHerbie(),
            spec,
            "u10",
            r":UGRD:10 m above ground:",
        )


def test_downloaded_subset_validation_rejects_missing_path(tmp_path):
    spec = herbie_wx_model.resolve_herbie_model("HRRR")

    with pytest.raises(herbie_wx_model.HerbieWeatherError, match="downloaded subset is missing"):
        herbie_wx_model._validate_downloaded_path(
            tmp_path / "missing.grib2",
            spec,
            "u10",
            r":UGRD:10 m above ground:",
        )


def test_single_message_fetch_strategy_uses_field_kwargs():
    spec = HerbieModelSpec(
        name="TEST-ECCC",
        model="gdps",
        product="15km/grib2/lat_lon",
        fetch_strategy="single_message",
        field_extra={"u10": {"variable": "UGRD", "level": "TGL_10"}},
    )

    assert herbie_wx_model._field_fetches(spec, "u10") == (
        (None, {"variable": "UGRD", "level": "TGL_10"}),
    )


def test_eccc_specs_use_current_herbie_single_message_templates():
    rdps = herbie_wx_model.resolve_herbie_model("RDPS")
    gdps = herbie_wx_model.resolve_herbie_model("GDPS")

    assert rdps.product == "hrdps"
    assert gdps.product == "15km/grib2/lat_lon"


def test_indexed_fetch_can_retry_subset_from_full_grib(tmp_path):
    class FakeHerbie:
        model = "rap"
        date = dt.datetime(2026, 1, 1)
        grib = "https://example.test/rap.grib2"
        grib_source = "aws"

        def __init__(self):
            self.download_calls = []

        def inventory(self, *, search, verbose):
            assert verbose is False
            return pd.DataFrame({"search_this": [search], "grib_message": [1]})

        def get_localFilePath(self, search):
            name = "full.grib2" if search is None else "subset.grib2"
            return tmp_path / name

        def download(self, search=None, *, save_dir, errors, verbose):
            assert save_dir == tmp_path
            assert errors == "raise"
            assert verbose is False
            self.download_calls.append((search, self.grib_source))
            out_dir = tmp_path / self.model / "20260101"
            out_dir.mkdir(parents=True, exist_ok=True)
            if search is not None and self.grib_source != "local":
                return out_dir / "missing.grib2"
            path = out_dir / ("full.grib2" if search is None else "subset.grib2")
            path.write_bytes(b"GRIB")
            return path

        def xarray(self, *, search, remove_grib, save_dir, errors, verbose):
            assert search == r":UGRD:10 m above ground:"
            assert remove_grib is False
            assert save_dir == tmp_path
            assert errors == "raise"
            assert verbose is False
            return "dataset"

    spec = HerbieModelSpec(
        name="RAP",
        model="rap",
        allow_full_file_fallback=True,
        search_patterns={"u10": (r":UGRD:10 m above ground:",)},
    )
    herbie = FakeHerbie()

    result = herbie_wx_model._open_herbie_dataset(
        herbie,
        spec,
        "u10",
        search=r":UGRD:10 m above ground:",
        cache_root=tmp_path,
    )

    assert result == "dataset"
    assert herbie.download_calls == [
        (r":UGRD:10 m above ground:", "aws"),
        (None, "aws"),
        (r":UGRD:10 m above ground:", "local"),
    ]


def test_indexed_fetch_can_open_filtered_full_grib_when_subset_fails(tmp_path):
    class FakeHerbie:
        model = "nam"
        date = dt.datetime(2026, 1, 1)
        grib = "https://example.test/nam.grib2"
        grib_source = "aws"

        def inventory(self, *, search, verbose):
            return pd.DataFrame({"search_this": [search], "grib_message": [1]})

        def get_localFilePath(self, search):
            name = "full.grib2" if search is None else "subset.grib2"
            return tmp_path / name

        def download(self, search=None, *, save_dir, errors, verbose):
            out_dir = tmp_path / self.model / "20260101"
            out_dir.mkdir(parents=True, exist_ok=True)
            if search is None:
                path = out_dir / "full.grib2"
                path.write_bytes(b"GRIB")
                return path
            return out_dir / "missing.grib2"

        def xarray(self, *, remove_grib, save_dir, errors, verbose, backend_kwargs):
            assert backend_kwargs == {
                "filter_by_keys": {
                    "typeOfLevel": "heightAboveGround",
                    "level": 10,
                    "shortName": "10u",
                }
            }
            return "filtered-full-dataset"

    spec = HerbieModelSpec(
        name="NAM",
        model="nam",
        allow_full_file_fallback=True,
        search_patterns={"u10": (r":UGRD:10 m above ground:",)},
    )

    result = herbie_wx_model._open_herbie_dataset(
        FakeHerbie(),
        spec,
        "u10",
        search=r":UGRD:10 m above ground:",
        cache_root=tmp_path,
    )

    assert result == "filtered-full-dataset"


def test_gdps_override_uses_wxo_dd_single_message_path():
    class FakeHerbie:
        def find_grib(self):
            return self.SOURCES["msc"], "msc"

        def find_idx(self):
            return None, None

    herbie = FakeHerbie()

    herbie_wx_model._apply_gdps_wxo_override(
        herbie,
        dt.datetime(2026, 5, 30, 0),
        9,
        variable="WindU",
        level="AGL-10m",
    )

    assert herbie.SOURCES == {
        "msc": (
            "https://dd.weather.gc.ca/20260530/WXO-DD/model_gdps/"
            "15km/00/009/20260530T00Z_MSC_GDPS_WindU_AGL-10m_LatLon0.15_PT009H.grib2"
        )
    }
    assert herbie.grib_source == "msc"


def test_speed_direction_to_uv_uses_meteorological_direction():
    speed = np.array([[10.0, 10.0]], dtype="float32")
    direction = np.array([[270.0, 180.0]], dtype="float32")

    u, v = herbie_wx_model._speed_direction_to_uv(speed, direction)

    assert np.allclose(u, [[10.0, 0.0]], atol=1e-5)
    assert np.allclose(v, [[0.0, 10.0]], atol=1e-5)


def test_extract_array_selects_requested_valid_time_step():
    spec = herbie_wx_model.resolve_herbie_model("HRRR")
    values = np.stack([
        np.full((2, 3), 1.0, dtype="float32"),
        np.full((2, 3), 2.0, dtype="float32"),
    ])
    ds = xr.Dataset(
        data_vars={"u10": (("step", "y", "x"), values)},
        coords={
            "step": ("step", np.array([0, 1], dtype="timedelta64[h]")),
            "valid_time": (
                "step",
                np.array(["2026-01-01T00:00:00", "2026-01-01T01:00:00"], dtype="datetime64[s]"),
            ),
            "latitude": (("y", "x"), np.full((2, 3), 39.0, dtype="float32")),
            "longitude": (("y", "x"), np.full((2, 3), -106.0, dtype="float32")),
        },
    )

    result = herbie_wx_model._extract_array(
        ds,
        spec,
        "u10",
        valid_time=dt.datetime(2026, 1, 1, 1),
    )

    assert result["values"].shape == (2, 3)
    assert np.all(result["values"] == 2.0)


def test_extract_array_rejects_ambiguous_extra_dimension():
    spec = herbie_wx_model.resolve_herbie_model("HRRR")
    ds = xr.Dataset(
        data_vars={"u10": (("step", "y", "x"), np.ones((2, 2, 3), dtype="float32"))},
        coords={
            "step": ("step", np.array([0, 1], dtype="timedelta64[h]")),
            "latitude": (("y", "x"), np.full((2, 3), 39.0, dtype="float32")),
            "longitude": (("y", "x"), np.full((2, 3), -106.0, dtype="float32")),
        },
    )

    with pytest.raises(herbie_wx_model.HerbieWeatherError, match="non-spatial dimension"):
        herbie_wx_model._extract_array(
            ds,
            spec,
            "u10",
            valid_time=dt.datetime(2026, 1, 1, 1),
        )


def test_clip_dataset_to_bbox_rejects_no_overlap():
    ds = xr.Dataset(
        data_vars={"u10": (("latitude", "longitude"), np.ones((2, 2), dtype="float32"))},
        coords={
            "latitude": ("latitude", np.array([60.0, 61.0], dtype="float32")),
            "longitude": ("longitude", np.array([-150.0, -149.0], dtype="float32")),
        },
    )

    with pytest.raises(herbie_wx_model.HerbieWeatherError, match="does not overlap"):
        herbie_wx_model.clip_dataset_to_bbox(ds, (-107.0, 39.0, -106.0, 40.0))
