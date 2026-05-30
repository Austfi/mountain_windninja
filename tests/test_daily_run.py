from __future__ import annotations

import argparse
import datetime as dt
import sys
import zipfile
from pathlib import Path

import pytest

import scripts.daily_run as daily_run
from scripts.config_loader import DomainConfig


UTC = dt.timezone.utc


def test_resolve_weather_model_supports_forecast_and_pastcast():
    assert daily_run.resolve_weather_model("NBM", "forecast") == "NOMADS-NBM-CONUS-2.5-KM"
    assert daily_run.resolve_weather_model("HRRR", "reanalysis") == "PASTCAST-GCP-HRRR-CONUS-3-KM"


def test_resolve_weather_model_supports_opt_in_herbie_models():
    for model in daily_run.HERBIE_MODEL_MAP:
        assert (
            daily_run.resolve_weather_model(model, "forecast", weather_source="herbie")
            == f"Herbie {model}"
        )


def test_resolve_weather_model_rejects_herbie_only_models_on_native_source():
    with pytest.raises(ValueError, match="only available with --weather-source herbie"):
        daily_run.resolve_weather_model("RRFS", "forecast")


def test_herbie_model_registry_only_lists_windninja_sensible_templates():
    excluded = {
        "HAFSA",
        "HAFSB",
        "CFS",
        "GEFS-REFORECAST",
        "GEFS-WAVE-REFORECAST",
        "GRAPHCAST",
        "HRDPS",
        "HRDPS-NORTH",
        "HREF",
        "NAVGEM-GODAE",
        "NAVGEM-NOMADS",
    }
    assert excluded.isdisjoint(daily_run.HERBIE_MODEL_MAP)
    assert sorted(daily_run.HERBIE_MODEL_MAP) == [
        "AIFS",
        "GDPS",
        "GEFS",
        "GEFS-MEAN",
        "GFS",
        "HIRESW",
        "HRRR",
        "HRRRAK",
        "IFS",
        "NAM",
        "NAM-ALASKA",
        "NAM-CONUS",
        "NBM",
        "RAP",
        "RDPS",
        "RRFS",
    ]


def test_resolve_weather_model_rejects_non_windninja_herbie_templates():
    with pytest.raises(ValueError, match="Herbie weather source only supports"):
        daily_run.resolve_weather_model("GRAPHCAST", "forecast", weather_source="herbie")


def test_resolve_weather_model_rejects_unsupported_reanalysis_model():
    with pytest.raises(ValueError):
        daily_run.resolve_weather_model("NAM", "reanalysis")


def _make_template(tmp_path):
    template = tmp_path / "template.cfg"
    template.write_text(
        "\n".join([
            "num_threads = 4",
            "elevation_file = {elevation_file}",
            "initialization_method = wxModelInitialization",
            "wx_model_type = NOMADS-HRRR-CONUS-3-KM",
            "start_year = {start_year}",
            "start_month = {start_month}",
            "start_day = {start_day}",
            "start_hour = {start_hour}",
            "start_minute = {start_minute}",
            "stop_year = {stop_year}",
            "stop_month = {stop_month}",
            "stop_day = {stop_day}",
            "stop_hour = {stop_hour}",
            "stop_minute = {stop_minute}",
            "forecast_duration = {forecast_duration}",
            "output_path = placeholder",
        ]),
        encoding="utf-8",
    )
    return template


def test_generate_config_applies_weather_model_override(tmp_path):
    template = _make_template(tmp_path)
    domain = DomainConfig(
        key="test", label="Test",
        template_path=template,
        elevation_file=Path("/tmp/test_dem.tif"),
    )

    config_path, _ = daily_run.generate_config(
        date_str="20260101",
        start_time=dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        stop_time=dt.datetime(2026, 1, 1, 6, 0, tzinfo=UTC),
        domain_config=domain,
        sub_dir=str(tmp_path / "out"),
        wx_model_type_override="NOMADS-NBM-CONUS-2.5-KM",
    )

    contents = Path(config_path).read_text(encoding="utf-8")
    assert "wx_model_type = NOMADS-NBM-CONUS-2.5-KM" in contents
    assert "output_path =" in contents


def test_generate_config_applies_forecast_filename(tmp_path):
    template = _make_template(tmp_path)
    domain = DomainConfig(
        key="test", label="Test",
        template_path=template,
        elevation_file=Path("/tmp/test_dem.tif"),
    )
    forecast_file = tmp_path / "weather" / "windninja_generic.nc"

    config_path, _ = daily_run.generate_config(
        date_str="20260101",
        start_time=dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        stop_time=dt.datetime(2026, 1, 1, 6, 0, tzinfo=UTC),
        domain_config=domain,
        sub_dir=str(tmp_path / "out"),
        forecast_filename=str(forecast_file),
    )

    contents = Path(config_path).read_text(encoding="utf-8")
    assert f"forecast_filename = {forecast_file.as_posix()}" in contents
    assert "wx_model_type =" not in contents


def test_generate_config_rejects_weather_model_and_forecast_filename(tmp_path):
    template = _make_template(tmp_path)
    domain = DomainConfig(
        key="test", label="Test",
        template_path=template,
        elevation_file=Path("/tmp/test_dem.tif"),
    )

    with pytest.raises(ValueError, match="either wx_model_type_override or forecast_filename"):
        daily_run.generate_config(
            date_str="20260101",
            start_time=dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
            stop_time=dt.datetime(2026, 1, 1, 6, 0, tzinfo=UTC),
            domain_config=domain,
            sub_dir=str(tmp_path / "out"),
            wx_model_type_override="NOMADS-HRRR-CONUS-3-KM",
            forecast_filename=str(tmp_path / "weather.nc"),
        )


def test_main_herbie_dry_run_uses_forecast_filename(tmp_path, monkeypatch):
    template = _make_template(tmp_path)
    domain = DomainConfig(
        key="test", label="Test",
        template_path=template,
        elevation_file=Path("/tmp/test_dem.tif"),
    )
    weather_file = tmp_path / "weather" / "windninja_generic.nc"
    weather_file.parent.mkdir()
    weather_file.write_text("placeholder", encoding="utf-8")

    monkeypatch.setattr(daily_run.config_loader, "init_directories", lambda: None)
    monkeypatch.setattr(daily_run.config_loader, "list_domains", lambda: ["test"])
    monkeypatch.setattr(daily_run.config_loader, "DEFAULT_DOMAIN", "test")
    monkeypatch.setattr(daily_run.config_loader, "get_domain_config", lambda key: domain)
    monkeypatch.setattr(daily_run.config_loader, "TEMP_DIR", tmp_path / "temp")
    monkeypatch.setattr(daily_run.config_loader, "GCS_UPLOAD_ENABLED", False)

    import scripts.herbie_wx_model as herbie_wx_model

    monkeypatch.setattr(
        herbie_wx_model,
        "prepare_herbie_wx_model",
        lambda **_kwargs: weather_file,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "daily_run.py",
            "--domain",
            "test",
            "--weather-source",
            "herbie",
            "--model",
            "HRRR",
            "--hours",
            "1",
            "--dry-run",
            "--no-upload",
        ],
    )

    daily_run.main()

    configs = list((tmp_path / "temp").glob("test_*_forecast_1h_HRRR/*.cfg"))
    assert len(configs) == 1
    contents = configs[0].read_text(encoding="utf-8")
    assert f"forecast_filename = {weather_file.as_posix()}" in contents
    assert "wx_model_type =" not in contents


def test_generate_config_allows_env_thread_override(tmp_path, monkeypatch):
    template = _make_template(tmp_path)
    domain = DomainConfig(
        key="test", label="Test",
        template_path=template,
        elevation_file=Path("/tmp/test_dem.tif"),
    )
    monkeypatch.setenv("MWN_NUM_THREADS", "6")

    config_path, _ = daily_run.generate_config(
        date_str="20260101",
        start_time=dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        stop_time=dt.datetime(2026, 1, 1, 6, 0, tzinfo=UTC),
        domain_config=domain,
        sub_dir=str(tmp_path / "out"),
    )

    contents = Path(config_path).read_text(encoding="utf-8")
    assert "num_threads = 6" in contents
    assert "num_threads = 4" not in contents


def test_generate_config_strips_forecast_duration_for_reanalysis(tmp_path):
    template = _make_template(tmp_path)
    domain = DomainConfig(
        key="test", label="Test",
        template_path=template,
        elevation_file=Path("/tmp/test_dem.tif"),
    )

    config_path, _ = daily_run.generate_config(
        date_str="20260101",
        start_time=dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        stop_time=dt.datetime(2026, 1, 2, 0, 0, tzinfo=UTC),
        domain_config=domain,
        sub_dir=str(tmp_path / "out"),
        wx_model_type_override="PASTCAST-GCP-HRRR-CONUS-3-KM",
        run_type="reanalysis",
    )

    contents = Path(config_path).read_text(encoding="utf-8")
    assert "wx_model_type = PASTCAST-GCP-HRRR-CONUS-3-KM" in contents
    assert "forecast_duration =" not in contents
    assert "start_year = 2026" in contents
    assert "stop_day = 2" in contents


def test_generate_config_injects_surface_vegetation_for_dem_runs(tmp_path):
    template = _make_template(tmp_path)
    domain = DomainConfig(
        key="test", label="Test",
        template_path=template,
        elevation_file=Path("/tmp/test_dem.tif"),
    )

    config_path, _ = daily_run.generate_config(
        date_str="20260101",
        start_time=dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        stop_time=dt.datetime(2026, 1, 1, 6, 0, tzinfo=UTC),
        domain_config=domain,
        sub_dir=str(tmp_path / "out"),
        surface_vegetation="brush",
    )

    contents = Path(config_path).read_text(encoding="utf-8")
    assert "vegetation = brush" in contents


def test_generate_config_does_not_inject_surface_vegetation_for_lcp_runs(tmp_path):
    template = _make_template(tmp_path)
    domain = DomainConfig(
        key="test", label="Test",
        template_path=template,
        elevation_file=Path("/tmp/test_surface.lcp"),
    )

    config_path, _ = daily_run.generate_config(
        date_str="20260101",
        start_time=dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        stop_time=dt.datetime(2026, 1, 1, 6, 0, tzinfo=UTC),
        domain_config=domain,
        sub_dir=str(tmp_path / "out"),
        surface_vegetation="brush",
    )

    contents = Path(config_path).read_text(encoding="utf-8")
    assert "vegetation = brush" not in contents


def test_generate_config_writes_output_path_once(tmp_path):
    template = _make_template(tmp_path)
    domain = DomainConfig(
        key="test", label="Test",
        template_path=template,
        elevation_file=Path("/tmp/test_dem.tif"),
    )

    config_path, run_dir = daily_run.generate_config(
        date_str="20260101",
        start_time=dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        stop_time=dt.datetime(2026, 1, 1, 6, 0, tzinfo=UTC),
        domain_config=domain,
        sub_dir=str(tmp_path / "out"),
    )

    contents = Path(config_path).read_text(encoding="utf-8")
    assert contents.count("output_path =") == 1


def test_generate_config_appends_point_sampling_files(tmp_path):
    template = _make_template(tmp_path)
    domain = DomainConfig(
        key="test", label="Test",
        template_path=template,
        elevation_file=Path("/tmp/test_dem.tif"),
    )

    points_file = tmp_path / "stations.csv"
    points_file.write_text("WGS84\npoint_name,latitude,longitude,height_meters_above_ground\n", encoding="utf-8")

    config_path, run_dir = daily_run.generate_config(
        date_str="20260101",
        start_time=dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        stop_time=dt.datetime(2026, 1, 1, 6, 0, tzinfo=UTC),
        domain_config=domain,
        sub_dir=str(tmp_path / "out"),
        input_points_file=str(points_file),
    )

    contents = Path(config_path).read_text(encoding="utf-8")
    assert f"input_points_file = {points_file.as_posix()}" in contents
    assert f"output_points_file = {Path(run_dir, 'test_sample_points.csv').as_posix()}" in contents


def test_get_run_parameters_forecast():
    params = daily_run.get_run_parameters("forecast", 6)
    assert params["type"] == "forecast"
    assert params["label"] == "forecast_6h"
    assert (params["stop"] - params["start"]).total_seconds() == 6 * 3600


def test_get_run_parameters_reanalysis():
    params = daily_run.get_run_parameters("reanalysis", 12)
    assert params["type"] == "reanalysis"
    assert params["label"] == "reanalysis_12h"
    assert (params["stop"] - params["start"]).total_seconds() == 12 * 3600


def test_get_run_parameters_rejects_non_positive_hours():
    with pytest.raises(ValueError, match="--hours must be >= 1"):
        daily_run.get_run_parameters("forecast", 0)
    with pytest.raises(ValueError, match="--hours must be >= 1"):
        daily_run.get_run_parameters("reanalysis", -1)


def test_build_run_parameters_accepts_explicit_reanalysis_window():
    start = dt.datetime(2026, 1, 1, 0, 0)
    end = dt.datetime(2026, 1, 8, 0, 0)

    params = daily_run.build_run_parameters("reanalysis", 12, start_time=start, end_time=end)

    assert params["type"] == "reanalysis"
    assert params["label"] == "reanalysis_168h"
    assert params["start"] == start
    assert params["stop"] == end


def test_run_identity_includes_full_start_time_and_domain():
    start_midnight = dt.datetime(2026, 1, 1, 0, 0)
    start_noon = dt.datetime(2026, 1, 1, 12, 0)

    first_dir = daily_run.build_output_dir_name(
        "loveland", start_midnight, "reanalysis_12h", "HRRR",
    )
    second_dir = daily_run.build_output_dir_name(
        "loveland", start_noon, "reanalysis_12h", "HRRR",
    )
    first_archive = daily_run.build_archive_name_base(
        "loveland", start_midnight, "reanalysis_12h", "HRRR",
    )
    second_archive = daily_run.build_archive_name_base(
        "loveland", start_noon, "reanalysis_12h", "HRRR",
    )

    assert first_dir == "loveland_20260101_0000_reanalysis_12h_HRRR"
    assert second_dir == "loveland_20260101_1200_reanalysis_12h_HRRR"
    assert first_archive == "loveland_reanalysis_12h_HRRR_20260101_0000"
    assert second_archive == "loveland_reanalysis_12h_HRRR_20260101_1200"
    assert first_dir != second_dir
    assert first_archive != second_archive


def test_domain_average_identity_includes_domain_and_timestamp():
    output_dir = daily_run.build_domain_average_output_dir_name(
        "loveland", "20260101_1200",
    )
    archive = daily_run.build_domain_average_archive_name(
        "loveland", "20260101_1200", 10.0, "mph", 270.0,
    )

    assert output_dir == "loveland_domavg_20260101_1200"
    assert archive == "loveland_domavg_20260101_1200_10mph_270deg"


def test_build_run_parameters_rejects_non_hour_aligned_windows():
    with pytest.raises(ValueError):
        daily_run.build_run_parameters(
            "reanalysis",
            12,
            start_time=dt.datetime(2026, 1, 1, 0, 30),
            end_time=dt.datetime(2026, 1, 1, 6, 0),
        )


def test_parse_utc_timestamp_supports_compact_and_iso_formats():
    assert daily_run.parse_utc_timestamp("202601010000") == dt.datetime(2026, 1, 1, 0, 0)
    assert daily_run.parse_utc_timestamp("2026-01-01T00:00") == dt.datetime(2026, 1, 1, 0, 0)


def test_resolve_weather_model_all_forecast_models():
    for name, full in daily_run.FORECAST_MODEL_MAP.items():
        assert daily_run.resolve_weather_model(name, "forecast") == full


def test_resolve_weather_model_rejects_unsupported_reanalysis_models():
    for name in daily_run.FORECAST_MODEL_MAP:
        if name not in daily_run.PASTCAST_MODEL_MAP:
            with pytest.raises(ValueError):
                daily_run.resolve_weather_model(name, "reanalysis")


def test_all_model_names_is_sorted_union():
    expected = sorted(set(
        list(daily_run.FORECAST_MODEL_MAP)
        + list(daily_run.PASTCAST_MODEL_MAP)
        + list(daily_run.HERBIE_MODEL_MAP)
    ))
    assert daily_run.ALL_MODEL_NAMES == expected


def test_generate_domain_average_config(tmp_path):
    domain = DomainConfig(
        key="test", label="Test",
        template_path=Path("/tmp/unused.cfg"),
        elevation_file=Path("/tmp/test_dem.tif"),
    )

    config_path, output_dir = daily_run.generate_domain_average_config(
        domain, wind_speed=15.0, wind_direction=270.0,
        speed_units="mph", surface_vegetation="trees",
        sub_dir=str(tmp_path / "domavg_out"),
    )

    contents = Path(config_path).read_text(encoding="utf-8")
    assert "initialization_method = domainAverageInitialization" in contents
    assert "input_speed = 15.0" in contents
    assert "input_direction = 270.0" in contents
    assert "input_speed_units = mph" in contents
    assert "vegetation = trees" in contents
    assert "output_path =" in contents
    assert "wx_model_type" not in contents


def test_generate_domain_average_config_skips_vegetation_for_lcp(tmp_path):
    domain = DomainConfig(
        key="test", label="Test",
        template_path=Path("/tmp/unused.cfg"),
        elevation_file=Path("/tmp/test_surface.lcp"),
    )

    config_path, _ = daily_run.generate_domain_average_config(
        domain, wind_speed=10.0, wind_direction=180.0,
        surface_vegetation="trees",
        sub_dir=str(tmp_path / "domavg_out"),
    )

    contents = Path(config_path).read_text(encoding="utf-8")
    assert "vegetation" not in contents


def test_generate_domain_average_config_uses_template_thread_cap(tmp_path, monkeypatch):
    monkeypatch.delenv("MWN_NUM_THREADS", raising=False)
    template = _make_template(tmp_path)
    template.write_text(
        template.read_text(encoding="utf-8").replace("num_threads = 4", "num_threads = 6"),
        encoding="utf-8",
    )
    domain = DomainConfig(
        key="test", label="Test",
        template_path=template,
        elevation_file=Path("/tmp/test_dem.tif"),
    )

    monkeypatch.setattr(daily_run.os, "cpu_count", lambda: 64)

    config_path, _ = daily_run.generate_domain_average_config(
        domain, wind_speed=12.0, wind_direction=225.0,
        sub_dir=str(tmp_path / "domavg_out"),
    )

    contents = Path(config_path).read_text(encoding="utf-8")
    assert "num_threads = 6" in contents


def test_generate_domain_average_config_allows_env_thread_override(tmp_path, monkeypatch):
    template = _make_template(tmp_path)
    domain = DomainConfig(
        key="test", label="Test",
        template_path=template,
        elevation_file=Path("/tmp/test_dem.tif"),
    )
    monkeypatch.setenv("MWN_NUM_THREADS", "6")

    config_path, _ = daily_run.generate_domain_average_config(
        domain,
        wind_speed=12.0,
        wind_direction=225.0,
        sub_dir=str(tmp_path / "domavg_out"),
    )

    contents = Path(config_path).read_text(encoding="utf-8")
    assert "num_threads = 6" in contents


def test_generate_domain_average_config_uses_template_momentum_flag(tmp_path):
    template = _make_template(tmp_path)
    template.write_text(
        template.read_text(encoding="utf-8") + "\nmomentum_flag = false\n",
        encoding="utf-8",
    )
    domain = DomainConfig(
        key="test",
        label="Test",
        template_path=template,
        elevation_file=Path("/tmp/test_dem.tif"),
    )

    config_path, _ = daily_run.generate_domain_average_config(
        domain,
        wind_speed=12.0,
        wind_direction=225.0,
        sub_dir=str(tmp_path / "domavg_out"),
    )

    contents = Path(config_path).read_text(encoding="utf-8")
    assert "momentum_flag = false" in contents


def test_generate_gridded_config_uses_gridded_initialization(tmp_path):
    template = _make_template(tmp_path)
    domain = DomainConfig(
        key="test", label="Test",
        template_path=template,
        elevation_file=Path("/tmp/test_dem.tif"),
    )
    run_time = dt.datetime(2026, 1, 1, 12, 0)

    config_path, output_dir = daily_run.generate_gridded_config(
        domain,
        tmp_path / "speed.asc",
        tmp_path / "direction.asc",
        run_time,
        label="case one",
        surface_vegetation="grass",
        sub_dir=str(tmp_path / "grid_out"),
        output_wind_height=20,
    )

    contents = Path(config_path).read_text(encoding="utf-8")
    assert "initialization_method = griddedInitialization" in contents
    assert f"input_speed_grid = {(tmp_path / 'speed.asc').as_posix()}" in contents
    assert f"input_dir_grid = {(tmp_path / 'direction.asc').as_posix()}" in contents
    assert "input_speed_units = mps" in contents
    assert "input_wind_height = 10.0" in contents
    assert "units_input_wind_height = m" in contents
    assert "diurnal_winds = false" in contents
    assert "year  = 2026" in contents
    assert "hour  = 12" in contents
    assert "vegetation = grass" in contents
    assert "output_wind_height = 20" in contents
    assert "wx_model_type" not in contents
    assert "forecast_duration" not in contents
    assert output_dir == str(tmp_path / "grid_out")


def test_grid_identity_helpers_sanitize_label():
    start = dt.datetime(2026, 1, 1, 12, 0)

    output_dir = daily_run.build_grid_output_dir_name("loveland", start, "case one")
    archive = daily_run.build_grid_archive_name("loveland", "case one", start)

    assert output_dir == "loveland_20260101_1200_grid_case_one"
    assert archive == "loveland_grid_case_one_20260101_1200"


def test_build_windninja_env_enables_unsigned_gcs_for_reanalysis(monkeypatch):
    monkeypatch.delenv("GS_NO_SIGN_REQUEST", raising=False)
    monkeypatch.delenv("GS_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.delenv("GS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("GS_OAUTH2_PRIVATE_KEY_FILE", raising=False)
    monkeypatch.delenv("GS_OAUTH2_CLIENT_EMAIL", raising=False)
    monkeypatch.delenv("GS_OAUTH2_REFRESH_TOKEN", raising=False)
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)

    env = daily_run.build_windninja_env("reanalysis")

    assert env["GS_NO_SIGN_REQUEST"] == "YES"


def test_build_windninja_env_preserves_explicit_gcs_auth_for_reanalysis(monkeypatch):
    monkeypatch.delenv("GS_NO_SIGN_REQUEST", raising=False)
    monkeypatch.setenv("GS_ACCESS_KEY_ID", "access")
    monkeypatch.setenv("GS_SECRET_ACCESS_KEY", "secret")

    env = daily_run.build_windninja_env("reanalysis")

    assert "GS_NO_SIGN_REQUEST" not in env
    assert env["GS_ACCESS_KEY_ID"] == "access"
    assert env["GS_SECRET_ACCESS_KEY"] == "secret"


def test_point_sampling_rejects_momentum_template(tmp_path, capsys):
    template = _make_template(tmp_path)
    template.write_text(
        template.read_text(encoding="utf-8") + "\nmomentum_flag = true\n",
        encoding="utf-8",
    )
    domain = DomainConfig(
        key="test",
        label="Test",
        template_path=template,
        elevation_file=Path("/tmp/test_dem.tif"),
    )
    parser = argparse.ArgumentParser(prog="daily_run.py")

    with pytest.raises(SystemExit) as excinfo:
        daily_run.validate_point_sampling_supported(parser, domain, tmp_path / "points.csv")

    assert excinfo.value.code == 2
    assert "validate-rasters" in capsys.readouterr().err


def test_point_sampling_allows_mass_only_template(tmp_path):
    template = _make_template(tmp_path)
    template.write_text(
        template.read_text(encoding="utf-8") + "\nmomentum_flag = false\n",
        encoding="utf-8",
    )
    domain = DomainConfig(
        key="test",
        label="Test",
        template_path=template,
        elevation_file=Path("/tmp/test_dem.tif"),
    )
    parser = argparse.ArgumentParser(prog="daily_run.py")

    daily_run.validate_point_sampling_supported(parser, domain, tmp_path / "points.csv")


def test_rename_reanalysis_outputs_preserves_parent_weather_rasters(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    files = [
        "windninja_202601010000_vel.asc",
        "windninja_202601010000_ang.asc",
        "PASTCAST-GCP-HRRR-CONUS-3-KM-202601010000_vel.asc",
        "PASTCAST-GCP-HRRR-CONUS-3-KM-202601010000_ang.asc",
    ]
    for name in files:
        (run_dir / name).write_text(name, encoding="utf-8")

    daily_run.rename_reanalysis_outputs(str(run_dir), "loveland")

    assert (run_dir / "loveland_20260101_0000_vel.asc").read_text(
        encoding="utf-8"
    ) == "windninja_202601010000_vel.asc"
    assert (run_dir / "loveland_20260101_0000_ang.asc").read_text(
        encoding="utf-8"
    ) == "windninja_202601010000_ang.asc"
    assert (run_dir / "PASTCAST-GCP-HRRR-CONUS-3-KM-202601010000_vel.asc").exists()
    assert (run_dir / "PASTCAST-GCP-HRRR-CONUS-3-KM-202601010000_ang.asc").exists()


def test_rename_reanalysis_outputs_refuses_collisions(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "windninja_202601010000_vel.asc").write_text("new", encoding="utf-8")
    (run_dir / "loveland_20260101_0000_vel.asc").write_text("existing", encoding="utf-8")

    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        daily_run.rename_reanalysis_outputs(str(run_dir), "loveland")


def test_archive_results_keeps_ascii_outputs_and_skips_grids_dir(tmp_path, monkeypatch):
    archive_dir = tmp_path / "archives"
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "forecast_playable.kmz").write_text("kmz", encoding="utf-8")
    (run_dir / "forecast_vel.asc").write_text("speed", encoding="utf-8")
    (run_dir / "forecast_vel.prj").write_text("proj", encoding="utf-8")
    (run_dir / "forecast.cfg").write_text("config", encoding="utf-8")
    grids_dir = run_dir / "grids"
    grids_dir.mkdir()
    (grids_dir / "ignore.txt").write_text("ignore", encoding="utf-8")

    monkeypatch.setattr(daily_run.config_loader, "ARCHIVE_DIR", str(archive_dir))

    archive_path = daily_run.archive_results(str(run_dir), "bundle")

    with zipfile.ZipFile(archive_path, "r") as zf:
        names = set(zf.namelist())

    assert names == {
        "forecast.cfg",
        "forecast_playable.kmz",
        "forecast_vel.asc",
        "forecast_vel.prj",
    }
    assert not run_dir.exists()
