from __future__ import annotations

import datetime as dt
import zipfile
from pathlib import Path

import pytest

import scripts.daily_run as daily_run
from scripts.config_loader import DomainConfig


UTC = dt.timezone.utc


def test_resolve_weather_model_supports_forecast_and_pastcast():
    assert daily_run.resolve_weather_model("NBM", "forecast") == "NOMADS-NBM-CONUS-2.5-KM"
    assert daily_run.resolve_weather_model("HRRR", "reanalysis") == "PASTCAST-GCP-HRRR-CONUS-3-KM"


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
        list(daily_run.FORECAST_MODEL_MAP) + list(daily_run.PASTCAST_MODEL_MAP)
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
