from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

import pytest

from ml.residual_unet.breck_synoptic_validation import (
    GcsPair,
    RasterSource,
    ascii_grid_indices,
    build_station_sample_rows,
    collect_raster_inventory,
    coverage_for_pair,
    emulator_summary_rows,
    model_summary_rows,
    observation_coverage_rows,
    pair_observation_counts,
    pair_observed_timestamps,
    pair_gcs_runs,
    parent_hrrr_raster_base,
    parse_gcs_run_uri,
    raster_timestamp_label,
    run_listing_patterns,
    sample_ascii_grid,
    safe_source_name,
    solver_raster_base,
    token_from_runtime_env,
)
from ml.residual_unet.infer import _copy_projection_sidecars
from scripts import synoptic_validation as sv


UTC = dt.timezone.utc


def test_breck_manifest_uses_explicit_10m_synoptic_heights():
    rows = list(
        csv.DictReader(
            Path("config/stations/breck_tenmile_ml_validation_manifest.csv").open(encoding="utf-8")
        )
    )

    assert [row["station_id"] for row in rows] == ["CABP6", "CABP8", "CAHSB"]
    assert {row["height_m_override"] for row in rows} == {"10.0"}
    assert {row["provider"] for row in rows} == {"synoptic"}


def test_parse_gcs_run_uri_and_pair_exact_mass_momentum_dates():
    mass_uris = [
        "gs://bucket/runtime_temp/breck_tenmile_9p6_mass_20260101_0000_reanalysis_24h_HRRR/",
        "gs://bucket/runtime_temp/breck_tenmile_9p6_mass_20260102_0000_reanalysis_24h_HRRR/",
    ]
    momentum_uris = [
        "gs://bucket/runtime_temp/breck_tenmile_9p6_20260101_0000_reanalysis_24h_HRRR/",
        "gs://bucket/runtime_temp/keystone_9p6_20260102_0000_reanalysis_24h_HRRR/",
    ]

    parsed = parse_gcs_run_uri(mass_uris[0])
    parsed_from_file = parse_gcs_run_uri(
        "gs://bucket/runtime_temp/breck_tenmile_9p6_mass_20260101_0000_reanalysis_24h_HRRR/"
        "breck_tenmile_9p6_01-01-2026_0000_100m_vel.asc"
    )
    pairs = pair_gcs_runs(mass_uris, momentum_uris)

    assert parsed.domain == "breck_tenmile_9p6_mass"
    assert parsed_from_file.uri == "gs://bucket/runtime_temp/breck_tenmile_9p6_mass_20260101_0000_reanalysis_24h_HRRR"
    assert parsed.start == dt.datetime(2026, 1, 1, tzinfo=UTC)
    assert parsed.hours == 24
    assert len(pairs) == 1
    assert pairs[0].mass.run_name == "breck_tenmile_9p6_mass_20260101_0000_reanalysis_24h_HRRR"
    assert pairs[0].momentum.run_name == "breck_tenmile_9p6_20260101_0000_reanalysis_24h_HRRR"


def test_run_listing_patterns_use_date_prefix_for_narrow_window():
    patterns = run_listing_patterns(
        "gs://bucket/runtime_temp",
        "breck_tenmile_9p6_mass",
        "HRRR",
        dt.datetime(2026, 1, 1, tzinfo=UTC),
        dt.datetime(2026, 1, 3, tzinfo=UTC),
    )

    assert patterns == [
        "gs://bucket/runtime_temp/breck_tenmile_9p6_mass_20260101_0000_reanalysis_*h_HRRR",
        "gs://bucket/runtime_temp/breck_tenmile_9p6_mass_20260102_0000_reanalysis_*h_HRRR",
    ]


def test_run_listing_patterns_use_month_prefix_for_long_window():
    patterns = run_listing_patterns(
        "gs://bucket/runtime_temp",
        "breck_tenmile_9p6_mass",
        "HRRR",
        dt.datetime(2025, 1, 15, tzinfo=UTC),
        dt.datetime(2025, 4, 2, tzinfo=UTC),
    )

    assert patterns == [
        "gs://bucket/runtime_temp/breck_tenmile_9p6_mass_202501*_reanalysis_*h_HRRR",
        "gs://bucket/runtime_temp/breck_tenmile_9p6_mass_202502*_reanalysis_*h_HRRR",
        "gs://bucket/runtime_temp/breck_tenmile_9p6_mass_202503*_reanalysis_*h_HRRR",
        "gs://bucket/runtime_temp/breck_tenmile_9p6_mass_202504*_reanalysis_*h_HRRR",
    ]


def test_exact_download_raster_names_for_observed_timestamp():
    timestamp = dt.datetime(2025, 9, 17, 17, 0, tzinfo=UTC)

    assert raster_timestamp_label(timestamp) == "09-17-2025_1700"
    assert (
        solver_raster_base("breck_tenmile_9p6_mass", timestamp)
        == "breck_tenmile_9p6_09-17-2025_1700_100m"
    )
    assert (
        solver_raster_base("breck_tenmile_9p6", timestamp)
        == "breck_tenmile_9p6_09-17-2025_1700_100m"
    )
    assert (
        parent_hrrr_raster_base(timestamp)
        == "PASTCAST-GCP-HRRR-CONUS-3-KM-09-17-2025_1700"
    )


def test_token_from_runtime_env_reads_without_printing(monkeypatch, tmp_path):
    monkeypatch.delenv("MWN_SYNOPTIC_TOKEN", raising=False)
    monkeypatch.delenv("CUSTOM_API_KEY", raising=False)
    runtime_env = tmp_path / "config" / "runtime.env"
    runtime_env.parent.mkdir(parents=True)
    runtime_env.write_text("MWN_SYNOPTIC_TOKEN='secret-token'\n", encoding="utf-8")

    assert token_from_runtime_env(tmp_path) == "secret-token"


def test_collect_raster_inventory_counts_solver_and_parent_pairs():
    paths = [
        "gs://bucket/run/breck_tenmile_9p6_01-01-2026_0000_100m_vel.asc",
        "gs://bucket/run/breck_tenmile_9p6_01-01-2026_0000_100m_ang.asc",
        "gs://bucket/run/breck_tenmile_9p6_01-01-2026_0100_100m_vel.asc",
        "gs://bucket/run/breck_tenmile_9p6_01-01-2026_0100_100m_ang.asc",
        "gs://bucket/run/PASTCAST-GCP-HRRR-CONUS-3-KM-01-01-2026_0000_vel.asc",
        "gs://bucket/run/PASTCAST-GCP-HRRR-CONUS-3-KM-01-01-2026_0000_ang.asc",
        "gs://bucket/run/PASTCAST-GCP-HRRR-CONUS-3-KM-01-01-2026_0100_vel.asc",
        "gs://bucket/run/GENERIC-GFS-01-01-2026_0100_vel.asc",
    ]

    inventory = collect_raster_inventory(paths)

    assert len(inventory.solver_timestamps) == 2
    assert len(inventory.parent_timestamps) == 1


def test_coverage_for_pair_flags_incomplete_parent_hrrr():
    mass = parse_gcs_run_uri(
        "gs://bucket/runtime_temp/breck_tenmile_9p6_mass_20260101_0000_reanalysis_2h_HRRR/"
    )
    momentum = parse_gcs_run_uri(
        "gs://bucket/runtime_temp/breck_tenmile_9p6_20260101_0000_reanalysis_2h_HRRR/"
    )
    pair = GcsPair(mass=mass, momentum=momentum)
    mass_paths = [
        f"{mass.uri}/breck_tenmile_9p6_01-01-2026_0000_100m_vel.asc",
        f"{mass.uri}/breck_tenmile_9p6_01-01-2026_0000_100m_ang.asc",
        f"{mass.uri}/breck_tenmile_9p6_01-01-2026_0100_100m_vel.asc",
        f"{mass.uri}/breck_tenmile_9p6_01-01-2026_0100_100m_ang.asc",
        f"{mass.uri}/PASTCAST-GCP-HRRR-CONUS-3-KM-01-01-2026_0000_vel.asc",
        f"{mass.uri}/PASTCAST-GCP-HRRR-CONUS-3-KM-01-01-2026_0000_ang.asc",
    ]
    momentum_paths = [
        f"{momentum.uri}/breck_tenmile_9p6_01-01-2026_0000_100m_vel.asc",
        f"{momentum.uri}/breck_tenmile_9p6_01-01-2026_0000_100m_ang.asc",
        f"{momentum.uri}/breck_tenmile_9p6_01-01-2026_0100_100m_vel.asc",
        f"{momentum.uri}/breck_tenmile_9p6_01-01-2026_0100_100m_ang.asc",
    ]

    coverage = coverage_for_pair(pair, mass_paths, momentum_paths)

    assert coverage.status == "incomplete"
    assert "parent_hrrr" in coverage.reason
    assert coverage.paired_timestamp_count == 1


def test_pair_observation_coverage_counts_matched_station_hours():
    mass = parse_gcs_run_uri(
        "gs://bucket/runtime_temp/breck_tenmile_9p6_mass_20260101_0000_reanalysis_2h_HRRR/"
    )
    momentum = parse_gcs_run_uri(
        "gs://bucket/runtime_temp/breck_tenmile_9p6_20260101_0000_reanalysis_2h_HRRR/"
    )
    pair = GcsPair(mass=mass, momentum=momentum)
    observations = {
        "CABP6": [
            {"datetime": dt.datetime(2026, 1, 1, 0, 10, tzinfo=UTC)},
            {"datetime": dt.datetime(2026, 1, 1, 2, 45, tzinfo=UTC)},
        ],
        "CAHSB": [],
    }

    counts = pair_observation_counts(pair, observations, tolerance_minutes=30)
    observed_timestamps = pair_observed_timestamps(pair, observations, tolerance_minutes=30)
    rows = observation_coverage_rows([pair], observations, tolerance_minutes=30)

    assert counts == {"CABP6": 1, "CAHSB": 0}
    assert observed_timestamps == {dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC)}
    assert rows[0]["matched_station_hour_count"] == 1
    assert rows[0]["status"] == "has_observations"
    assert rows[0]["CABP6_matched_station_hours"] == 1
    assert rows[0]["CAHSB_matched_station_hours"] == 0


def test_station_sampling_and_metric_aggregation_with_fake_rasters():
    stamp = dt.datetime(2026, 1, 1, tzinfo=UTC)
    model_name = "breck_tenmile_9p6_specific_lcp_canopy_v2"
    source_sets = {
        stamp: {
            "hrrr": RasterSource(Path("hrrr_vel.asc"), Path("hrrr_ang.asc")),
            "mass": RasterSource(Path("mass_vel.asc"), Path("mass_ang.asc")),
            "momentum": RasterSource(Path("momentum_vel.asc"), Path("momentum_ang.asc")),
            model_name: RasterSource(Path("ml_vel.asc"), Path("ml_ang.asc")),
        }
    }
    station_records = [{
        "station_id": "CABP6",
        "label": "Breckenridge Peak 6 CAIC",
        "group": "ridge",
        "height_m": 10.0,
        "longitude": -106.0,
        "latitude": 39.0,
    }]
    u_obs, v_obs = sv.obs_to_uv(10.0, 270.0)
    observations = {
        "CABP6": [{
            "datetime": stamp,
            "speed_obs": 10.0,
            "dir_obs_deg": 270.0,
            "u_obs": u_obs,
            "v_obs": v_obs,
        }]
    }
    values = {
        "hrrr_vel.asc": 7.0,
        "hrrr_ang.asc": 270.0,
        "mass_vel.asc": 8.0,
        "mass_ang.asc": 270.0,
        "momentum_vel.asc": 11.0,
        "momentum_ang.asc": 270.0,
        "ml_vel.asc": 10.5,
        "ml_ang.asc": 270.0,
    }

    def fake_sampler(path: Path, lon: float, lat: float) -> float | None:
        assert lon == -106.0
        assert lat == 39.0
        return values[path.name]

    sample_rows = build_station_sample_rows(
        station_records,
        source_sets,
        observations,
        model_names=[model_name],
        tolerance_minutes=30,
        sampler=fake_sampler,
    )
    model_rows = model_summary_rows(sample_rows, [model_name])
    emulator_rows = emulator_summary_rows(sample_rows, [model_name])
    model_prefix = safe_source_name(model_name)
    ml_emulator = next(row for row in emulator_rows if row["scope"] == "ALL" and row["source"] == model_name)

    assert len(sample_rows) == 1
    assert sample_rows[0][f"{model_prefix}_sample_status"] == "ok"
    assert sample_rows[0]["mass_vector_error"] == pytest.approx(2.0)
    assert next(row for row in model_rows if row["source"] == "hrrr")["vector_rmse"] == pytest.approx(3.0)
    assert ml_emulator["vector_rmse_vs_momentum"] == pytest.approx(0.5)
    assert ml_emulator["vector_rmse_improvement_vs_mass_percent"] == pytest.approx(83.333333)


def test_sample_ascii_grid_uses_nearest_cell_center(tmp_path):
    raster = tmp_path / "grid.asc"
    raster.write_text(
        "\n".join([
            "ncols 3",
            "nrows 2",
            "xllcorner 0",
            "yllcorner 0",
            "cellsize 1",
            "NODATA_value -9999",
            "1 2 3",
            "4 -9999 6",
            "",
        ]),
        encoding="utf-8",
    )
    header = {
        "ncols": 3,
        "nrows": 2,
        "xllcorner": 0,
        "yllcorner": 0,
        "cellsize": 1,
    }

    assert ascii_grid_indices(header, 0.5, 1.5) == (0, 0)
    samples = sample_ascii_grid(
        raster,
        [
            ("top_left", 0.5, 1.5),
            ("bottom_middle_nodata", 1.5, 0.5),
            ("outside", 4.0, 0.5),
        ],
    )

    assert samples == {
        "top_left": 1.0,
        "bottom_middle_nodata": None,
        "outside": None,
    }


def test_copy_projection_sidecars_to_ml_outputs(tmp_path):
    reference = tmp_path / "mass_vel.asc"
    reference.write_text("", encoding="utf-8")
    reference.with_suffix(".prj").write_text("PROJCS[\"test\"]\n", encoding="utf-8")
    outputs = [tmp_path / "ml_vel.asc", tmp_path / "ml_ang.asc"]
    for output in outputs:
        output.write_text("", encoding="utf-8")

    _copy_projection_sidecars(reference, outputs)

    assert (tmp_path / "ml_vel.prj").read_text(encoding="utf-8") == "PROJCS[\"test\"]\n"
    assert (tmp_path / "ml_ang.prj").read_text(encoding="utf-8") == "PROJCS[\"test\"]\n"
