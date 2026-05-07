from __future__ import annotations

import json
from pathlib import Path

from scripts import k0co_height_hrrr_validation as kh


def _asc(path: Path, rows: list[str]) -> None:
    path.write_text(
        "\n".join([
            "ncols 2",
            "nrows 1",
            "xllcorner 0",
            "yllcorner 0",
            "cellsize 1",
            "NODATA_value -9999",
            *rows,
        ]) + "\n",
        encoding="utf-8",
    )


def test_parse_hrrr_idx_selects_required_analysis_fields():
    records = kh.parse_hrrr_idx(
        "\n".join([
            "60:27174444:d=2026010100:UGRD:80 m above ground:anl:",
            "61:28322813:d=2026010100:VGRD:80 m above ground:anl:",
            "63:30959245:d=2026010100:HGT:surface:anl:",
            "77:44311332:d=2026010100:UGRD:10 m above ground:anl:",
            "78:46692947:d=2026010100:VGRD:10 m above ground:anl:",
        ])
    )

    selected = kh.select_hrrr_messages(records)

    assert selected["u10"].offset == 44311332
    assert selected["v10"].parameter == "VGRD"
    assert selected["u80"].level == "80 m above ground"
    assert selected["hgt"].level == "surface"


def test_write_adjusted_forcing_grids_blends_and_caps(tmp_path):
    u10 = tmp_path / "u10.asc"
    v10 = tmp_path / "v10.asc"
    u80 = tmp_path / "u80.asc"
    v80 = tmp_path / "v80.asc"
    hgt = tmp_path / "hgt.asc"
    dem = tmp_path / "dem.asc"
    speed = tmp_path / "speed.asc"
    direction = tmp_path / "direction.asc"

    _asc(u10, ["10 10"])
    _asc(v10, ["0 0"])
    _asc(u80, ["20 1"])
    _asc(v80, ["0 0"])
    _asc(hgt, ["1000 1000"])
    _asc(dem, ["1300 1300"])

    stats = kh.write_adjusted_forcing_grids(u10, v10, u80, v80, hgt, dem, speed, direction)

    assert stats["valid_cell_count"] == 2
    assert stats["adjusted_cell_count"] == 2
    assert stats["raw_fallback_count"] == 0
    assert stats["weight_mean"] == 1.0
    assert stats["cap_high_count"] == 1
    assert stats["cap_low_count"] == 1
    assert speed.read_text(encoding="utf-8").splitlines()[6:] == ["13.500000 7.500000"]
    assert direction.read_text(encoding="utf-8").splitlines()[6:] == [
        "270.000000 270.000000"
    ]


def test_write_adjusted_forcing_grids_keeps_raw_hrrr_outside_dem(tmp_path):
    u10 = tmp_path / "u10.asc"
    v10 = tmp_path / "v10.asc"
    u80 = tmp_path / "u80.asc"
    v80 = tmp_path / "v80.asc"
    hgt = tmp_path / "hgt.asc"
    dem = tmp_path / "dem.asc"
    speed = tmp_path / "speed.asc"
    direction = tmp_path / "direction.asc"

    _asc(u10, ["10 10"])
    _asc(v10, ["0 0"])
    _asc(u80, ["20 20"])
    _asc(v80, ["0 0"])
    _asc(hgt, ["1000 1000"])
    _asc(dem, ["1300 -9999"])

    stats = kh.write_adjusted_forcing_grids(u10, v10, u80, v80, hgt, dem, speed, direction)

    assert stats["valid_cell_count"] == 2
    assert stats["adjusted_cell_count"] == 1
    assert stats["raw_fallback_count"] == 1
    assert speed.read_text(encoding="utf-8").splitlines()[6:] == ["13.500000 10.000000"]
    assert direction.read_text(encoding="utf-8").splitlines()[6:] == [
        "270.000000 270.000000"
    ]


def test_write_speed_grid_in_units_converts_to_mph(tmp_path):
    source = tmp_path / "speed_mps.asc"
    target = tmp_path / "speed_mph.asc"
    _asc(source, ["10 -9999"])

    kh.write_speed_grid_in_units(source, target, "mph")

    assert target.read_text(encoding="utf-8").splitlines()[6:] == ["22.369363 -9999"]


def test_plan_counts_only_adjusted_windninja_runs(capsys):
    result = kh.main([
        "--start",
        "202601010000",
        "--end",
        "202601010300",
        "--chunk-hours",
        "24",
        "--plan",
    ])

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["external_grid_hour_count"] == 3
    assert payload["height_adjusted_grid_included"] is True
    assert payload["windninja_grid_run_count"] == 3
    assert payload["grid"] == "GMTED 500 m adjusted HRRR grid"
    assert payload["adjustment_resolution_m"] == 500


def test_hrrr_comparison_outputs_publish_documented_period_only(tmp_path):
    short_dirs = kh.hrrr_comparison_output_dirs(
        tmp_path,
        kh.vs.parse_utc("202601010000"),
        kh.vs.parse_utc("202601020000"),
    )
    documented_dirs = kh.hrrr_comparison_output_dirs(
        tmp_path,
        kh.vs.parse_utc("202601010000"),
        kh.vs.parse_utc("202604010000"),
    )

    assert short_dirs == [tmp_path / "hrrr_comparisons" / "202601010000_202601020000"]
    assert documented_dirs == [
        tmp_path / "hrrr_comparisons" / "202601010000_202604010000",
        tmp_path,
    ]


def test_hrrr_comparison_writes_html_view(tmp_path):
    rows = [
        {
            "station_id": "K0CO",
            "sample_time_utc": "2026-01-01T00:00:00Z",
            "obs_time_utc": "2026-01-01T00:00:00Z",
            "observed_speed": 20.0,
            "observed_dir_deg": 300.0,
            "hrrr_speed": 10.0,
            "hrrr_dir_deg": 290.0,
            "hrrr_speed_error": -10.0,
            "hrrr_dir_abs_error_deg": 10.0,
            "hrrr_vector_error": 10.5,
            "adjusted_hrrr_speed": 16.0,
            "adjusted_hrrr_dir_deg": 295.0,
            "adjusted_hrrr_speed_error": -4.0,
            "adjusted_hrrr_dir_abs_error_deg": 5.0,
            "adjusted_hrrr_vector_error": 4.5,
        },
    ]

    kh.write_hrrr_comparison_files(
        tmp_path,
        rows,
        kh.vs.parse_utc("202601010000"),
        kh.vs.parse_utc("202601010100"),
    )

    html = (tmp_path / "hrrr_comparison.html").read_text(encoding="utf-8")
    assert "GMTED2010" in html
    assert "Speed MAE Change" in html
    assert "-6.00 mph" in html
    assert "Adjusted HRRR" in html


def test_metadata_is_gmted_500m_detects_current_cache_version(tmp_path):
    metadata = tmp_path / "adjustment_metadata.json"
    metadata.write_text('{"stats": {"valid_cell_count": 10}}\n', encoding="utf-8")
    assert not kh.metadata_is_gmted_500m(metadata)

    metadata.write_text(
        '{"adjustment_grid_version": "gmted_500m_v1"}\n',
        encoding="utf-8",
    )
    assert not kh.metadata_is_gmted_500m(metadata)

    metadata.write_text(
        '{"adjustment_grid_version": "gmted_500m_v2"}\n',
        encoding="utf-8",
    )
    assert kh.metadata_is_gmted_500m(metadata)


def test_cleanup_ninjafoam_caches_removes_only_matching_domain(tmp_path, monkeypatch):
    monkeypatch.setattr(kh.config_loader, "STATIC_DATA_DIR", tmp_path)
    matching = tmp_path / "NINJAFOAM_berthoud_pass_123_4"
    other_domain = tmp_path / "NINJAFOAM_loveland_pass_123_4"
    terrain = tmp_path / "berthoud_pass.tif"
    matching.mkdir()
    other_domain.mkdir()
    terrain.write_text("terrain\n", encoding="utf-8")

    removed = kh.cleanup_ninjafoam_caches("berthoud_pass")

    assert removed == 1
    assert not matching.exists()
    assert other_domain.exists()
    assert terrain.exists()


def test_mode_validation_complete_requires_samples_and_summary(tmp_path):
    chunk = kh.vs.plan_chunks(kh.vs.parse_utc("202601010000"), kh.vs.parse_utc("202601020000"), 24)[0]
    paths = kh.mode_chunk_paths(tmp_path, chunk, "height_adjusted_grid")

    assert not kh.mode_validation_is_complete(tmp_path, chunk, "height_adjusted_grid")

    paths["samples"].parent.mkdir(parents=True)
    paths["samples"].write_text("station_id\nK0CO\n", encoding="utf-8")
    assert not kh.mode_validation_is_complete(tmp_path, chunk, "height_adjusted_grid")

    paths["summary"].write_text("{}\n", encoding="utf-8")
    assert kh.mode_validation_is_complete(tmp_path, chunk, "height_adjusted_grid")
