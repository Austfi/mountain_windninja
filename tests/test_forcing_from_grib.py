from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

import scripts.forcing_from_grib as forcing


def _asc(path: Path, rows: list[str]) -> None:
    path.write_text(
        "\n".join([
            "ncols 2",
            "nrows 2",
            "xllcorner 0",
            "yllcorner 0",
            "cellsize 1",
            "NODATA_value -9999",
            *rows,
        ]) + "\n",
        encoding="utf-8",
    )


def test_select_candidate_uses_var_level_and_time():
    candidates = [
        forcing.DatasetCandidate("input.grib", 1, "UGRD 10 m 202601010000", {}),
        forcing.DatasetCandidate("input.grib", 2, "UGRD 80 m 202601010000", {}),
        forcing.DatasetCandidate("input.grib", 3, "VGRD 10 m 202601010000", {}),
    ]

    selected = forcing.select_candidate(
        candidates,
        var_name="UGRD",
        level="10m",
        raw_time="202601010000",
    )

    assert selected.band == 1


def test_select_candidate_reports_ambiguous_matches():
    candidates = [
        forcing.DatasetCandidate("input.grib", 1, "UGRD 10 m 202601010000", {}),
        forcing.DatasetCandidate("input.grib", 2, "UGRD 10 m 202601010100", {}),
    ]

    with pytest.raises(forcing.ForcingError, match="Ambiguous"):
        forcing.select_candidate(
            candidates,
            var_name="UGRD",
            level="10m",
            raw_time="202601010030",
        )


def test_write_speed_direction_grids_handles_cardinals_and_nodata(tmp_path):
    u_grid = tmp_path / "u.asc"
    v_grid = tmp_path / "v.asc"
    speed_grid = tmp_path / "speed.asc"
    direction_grid = tmp_path / "direction.asc"
    _asc(u_grid, ["1 0", "-1 -9999"])
    _asc(v_grid, ["0 1", "0 0"])

    forcing.write_speed_direction_grids(u_grid, v_grid, speed_grid, direction_grid)

    speed_lines = speed_grid.read_text(encoding="utf-8").splitlines()
    direction_lines = direction_grid.read_text(encoding="utf-8").splitlines()
    assert speed_lines[6:] == ["1.000000 1.000000", "1.000000 -9999"]
    assert direction_lines[6:] == ["270.000000 180.000000", "90.000000 -9999"]


def test_write_speed_direction_pair_grids_converts_speed_units(tmp_path):
    source_speed = tmp_path / "source_speed.asc"
    source_direction = tmp_path / "source_direction.asc"
    speed_grid = tmp_path / "speed.asc"
    direction_grid = tmp_path / "direction.asc"
    _asc(source_speed, ["10 -9999", "1 2"])
    _asc(source_direction, ["270 180", "-9999 361"])

    forcing.write_speed_direction_pair_grids(
        source_speed,
        source_direction,
        speed_grid,
        direction_grid,
        input_speed_units="mps",
        output_speed_units="mph",
    )

    speed_lines = speed_grid.read_text(encoding="utf-8").splitlines()
    direction_lines = direction_grid.read_text(encoding="utf-8").splitlines()
    assert speed_lines[6:] == ["22.369363 -9999", "-9999 4.473873"]
    assert direction_lines[6:] == ["270.000000 -9999", "-9999 1.000000"]


def test_write_prj_sidecars_uses_esri_wkt(tmp_path, monkeypatch):
    speed_grid = tmp_path / "speed.asc"
    direction_grid = tmp_path / "direction.asc"

    def fake_run(command, capture_output, text, check):
        assert command[:3] == ["gdalsrsinfo", "-o", "wkt_esri"]
        return subprocess.CompletedProcess(command, 0, stdout='PROJCS["Test"]\n', stderr="")

    monkeypatch.setattr(forcing.subprocess, "run", fake_run)

    forcing.write_prj_sidecars(speed_grid, direction_grid, 'PROJCRS["Test"]')

    assert speed_grid.with_suffix(".prj").read_text(encoding="utf-8") == 'PROJCS["Test"]\n'
    assert direction_grid.with_suffix(".prj").read_text(encoding="utf-8") == 'PROJCS["Test"]\n'


def test_warp_candidate_to_terrain_builds_gdalwarp_command(tmp_path, monkeypatch):
    commands = []

    def fake_run(command, capture_output, text, check):
        commands.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(forcing.subprocess, "run", fake_run)
    terrain = forcing.TerrainGrid(
        path=tmp_path / "terrain.tif",
        size=(10, 20),
        geo_transform=(100.0, 30.0, 0.0, 200.0, 0.0, -30.0),
        wkt="EPSG:32613",
    )
    candidate = forcing.DatasetCandidate("input.grib", 2, "UGRD 10 m", {})

    forcing.warp_candidate_to_terrain(candidate, terrain, tmp_path / "u.asc")

    translate_command = commands[0]
    assert translate_command[:6] == ["gdal_translate", "-q", "-of", "VRT", "-b", "2"]

    command = commands[1]
    assert command[:6] == ["gdalwarp", "-overwrite", "-of", "AAIGrid", "-r", "bilinear"]
    assert "-b" not in command
    assert command[command.index("-te") + 1:command.index("-te") + 5] == [
        "70.0",
        "-430.0",
        "430.0",
        "230.0",
    ]
    assert command[command.index("-ts") + 1:command.index("-ts") + 3] == ["12", "22"]
    assert command[-1:] == [str(tmp_path / "u.asc")]
