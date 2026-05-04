from __future__ import annotations

from pathlib import Path

import pytest

import scripts.gridded_run as gridded_run
from scripts.config_loader import DomainConfig


def _domain(tmp_path: Path, suffix: str = ".tif") -> DomainConfig:
    template = tmp_path / "template.cfg"
    template.write_text("num_threads = 4\n", encoding="utf-8")
    terrain = tmp_path / f"terrain{suffix}"
    terrain.write_text("terrain", encoding="utf-8")
    return DomainConfig(
        key="test",
        label="Test",
        template_path=template,
        elevation_file=terrain,
    )


def _info(path: Path, size=(2, 2), wkt="EPSG:32613", nodata=None):
    return gridded_run.RasterInfo(
        path=path,
        size=size,
        geo_transform=(0.0, 1.0, 0.0, 2.0, 0.0, -1.0),
        wkt=wkt,
        nodata=nodata,
    )


def test_validate_grid_inputs_rejects_lcp_domains(tmp_path):
    speed = tmp_path / "speed.asc"
    direction = tmp_path / "direction.asc"

    with pytest.raises(gridded_run.GridValidationError, match="does not support .lcp"):
        gridded_run.validate_grid_inputs(speed, direction, _domain(tmp_path, ".lcp"))


def test_validate_grid_inputs_rejects_missing_files(tmp_path):
    direction = tmp_path / "direction.asc"
    direction.write_text("grid", encoding="utf-8")

    with pytest.raises(gridded_run.GridValidationError, match="Missing speed grid"):
        gridded_run.validate_grid_inputs(tmp_path / "speed.asc", direction, _domain(tmp_path))


def test_validate_grid_inputs_rejects_mismatched_dimensions(tmp_path, monkeypatch):
    speed = tmp_path / "speed.asc"
    direction = tmp_path / "direction.asc"
    speed.write_text("grid", encoding="utf-8")
    direction.write_text("grid", encoding="utf-8")
    domain = _domain(tmp_path)

    def fake_raster_info(path):
        if path == speed:
            return _info(path, size=(2, 2))
        if path == direction:
            return _info(path, size=(3, 2))
        return _info(path, size=(2, 2))

    monkeypatch.setattr(gridded_run, "_raster_info", fake_raster_info)

    with pytest.raises(gridded_run.GridValidationError, match="dimensions do not match"):
        gridded_run.validate_grid_inputs(speed, direction, domain)


def test_validate_grid_inputs_rejects_missing_crs(tmp_path, monkeypatch):
    speed = tmp_path / "speed.asc"
    direction = tmp_path / "direction.asc"
    speed.write_text("grid", encoding="utf-8")
    direction.write_text("grid", encoding="utf-8")
    domain = _domain(tmp_path)

    def fake_raster_info(path):
        if path == speed:
            raise gridded_run.GridValidationError(f"Missing CRS/projection: {path}")
        return _info(path)

    monkeypatch.setattr(gridded_run, "_raster_info", fake_raster_info)

    with pytest.raises(gridded_run.GridValidationError, match="Missing CRS"):
        gridded_run.validate_grid_inputs(speed, direction, domain)


def test_raster_info_uses_prj_sidecar_when_gdalinfo_lacks_crs(tmp_path, monkeypatch):
    speed = tmp_path / "speed.asc"
    speed.write_text("grid", encoding="utf-8")
    speed.with_suffix(".prj").write_text('PROJCS["Test"]', encoding="utf-8")

    monkeypatch.setattr(
        gridded_run,
        "_run_json",
        lambda command: {
            "size": [2, 2],
            "geoTransform": [0, 1, 0, 2, 0, -1],
            "bands": [{"noDataValue": -9999}],
        },
    )

    def fake_run(command, capture_output, text, check):
        assert command[:3] == ["gdalsrsinfo", "-o", "wkt"]
        return gridded_run.subprocess.CompletedProcess(
            command,
            0,
            stdout='PROJCS["Test"]\n',
            stderr="",
        )

    monkeypatch.setattr(gridded_run.subprocess, "run", fake_run)

    info = gridded_run._raster_info(speed)

    assert info.wkt == 'PROJCS["Test"]'


def test_validate_grid_inputs_rejects_nodata_overlap(tmp_path, monkeypatch):
    speed = tmp_path / "speed.asc"
    direction = tmp_path / "direction.asc"
    speed.write_text("grid", encoding="utf-8")
    direction.write_text("grid", encoding="utf-8")
    domain = _domain(tmp_path)

    monkeypatch.setattr(gridded_run, "_raster_info", lambda path: _info(path, nodata=-9999))
    monkeypatch.setattr(gridded_run, "_crs_matches", lambda left, right: True)
    monkeypatch.setattr(gridded_run, "_covers", lambda grid, terrain: True)
    monkeypatch.setattr(gridded_run, "_has_nodata_overlap", lambda grid, terrain: grid.path == speed)

    with pytest.raises(gridded_run.GridValidationError, match="Speed grid has no-data"):
        gridded_run.validate_grid_inputs(speed, direction, domain)
