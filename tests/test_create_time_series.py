from __future__ import annotations

import zipfile
from pathlib import Path

import scripts.create_time_series as create_time_series


def _write_hourly_kmz(path: Path):
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("doc.kml", "<kml><Document /></kml>")


def test_create_playable_kmz_uses_date_scoped_member_paths(tmp_path):
    _write_hourly_kmz(tmp_path / "my_area_20260101_0000_80m.kmz")
    _write_hourly_kmz(tmp_path / "my_area_20260102_0000_80m.kmz")

    output_path = create_time_series.create_playable_kmz(
        str(tmp_path), "bundle", domain_label="My Area"
    )

    with zipfile.ZipFile(output_path, "r") as zf:
        names = zf.namelist()
        assert "20260101_0000/doc.kml" in names
        assert "20260102_0000/doc.kml" in names

        master_kml = zf.read("doc.kml").decode("utf-8")
        assert "20260101_0000/doc.kml" in master_kml
        assert "20260102_0000/doc.kml" in master_kml
