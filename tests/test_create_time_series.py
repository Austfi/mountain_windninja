from __future__ import annotations

import zipfile
from pathlib import Path

import scripts.create_time_series as create_time_series


def _write_hourly_kmz(path: Path):
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("doc.kml", "<kml><Document /></kml>")


def _write_hourly_kmz_with_kml(path: Path, kml: str):
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("doc.kml", kml)


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


def test_create_playable_kmz_normalizes_windninja_gmt_labels(tmp_path):
    raw_kml = """
<kml>
  <Document>
    <name>Simulation time is 2026-Jan-01 00:00:00 GMT</name>
    <description>Valid 2026-Jan-01 00:00:00 GMT</description>
  </Document>
</kml>
"""
    hourly_kmz = tmp_path / "my_area_20260101_0000_80m.kmz"
    _write_hourly_kmz_with_kml(hourly_kmz, raw_kml)

    output_path = create_time_series.create_playable_kmz(
        str(tmp_path), "bundle", domain_label="My Area"
    )

    with zipfile.ZipFile(hourly_kmz, "r") as zf:
        normalized_hourly = zf.read("doc.kml").decode("utf-8")
    assert "GMT" not in normalized_hourly
    assert "2026-Jan-01 00:00:00 UTC" in normalized_hourly

    with zipfile.ZipFile(output_path, "r") as zf:
        embedded_kml = zf.read("20260101_0000/doc.kml").decode("utf-8")
    assert "GMT" not in embedded_kml
    assert "2026-Jan-01 00:00:00 UTC" in embedded_kml
