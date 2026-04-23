from __future__ import annotations

import datetime as dt
from pathlib import Path

from scripts import raster_validation as rv


UTC = dt.timezone.utc


def test_parse_run_label():
    assert rv.parse_run_label("01-01-2026_0000") == dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC)


def test_collect_raster_sets_requires_complete_pairs(tmp_path):
    complete = [
        "loveland_validation_01-01-2026_0000_100m_vel.asc",
        "loveland_validation_01-01-2026_0000_100m_ang.asc",
        "PASTCAST-GCP-HRRR-CONUS-3-KM-01-01-2026_0000_vel.asc",
        "PASTCAST-GCP-HRRR-CONUS-3-KM-01-01-2026_0000_ang.asc",
    ]
    incomplete = [
        "loveland_validation_01-01-2026_0100_100m_vel.asc",
        "PASTCAST-GCP-HRRR-CONUS-3-KM-01-01-2026_0100_vel.asc",
    ]

    for name in complete + incomplete:
        (tmp_path / name).write_text("0", encoding="utf-8")

    raster_sets = rv.collect_raster_sets(tmp_path)

    assert list(raster_sets) == [dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC)]
    paths = raster_sets[dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC)]
    assert paths["wn_vel"] == tmp_path / complete[0]
    assert paths["wn_ang"] == tmp_path / complete[1]
    assert paths["wx_vel"] == tmp_path / complete[2]
    assert paths["wx_ang"] == tmp_path / complete[3]


def test_summary_rows_station_id_includes_station_metadata():
    sample_rows = [{
        "station_id": "CALVP",
        "station_label": "Loveland Pass",
        "group": "ridge",
        "height_m": 10.0,
        "wn_speed_error": 1.0,
        "wx_speed_error": 2.0,
        "wn_dir_abs_error_deg": 5.0,
        "wx_dir_abs_error_deg": 10.0,
        "wn_vector_error": 1.5,
        "wx_vector_error": 3.0,
    }]

    rows = rv.summary_rows(sample_rows, "station_id")

    assert len(rows) == 1
    assert rows[0]["station_id"] == "CALVP"
    assert rows[0]["station_label"] == "Loveland Pass"
    assert rows[0]["group"] == "ridge"
    assert rows[0]["height_m"] == 10.0
