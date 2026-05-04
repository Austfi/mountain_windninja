from __future__ import annotations

import datetime as dt

import pytest

from scripts import nbm_archive


def test_nbm_grib_url_uses_cycle_and_lead():
    url = nbm_archive.nbm_grib_url(
        dt.datetime(2026, 1, 1, 0, 0),
        6,
        "co",
    )

    assert url.endswith("/blend.20260101/00/core/blend.t00z.core.f006.co.grib2")


def test_parse_index_and_find_main_wind_records():
    records = nbm_archive.parse_index(
        "\n".join([
            "1:0:d=2026010100:WDIR:10 m above ground:1 hour fcst:",
            "2:120:d=2026010100:WIND:10 m above ground:1 hour fcst:",
            "3:240:d=2026010100:WIND:10 m above ground:1 hour fcst:ens std dev",
        ])
    )

    assert nbm_archive.find_record(records, "WDIR", "10 m above ground").offset == 0
    wind = nbm_archive.find_record(records, "WIND", "10 m above ground")
    assert wind.offset == 120
    assert nbm_archive.record_end(records, wind) == 239


def test_find_record_reports_missing_variable():
    records = nbm_archive.parse_index(
        "1:0:d=2026010100:TMP:2 m above ground:1 hour fcst:"
    )

    with pytest.raises(ValueError, match="WIND"):
        nbm_archive.find_record(records, "WIND", "10 m above ground")
