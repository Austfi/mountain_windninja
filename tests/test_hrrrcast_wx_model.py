from __future__ import annotations

import datetime as dt

import pytest

from scripts import hrrrcast_wx_model


IDX_TEXT = "\n".join(
    [
        "1:0:d=2026060100:UGRD:10 m above ground:1 hour fcst:",
        "2:100:d=2026060100:VGRD:10 m above ground:1 hour fcst:",
        "3:240:d=2026060100:TMP:2 m above ground:1 hour fcst:",
        "4:400:d=2026060100:TCDC:entire atmosphere:1 hour fcst:",
        "5:520:d=2026060100:REFC:entire atmosphere:1 hour fcst:",
    ]
)


def _list_xml(prefixes):
    entries = "".join(
        f"<CommonPrefixes><Prefix>{prefix}</Prefix></CommonPrefixes>"
        for prefix in prefixes
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
        f"{entries}"
        "</ListBucketResult>"
    )


def test_expand_hrrrcast_members_supports_all_and_explicit_avg():
    assert hrrrcast_wx_model.expand_hrrrcast_members("all") == [
        f"m{idx:02d}" for idx in range(9)
    ]
    assert hrrrcast_wx_model.expand_hrrrcast_members("m00,avg,m00,m02") == [
        "m00",
        "avg",
        "m02",
    ]


def test_expand_hrrrcast_members_rejects_unknown_member():
    with pytest.raises(hrrrcast_wx_model.HrrrCastWeatherError, match="avg or m00..m08"):
        hrrrcast_wx_model.expand_hrrrcast_members("m09")


def test_hrrrcast_idx_parser_computes_required_byte_ranges():
    ranges = hrrrcast_wx_model.required_message_ranges(IDX_TEXT)

    assert ranges["u10"].byte_start == 0
    assert ranges["u10"].byte_end == 99
    assert ranges["v10"].byte_start == 100
    assert ranges["v10"].byte_end == 239
    assert ranges["t2m"].byte_start == 240
    assert ranges["t2m"].byte_end == 399
    assert ranges["tcc"].byte_start == 400
    assert ranges["tcc"].byte_end == 519


def test_hrrrcast_idx_parser_rejects_missing_required_field():
    idx_text = IDX_TEXT.replace("TCDC", "REFC")

    with pytest.raises(hrrrcast_wx_model.HrrrCastWeatherError, match="TCDC"):
        hrrrcast_wx_model.required_message_ranges(idx_text)


def test_hrrrcast_url_uses_public_s3_layout():
    cycle = dt.datetime(2026, 6, 1, 0)

    assert hrrrcast_wx_model.hrrrcast_idx_url(
        "https://noaa-gsl-experimental-pds.s3.amazonaws.com",
        cycle,
        "avg",
        1,
    ) == (
        "https://noaa-gsl-experimental-pds.s3.amazonaws.com/"
        "HRRRCast/20260601/00/hrrrcast.avg.t00z.pgrb2.f01.idx"
    )


def test_hrrrcast_lists_cycle_prefixes_from_s3_xml():
    prefixes = hrrrcast_wx_model.list_hrrrcast_cycle_prefixes(
        "https://example.test",
        dt.date(2026, 6, 2),
        fetch_text=lambda _url: _list_xml([
            "HRRRCast/20260602/00/",
            "HRRRCast/20260602/05/",
        ]),
    )

    assert prefixes == ("HRRRCast/20260602/00/", "HRRRCast/20260602/05/")


def test_hrrrcast_discovered_cycles_are_sorted_newest_first():
    def fake_fetch(url: str) -> str:
        if "20260602" in url:
            return _list_xml([
                "HRRRCast/20260602/00/",
                "HRRRCast/20260602/05/",
                "HRRRCast/20260602/06/",
            ])
        if "20260601" in url:
            return _list_xml(["HRRRCast/20260601/23/"])
        return _list_xml([])

    candidates = hrrrcast_wx_model.discover_hrrrcast_cycle_candidates(
        start_time=dt.datetime(2026, 6, 2, 22),
        max_cycle_rewind=48,
        base_url="https://example.test",
        fetch_text=fake_fetch,
    )

    assert candidates == (
        dt.datetime(2026, 6, 2, 6),
        dt.datetime(2026, 6, 2, 5),
        dt.datetime(2026, 6, 2, 0),
        dt.datetime(2026, 6, 1, 23),
    )


def test_resolve_hrrrcast_cycle_plan_uses_discovered_latest_complete_cycle():
    start = dt.datetime(2026, 6, 2, 22)
    stop = dt.datetime(2026, 6, 2, 23)
    seen_urls = []

    def fake_fetch(url: str) -> str:
        seen_urls.append(url)
        if "list-type=2" in url:
            if "20260602" in url:
                return _list_xml([
                    "HRRRCast/20260602/06/",
                    "HRRRCast/20260602/05/",
                    "HRRRCast/20260602/00/",
                ])
            return _list_xml([])
        if "t06z" in url:
            raise hrrrcast_wx_model.HrrrCastWeatherError("partial cycle")
        return IDX_TEXT

    plan = hrrrcast_wx_model.resolve_hrrrcast_cycle_plan(
        start_time=start,
        stop_time=stop,
        member="avg",
        base_url="https://example.test",
        max_cycle_rewind=48,
        fetch_idx_text=fake_fetch,
    )

    assert plan.cycle == dt.datetime(2026, 6, 2, 5)
    assert plan.forecast_hours == (17, 18)
    assert any("t06z" in url for url in seen_urls)
    assert any("t05z" in url for url in seen_urls)


def test_resolve_hrrrcast_cycle_plan_rewinds_until_complete_cycle():
    start = dt.datetime(2026, 6, 1, 1)
    stop = dt.datetime(2026, 6, 1, 2)
    seen_urls = []

    def fake_fetch(url: str) -> str:
        seen_urls.append(url)
        if "t01z" in url and ".f00.idx" in url:
            raise hrrrcast_wx_model.HrrrCastWeatherError("missing f00")
        return IDX_TEXT

    plan = hrrrcast_wx_model.resolve_hrrrcast_cycle_plan(
        start_time=start,
        stop_time=stop,
        member="avg",
        base_url="https://example.test",
        max_cycle_rewind=2,
        fetch_idx_text=fake_fetch,
    )

    assert plan.cycle == dt.datetime(2026, 6, 1, 0)
    assert plan.forecast_hours == (1, 2)
    assert any("t01z" in url for url in seen_urls)
    assert any("t00z" in url for url in seen_urls)


def test_resolve_hrrrcast_cycle_plan_rejects_window_beyond_cycle_coverage():
    start = dt.datetime(2026, 6, 1, 1)
    stop = dt.datetime(2026, 6, 2, 3)

    with pytest.raises(hrrrcast_wx_model.HrrrCastWeatherError, match="Could not prepare"):
        hrrrcast_wx_model.resolve_hrrrcast_cycle_plan(
            start_time=start,
            stop_time=stop,
            member="avg",
            base_url="https://example.test",
            max_cycle_rewind=0,
            fetch_idx_text=lambda _url: IDX_TEXT,
        )


def test_download_hrrrcast_subset_appends_required_ranges(tmp_path, monkeypatch):
    chunks = {
        (0, 99): b"u",
        (100, 239): b"v",
        (240, 399): b"t",
        (400, 519): b"c",
    }

    def fake_fetch_range(_url, start, end):
        return chunks[(start, end)]

    monkeypatch.setattr(hrrrcast_wx_model, "_fetch_range", fake_fetch_range)
    subset_path = tmp_path / "subset.grib2"

    hrrrcast_wx_model._download_hrrrcast_subset(
        subset_path,
        grib_url="https://example.test/input.grib2",
        idx_text=IDX_TEXT,
    )

    assert subset_path.read_bytes() == b"uvtc"
