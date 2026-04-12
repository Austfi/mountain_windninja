from __future__ import annotations

import datetime as dt

from scripts import synoptic_validation as sv


UTC = dt.timezone.utc


def test_build_station_records_prefers_synoptic_sensor_height():
    payload = {
        "STATION": [{
            "STID": "CALVP",
            "NAME": "Loveland Pass",
            "LATITUDE": "39.66",
            "LONGITUDE": "-105.87",
            "ELEVATION": "12000",
            "UNITS": {"position": "m"},
            "SENSOR_VARIABLES": {
                "wind_speed": {
                    "wind_speed_1": {
                        "position": "10.0",
                        "PERIOD_OF_RECORD": {
                            "start": "2020-01-01T00:00:00Z",
                            "end": "2026-01-08T00:00:00Z",
                        },
                    }
                }
            },
        }]
    }
    manifest = [{
        "station_id": "CALVP",
        "label": "LOVELAND PASS",
        "group": "ridge",
        "height_m_override": "",
    }]

    records = sv.build_station_records(payload, manifest)

    assert records[0]["station_id"] == "CALVP"
    assert records[0]["height_m"] == 10.0
    assert records[0]["height_source"] == "synoptic_sensor_metadata"


def test_extract_station_observations_matches_set_keys_to_sensor_metadata():
    station = {
        "OBSERVATIONS": {
            "date_time": [
                "2026-01-01T00:00:00Z",
                "2026-01-01T01:00:00Z",
            ],
            "wind_speed_set_1": [12.0, 15.0],
            "wind_direction_set_1": [270.0, 280.0],
        },
        "SENSOR_VARIABLES": {
            "wind_speed": {
                "wind_speed_1": {"position": "10.0"},
            },
            "wind_direction": {
                "wind_direction_1": {"position": "10.0"},
            },
        },
    }

    rows = sv.extract_station_observations(station, target_height_m=10.0)

    assert len(rows) == 2
    assert rows[0]["datetime"] == dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    assert rows[0]["speed_obs"] == 12.0
    assert rows[0]["dir_obs_deg"] == 270.0


def test_nearest_observation_respects_tolerance():
    observations = [
        {"datetime": dt.datetime(2026, 1, 1, 0, 5, tzinfo=UTC)},
        {"datetime": dt.datetime(2026, 1, 1, 1, 20, tzinfo=UTC)},
    ]

    matched = sv.nearest_observation(
        observations,
        dt.datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
        tolerance_minutes=10,
    )
    missed = sv.nearest_observation(
        observations,
        dt.datetime(2026, 1, 1, 1, 0, tzinfo=UTC),
        tolerance_minutes=10,
    )

    assert matched == observations[0]
    assert missed is None


def test_summarize_samples_reports_improvement_when_windninja_is_better():
    samples = [
        {
            "wn_speed_error": 1.0,
            "wx_speed_error": 3.0,
            "wn_dir_abs_error_deg": 5.0,
            "wx_dir_abs_error_deg": 12.0,
            "wn_vector_error": 1.5,
            "wx_vector_error": 4.0,
        },
        {
            "wn_speed_error": -1.0,
            "wx_speed_error": -2.0,
            "wn_dir_abs_error_deg": 7.0,
            "wx_dir_abs_error_deg": 10.0,
            "wn_vector_error": 2.0,
            "wx_vector_error": 3.0,
        },
    ]

    summary = sv.summarize_samples(samples)

    assert summary["sample_count"] == 2
    assert summary["windninja"]["speed_mae"] < summary["hrrr"]["speed_mae"]
    assert summary["improvement"]["vector_rmse"] > 0
