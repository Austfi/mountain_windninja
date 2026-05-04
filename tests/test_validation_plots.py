from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts import validation_plots as vp


FIELDNAMES = [
    "station_id",
    "station_label",
    "group",
    "sample_time_utc",
    "obs_time_utc",
    "obs_age_minutes",
    "height_m",
    "speed_obs",
    "dir_obs_deg",
    "u_obs",
    "v_obs",
    "wn_speed",
    "wn_dir_deg",
    "wn_u",
    "wn_v",
    "wn_speed_error",
    "wn_dir_abs_error_deg",
    "wn_vector_error",
    "wx_speed",
    "wx_dir_deg",
    "wx_u",
    "wx_v",
    "wx_speed_error",
    "wx_dir_abs_error_deg",
    "wx_vector_error",
    "wn_vel_path",
    "wx_vel_path",
]


def write_samples(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def row(stamp: str, obs: float, wn: float, wx: float) -> dict:
    return {
        "station_id": "K0CO",
        "station_label": "Berthoud Pass",
        "group": "ridge",
        "sample_time_utc": stamp,
        "obs_time_utc": stamp,
        "obs_age_minutes": "0",
        "height_m": "10",
        "speed_obs": str(obs),
        "dir_obs_deg": "270",
        "u_obs": "10",
        "v_obs": "0",
        "wn_speed": str(wn),
        "wn_dir_deg": "260",
        "wn_u": "9",
        "wn_v": "1",
        "wn_speed_error": str(wn - obs),
        "wn_dir_abs_error_deg": "10",
        "wn_vector_error": "2",
        "wx_speed": str(wx),
        "wx_dir_deg": "240",
        "wx_u": "8",
        "wx_v": "2",
        "wx_speed_error": str(wx - obs),
        "wx_dir_abs_error_deg": "30",
        "wx_vector_error": "5",
        "wn_vel_path": "wn.asc",
        "wx_vel_path": "wx.asc",
    }


def test_plot_validation_outputs_svg_html_and_summary(tmp_path):
    study_root = tmp_path / "validation" / "berthoud_pass"
    write_samples(
        study_root / "chunks" / "20260101_0000_20260102_0000" / "samples.csv",
        [
            row("2026-01-01T00:00:00Z", 20.0, 18.0, 15.0),
            row("2026-01-01T01:00:00Z", 22.0, 19.0, 17.0),
        ],
    )
    write_samples(
        study_root / "chunks" / "20260102_0000_20260103_0000" / "samples.csv",
        [row("2026-01-02T00:00:00Z", 24.0, 21.0, 20.0)],
    )

    result = vp.main([
        "--study-root",
        str(study_root),
        "--output-dir",
        str(study_root / "plots"),
    ])

    assert result == 0
    expected = {
        "speed_timeseries.svg",
        "speed_error_timeseries.svg",
        "direction_error_timeseries.svg",
        "speed_scatter.svg",
        "daily_metrics.svg",
        "index.html",
        "plot_summary.json",
    }
    assert expected <= {path.name for path in (study_root / "plots").iterdir()}
    summary = json.loads((study_root / "plots" / "plot_summary.json").read_text())
    assert summary["summary"]["sample_count"] == 3
    assert summary["summary"]["windninja"]["speed_mae"] == 2.67
    assert summary["summary"]["hrrr"]["vector_rmse"] == 5.0


def test_plot_validation_uses_model_label_from_summary(tmp_path):
    study_root = tmp_path / "validation" / "berthoud_pass"
    write_samples(
        study_root / "chunks" / "20260101_0000_20260102_0000" / "samples.csv",
        [row("2026-01-01T00:00:00Z", 20.0, 18.0, 15.0)],
    )
    (study_root / "summary.json").write_text(
        json.dumps({"model": "NBM"}),
        encoding="utf-8",
    )

    result = vp.main([
        "--study-root",
        str(study_root),
        "--output-dir",
        str(study_root / "plots"),
    ])

    assert result == 0
    summary = json.loads((study_root / "plots" / "plot_summary.json").read_text())
    assert summary["summary"]["model_label"] == "NBM"
    index_html = (study_root / "plots" / "index.html").read_text(encoding="utf-8")
    assert "NBM Speed MAE" in index_html


def test_load_samples_deduplicates_overlapping_chunks(tmp_path):
    short_chunk = tmp_path / "chunks" / "20260101_0000_20260101_0100" / "samples.csv"
    full_chunk = tmp_path / "chunks" / "20260101_0000_20260102_0000" / "samples.csv"
    write_samples(short_chunk, [row("2026-01-01T00:00:00Z", 20.0, 10.0, 10.0)])
    write_samples(full_chunk, [row("2026-01-01T00:00:00Z", 20.0, 18.0, 15.0)])

    rows = vp.load_samples([short_chunk, full_chunk])

    assert len(rows) == 1
    assert rows[0]["wn_speed"] == 18.0
