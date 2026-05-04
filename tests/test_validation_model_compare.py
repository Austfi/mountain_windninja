from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts import validation_model_compare as vmc


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


def sample(station_id: str, stamp: str, obs: float, wn: float, wx: float) -> dict:
    return {
        "station_id": station_id,
        "station_label": station_id,
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
        "wn_vector_error": str(abs(wn - obs) + 1),
        "wx_speed": str(wx),
        "wx_dir_deg": "240",
        "wx_u": "8",
        "wx_v": "2",
        "wx_speed_error": str(wx - obs),
        "wx_dir_abs_error_deg": "30",
        "wx_vector_error": str(abs(wx - obs) + 2),
        "wn_vel_path": "wn.asc",
        "wx_vel_path": "wx.asc",
    }


def write_samples(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def test_compare_validation_roots_on_common_station_hours(tmp_path):
    hrrr_root = tmp_path / "validation" / "berthoud_pass"
    nbm_root = tmp_path / "validation" / "berthoud_pass_nbm"
    output_dir = tmp_path / "validation" / "model_comparison"

    common_rows_hrrr = [
        sample("K0CO", "2026-01-01T00:00:00Z", 20.0, 18.0, 14.0),
        sample("K0CO", "2026-01-01T01:00:00Z", 22.0, 21.0, 16.0),
        sample("CABTP", "2026-01-01T00:00:00Z", 10.0, 11.0, 14.0),
    ]
    common_rows_nbm = [
        sample("K0CO", "2026-01-01T00:00:00Z", 20.0, 19.0, 17.0),
        sample("K0CO", "2026-01-01T01:00:00Z", 22.0, 20.0, 21.0),
        sample("CABTP", "2026-01-01T00:00:00Z", 10.0, 9.0, 8.0),
        sample("CABTP", "2026-01-01T01:00:00Z", 12.0, 12.0, 12.0),
    ]
    write_samples(hrrr_root / "chunks" / "20260101_0000_20260102_0000" / "samples.csv", common_rows_hrrr)
    write_samples(nbm_root / "chunks" / "20260101_0000_20260102_0000" / "samples.csv", common_rows_nbm)
    (hrrr_root / "summary.json").write_text(json.dumps({"model": "HRRR"}), encoding="utf-8")
    (nbm_root / "summary.json").write_text(json.dumps({"model": "NBM"}), encoding="utf-8")

    result = vmc.main([
        str(hrrr_root),
        str(nbm_root),
        "--output-dir",
        str(output_dir),
    ])

    assert result == 0
    expected = {
        "metrics.csv",
        "summary.json",
        "index.html",
        "speed_mae_by_station.svg",
        "vector_rmse_by_station.svg",
    }
    assert expected <= {path.name for path in output_dir.iterdir()}

    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["common_sample_count"] == 3
    assert summary["station_count"] == 2

    rows = list(csv.DictReader((output_dir / "metrics.csv").open(encoding="utf-8")))
    all_rows = [row for row in rows if row["station_id"] == "ALL"]
    assert {row["series"] for row in all_rows} == {
        "HRRR",
        "WindNinja (HRRR)",
        "NBM",
        "WindNinja (NBM)",
    }
    nbm_all = next(row for row in all_rows if row["series"] == "NBM")
    assert nbm_all["sample_count"] == "3"
    assert nbm_all["speed_mae"] == "2.0"
