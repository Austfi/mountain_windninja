from __future__ import annotations

import datetime as dt
import json

import numpy as np

from scripts import hrrrcast_ensemble


HEADER = "\n".join(
    [
        "ncols 2",
        "nrows 2",
        "xllcorner 0",
        "yllcorner 0",
        "cellsize 1",
        "NODATA_value -9999",
    ]
)


def _write_grid(path, values):
    path.write_text(
        f"{HEADER}\n"
        + "\n".join(" ".join(str(value) for value in row) for row in values)
        + "\n",
        encoding="utf-8",
    )


def test_write_ensemble_summary_writes_speed_products(tmp_path):
    m00 = tmp_path / "m00"
    m01 = tmp_path / "m01"
    m00.mkdir()
    m01.mkdir()
    _write_grid(m00 / "test_20260601_0100_vel.asc", [[1, 2], [3, 4]])
    _write_grid(m00 / "GENERIC-06-01-2026_0100_vel.asc", [[99, 99], [99, 99]])
    _write_grid(m01 / "test_20260601_0100_vel.asc", [[3, 4], [5, 6]])
    _write_grid(m01 / "GENERIC-06-01-2026_0100_vel.asc", [[99, 99], [99, 99]])

    summary_path = hrrrcast_ensemble.write_ensemble_summary(
        member_output_dirs={"m00": m00, "m01": m01},
        summary_dir=tmp_path / "ensemble",
        domain_key="test",
        start_time=dt.datetime(2026, 6, 1, 1),
        stop_time=dt.datetime(2026, 6, 1, 2),
    )

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["members"] == ["m00", "m01"]
    assert len(summary["products"]) == 1
    assert summary["products"][0]["timestamp"] == "20260601_0100"
    mean_path = summary["products"][0]["files"]["mean"]
    mean = np.loadtxt(mean_path, skiprows=6)
    assert mean.tolist() == [[2.0, 3.0], [4.0, 5.0]]
    spread_path = summary["products"][0]["files"]["spread"]
    spread = np.loadtxt(spread_path, skiprows=6)
    assert np.allclose(spread, [[1.6, 1.6], [1.6, 1.6]])
