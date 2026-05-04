from __future__ import annotations

import pytest

from scripts.wind_math import convert_speed, speed_direction_from_uv


@pytest.mark.parametrize(
    ("u", "v", "expected_direction"),
    [
        (1.0, 0.0, 270.0),
        (0.0, 1.0, 180.0),
        (-1.0, 0.0, 90.0),
        (0.0, -1.0, 0.0),
    ],
)
def test_speed_direction_from_uv_cardinal_directions(u, v, expected_direction):
    speed, direction = speed_direction_from_uv(u, v)

    assert speed == pytest.approx(1.0)
    assert direction == pytest.approx(expected_direction)


def test_speed_direction_from_uv_returns_mask_for_nodata():
    assert speed_direction_from_uv(-9999.0, 2.0, nodata=-9999.0) == (None, None)
    assert speed_direction_from_uv(2.0, -9999.0, nodata=-9999.0) == (None, None)


def test_convert_speed_between_mps_and_mph():
    assert convert_speed(10.0, "mps", "mph") == pytest.approx(22.3693629)
    assert convert_speed(22.3693629, "mph", "mps") == pytest.approx(10.0)
