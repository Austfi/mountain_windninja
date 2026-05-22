"""Wind vector math for gridded WindNinja ML experiments."""
from __future__ import annotations

import math
from typing import Literal

SpeedUnits = Literal["mps", "mph", "kph", "kts"]


def speed_to_mps_factor(units: SpeedUnits) -> float:
    if units == "mps":
        return 1.0
    if units == "mph":
        return 0.44704
    if units == "kph":
        return 1000.0 / 3600.0
    if units == "kts":
        return 0.514444
    raise ValueError(f"Unsupported speed units: {units}")


def mps_to_speed_factor(units: SpeedUnits) -> float:
    return 1.0 / speed_to_mps_factor(units)


def speed_direction_to_uv(speed, direction_deg, *, units: SpeedUnits = "mps"):
    """Convert meteorological speed/direction into eastward/northward u/v."""
    import numpy as np

    speed_mps = np.asarray(speed, dtype=np.float32) * speed_to_mps_factor(units)
    theta = np.deg2rad(np.asarray(direction_deg, dtype=np.float32))
    u = -speed_mps * np.sin(theta)
    v = -speed_mps * np.cos(theta)
    return u.astype(np.float32), v.astype(np.float32)


def uv_to_speed_direction(u, v, *, units: SpeedUnits = "mps"):
    """Convert eastward/northward u/v into meteorological speed/direction."""
    import numpy as np

    u_values = np.asarray(u, dtype=np.float32)
    v_values = np.asarray(v, dtype=np.float32)
    speed_mps = vector_speed(u_values, v_values)
    speed = speed_mps * mps_to_speed_factor(units)
    direction = (np.rad2deg(np.arctan2(-u_values, -v_values)) + 360.0) % 360.0
    return speed.astype(np.float32), direction.astype(np.float32)


def vector_speed(u, v):
    import numpy as np

    return np.sqrt(np.asarray(u, dtype=np.float32) ** 2 + np.asarray(v, dtype=np.float32) ** 2)


def vector_rmse(pred_uv, target_uv, mask=None) -> float:
    import numpy as np

    pred = np.asarray(pred_uv, dtype=np.float32)
    target = np.asarray(target_uv, dtype=np.float32)
    err = np.sum((pred - target) ** 2, axis=0)
    if mask is not None:
        err = err[np.asarray(mask, dtype=bool)]
    if err.size == 0:
        return math.nan
    return float(np.sqrt(np.mean(err)))


def speed_mae(pred_uv, target_uv, mask=None) -> float:
    import numpy as np

    pred = np.asarray(pred_uv, dtype=np.float32)
    target = np.asarray(target_uv, dtype=np.float32)
    err = np.abs(vector_speed(pred[0], pred[1]) - vector_speed(target[0], target[1]))
    if mask is not None:
        err = err[np.asarray(mask, dtype=bool)]
    if err.size == 0:
        return math.nan
    return float(np.mean(err))
