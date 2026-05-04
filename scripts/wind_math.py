"""Wind vector and ASCII-grid helpers."""
from __future__ import annotations

import math
from pathlib import Path
from typing import Iterable


ASC_HEADER_LINE_COUNT = 6


def is_nodata(value: float, nodata: float | None) -> bool:
    """Return true when *value* should be treated as grid no-data."""
    if math.isnan(value):
        return True
    if nodata is None:
        return False
    if math.isnan(nodata):
        return math.isnan(value)
    return value == nodata


def speed_direction_from_uv(
    u: float,
    v: float,
    *,
    nodata: float | None = None,
) -> tuple[float | None, float | None]:
    """Convert U/V wind components into speed and meteorological direction."""
    if is_nodata(u, nodata) or is_nodata(v, nodata):
        return None, None
    speed = math.hypot(u, v)
    direction = (270.0 - math.degrees(math.atan2(v, u))) % 360.0
    return speed, direction


def read_asc_header(path: str | Path) -> tuple[list[str], dict[str, str]]:
    """Read the standard six-line AAIGrid header."""
    header_lines: list[str] = []
    header: dict[str, str] = {}
    with Path(path).open("r", encoding="utf-8") as f:
        for _ in range(ASC_HEADER_LINE_COUNT):
            line = f.readline()
            if not line:
                raise ValueError(f"{path} ended before the AAIGrid header was complete.")
            header_lines.append(line.rstrip("\n"))
            parts = line.strip().split(maxsplit=1)
            if len(parts) == 2:
                header[parts[0].lower()] = parts[1].strip()
    return header_lines, header


def asc_shape(path: str | Path) -> tuple[int, int]:
    """Return (ncols, nrows) from an AAIGrid header."""
    _lines, header = read_asc_header(path)
    return int(header["ncols"]), int(header["nrows"])


def asc_nodata_value(header: dict[str, str], default: float | None = None) -> float | None:
    """Return the no-data value from an AAIGrid header."""
    raw = header.get("nodata_value")
    if raw is None:
        return default
    return float(raw)


def iter_asc_data_rows(path: str | Path) -> Iterable[list[float]]:
    """Yield numeric data rows from an AAIGrid file."""
    with Path(path).open("r", encoding="utf-8") as f:
        for _ in range(ASC_HEADER_LINE_COUNT):
            next(f)
        for line in f:
            stripped = line.strip()
            if stripped:
                yield [float(value) for value in stripped.split()]


def asc_has_nodata(path: str | Path, nodata: float | None = None) -> bool:
    """Return true when any data cell equals the configured no-data value."""
    _lines, header = read_asc_header(path)
    effective_nodata = asc_nodata_value(header, nodata)
    for row in iter_asc_data_rows(path):
        for value in row:
            if is_nodata(value, effective_nodata):
                return True
    return False
