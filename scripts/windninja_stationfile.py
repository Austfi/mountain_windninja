from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Iterable, Mapping

# Column names per WindNinja "station file" (point initialization) format.
DEFAULT_STATION_FIELDS: list[str] = [
    "Station_Name",
    "Coord_Sys",
    "Datum",
    "Lat/YCoord",
    "Lon/XCoord",
    "Height",
    "Height_Units",
    "Speed",
    "Speed_Units",
    "Direction",
    "Temperature",
    "Temperature_Units",
    "Cloud_Cover",
    "Radius_of_Influence",
    "Radius_of_Influence_Units",
]


def read_station_template_fieldnames(template_path: str | Path) -> list[str]:
    """
    Reads a WindNinja station CSV template and returns the header fieldnames in order.
    The template can be a "blank station file" containing only the header row.
    """
    template_path = Path(template_path)
    if not template_path.exists():
        raise FileNotFoundError(f"Template CSV not found: {template_path}")

    with template_path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"Template CSV has no header row: {template_path}")
        return list(reader.fieldnames)


def write_windninja_station_csv(
    template_path: str | Path,
    out_path: str | Path,
    stations: Iterable[Mapping[str, Any]],
) -> None:
    """
    Writes a WindNinja point-initialization station file (.csv) using a template to
    preserve column order and any custom header names.
    """
    fieldnames = read_station_template_fieldnames(template_path)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for st in stations:
            row = {k: st.get(k, "") for k in fieldnames}
            writer.writerow(row)


def build_windninja_station_row(
    station_name: str,
    lat: float,
    lon: float,
    *,
    height: float = 10.0,
    height_units: str = "meters",
    speed: float | None = None,
    speed_units: str = "mph",
    direction: float | None = None,
    temperature: float | None = None,
    temperature_units: str = "F",
    cloud_cover: int | None = 0,
    radius_of_influence: float = -1.0,
    radius_units: str = "miles",
    coord_sys: str = "GEOGCS",
    datum: str = "WGS84",
) -> dict[str, Any]:
    """
    Convenience helper to populate WindNinja station file fields.
    Any None measurement fields are written as blank cells.
    """
    def _blank_if_none(v: Any) -> Any:
        return "" if v is None else v

    return {
        "Station_Name": station_name,
        "Coord_Sys": coord_sys,
        "Datum": datum,
        "Lat/YCoord": lat,
        "Lon/XCoord": lon,
        "Height": height,
        "Height_Units": height_units,
        "Speed": _blank_if_none(speed),
        "Speed_Units": speed_units,
        "Direction": _blank_if_none(direction),
        "Temperature": _blank_if_none(temperature),
        "Temperature_Units": temperature_units,
        "Cloud_Cover": _blank_if_none(cloud_cover),
        "Radius_of_Influence": radius_of_influence,
        "Radius_of_Influence_Units": radius_units,
    }
