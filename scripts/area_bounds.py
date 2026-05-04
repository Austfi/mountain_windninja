#!/usr/bin/env python3
"""Resolve beginner-friendly area inputs into WindNinja bounding boxes."""
from __future__ import annotations

import argparse
import math
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from xml.etree import ElementTree


EARTH_RADIUS_KM = 6371.0088
KM_PER_DEG_LAT = (2 * math.pi * EARTH_RADIUS_KM) / 360
MI_TO_KM = 1.609344
MAX_CENTER_SIZE_KM = 50.0


class AreaBoundsError(ValueError):
    """Raised for user-fixable area input problems."""


@dataclass(frozen=True)
class Bounds:
    north: float
    east: float
    south: float
    west: float

    def as_cli_fields(self) -> str:
        return (
            f"{self.north:.8f} {self.east:.8f} "
            f"{self.south:.8f} {self.west:.8f}"
        )


def _parse_float(raw: str, label: str) -> float:
    try:
        return float(raw)
    except ValueError as exc:
        raise AreaBoundsError(f"{label} must be numeric.") from exc


def _validate_lat_lon(lat: float, lon: float) -> None:
    if not -90.0 <= lat <= 90.0:
        raise AreaBoundsError("Latitude must be between -90 and 90.")
    if not -180.0 <= lon <= 180.0:
        raise AreaBoundsError("Longitude must be between -180 and 180.")


def validate_bounds(bounds: Bounds) -> Bounds:
    for lat, label in ((bounds.north, "North latitude"), (bounds.south, "South latitude")):
        if not -90.0 <= lat <= 90.0:
            raise AreaBoundsError(f"{label} must be between -90 and 90.")
    for lon, label in ((bounds.east, "East longitude"), (bounds.west, "West longitude")):
        if not -180.0 <= lon <= 180.0:
            raise AreaBoundsError(f"{label} must be between -180 and 180.")
    if bounds.north <= bounds.south:
        raise AreaBoundsError("Resolved area has no north/south extent.")
    if bounds.east <= bounds.west:
        raise AreaBoundsError(
            "Resolved area has no east/west extent or crosses the antimeridian."
        )
    return bounds


def bbox_from_center(lat: str | float, lon: str | float, size_km: str | float) -> Bounds:
    """Return a square WGS84 bbox centered on lat/lon."""
    center_lat = _parse_float(str(lat), "Center latitude")
    center_lon = _parse_float(str(lon), "Center longitude")
    size = _parse_float(str(size_km), "Size")
    _validate_lat_lon(center_lat, center_lon)

    if size <= 0:
        raise AreaBoundsError("Size must be greater than 0 km.")
    if size > MAX_CENTER_SIZE_KM:
        raise AreaBoundsError(
            f"Center/size boxes are limited to {MAX_CENTER_SIZE_KM:g} km. "
            "Use explicit bbox for larger advanced domains."
        )

    half_km = size / 2
    lat_delta = half_km / KM_PER_DEG_LAT
    cos_lat = math.cos(math.radians(center_lat))
    if abs(cos_lat) < 0.01:
        raise AreaBoundsError("Center latitude is too close to a pole for size-based bounds.")
    lon_delta = half_km / (KM_PER_DEG_LAT * cos_lat)

    return validate_bounds(
        Bounds(
            north=center_lat + lat_delta,
            east=center_lon + lon_delta,
            south=center_lat - lat_delta,
            west=center_lon - lon_delta,
        )
    )


def expand_bounds(bounds: Bounds, padding_km: float) -> Bounds:
    if padding_km < 0:
        raise AreaBoundsError("Padding must be 0 or greater.")
    if padding_km == 0:
        return validate_bounds(bounds)

    center_lat = (bounds.north + bounds.south) / 2
    cos_lat = math.cos(math.radians(center_lat))
    if abs(cos_lat) < 0.01:
        raise AreaBoundsError("Area is too close to a pole for kilometer padding.")

    lat_delta = padding_km / KM_PER_DEG_LAT
    lon_delta = padding_km / (KM_PER_DEG_LAT * cos_lat)
    return validate_bounds(
        Bounds(
            north=bounds.north + lat_delta,
            east=bounds.east + lon_delta,
            south=bounds.south - lat_delta,
            west=bounds.west - lon_delta,
        )
    )


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _read_kml_text(path: Path) -> str:
    if not path.exists():
        raise AreaBoundsError(f"Area file does not exist: {path}")
    if path.suffix.lower() != ".kmz":
        return path.read_text(encoding="utf-8")

    with zipfile.ZipFile(path) as zf:
        names = [name for name in zf.namelist() if name.lower().endswith(".kml")]
        if not names:
            raise AreaBoundsError(f"KMZ file has no .kml member: {path}")
        with zf.open(names[0]) as fp:
            return fp.read().decode("utf-8")


def _coordinates_from_kml_text(kml_text: str) -> list[tuple[float, float]]:
    try:
        root = ElementTree.fromstring(kml_text)
    except ElementTree.ParseError as exc:
        raise AreaBoundsError(f"Could not parse KML: {exc}") from exc

    coords: list[tuple[float, float]] = []
    for elem in root.iter():
        name = _local_name(elem.tag)
        text = (elem.text or "").strip()
        if not text:
            continue

        if name == "coordinates":
            for token in text.replace("\n", " ").split():
                parts = token.split(",")
                if len(parts) < 2:
                    continue
                lon = _parse_float(parts[0], "KML longitude")
                lat = _parse_float(parts[1], "KML latitude")
                _validate_lat_lon(lat, lon)
                coords.append((lat, lon))
        elif name == "coord":
            parts = text.split()
            if len(parts) < 2:
                continue
            lon = _parse_float(parts[0], "KML longitude")
            lat = _parse_float(parts[1], "KML latitude")
            _validate_lat_lon(lat, lon)
            coords.append((lat, lon))

    if not coords:
        raise AreaBoundsError("KML contains no coordinates.")
    return coords


def bbox_from_kml(path: str | Path, padding_km: str | float = 0) -> Bounds:
    """Return bbox from all KML/KMZ coordinates, optionally padded in km."""
    padding = _parse_float(str(padding_km), "Padding")
    coords = _coordinates_from_kml_text(_read_kml_text(Path(path)))
    lats = [lat for lat, _lon in coords]
    lons = [lon for _lat, lon in coords]
    raw_bounds = Bounds(
        north=max(lats),
        east=max(lons),
        south=min(lats),
        west=min(lons),
    )
    return expand_bounds(raw_bounds, padding)


def _size_from_args(args: argparse.Namespace) -> float:
    provided = [
        args.size_km is not None,
        args.size_mi is not None,
        args.radius_km is not None,
        args.radius_mi is not None,
    ]
    if sum(provided) != 1:
        raise AreaBoundsError(
            "Provide exactly one of --size-km, --size-mi, --radius-km, or --radius-mi."
        )
    if args.size_km is not None:
        return _parse_float(args.size_km, "Size")
    if args.size_mi is not None:
        return _parse_float(args.size_mi, "Size") * MI_TO_KM
    if args.radius_km is not None:
        return _parse_float(args.radius_km, "Radius") * 2
    return _parse_float(args.radius_mi, "Radius") * MI_TO_KM * 2


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    center_parser = subparsers.add_parser("center")
    center_parser.add_argument("lat")
    center_parser.add_argument("lon")
    center_parser.add_argument("--size-km")
    center_parser.add_argument("--size-mi")
    center_parser.add_argument("--radius-km")
    center_parser.add_argument("--radius-mi")

    file_parser = subparsers.add_parser("file")
    file_parser.add_argument("path")
    file_parser.add_argument("--padding-km", default="0")

    args = parser.parse_args(argv)
    try:
        if args.command == "center":
            bounds = bbox_from_center(args.lat, args.lon, _size_from_args(args))
        elif args.command == "file":
            bounds = bbox_from_kml(args.path, args.padding_km)
        else:
            parser.error(f"Unknown command: {args.command}")
    except AreaBoundsError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print(bounds.as_cli_fields())
    return 0


if __name__ == "__main__":
    sys.exit(main())
