"""Pair WindNinja mass and momentum raster outputs by valid time."""
from __future__ import annotations

import datetime as dt
import re
from dataclasses import dataclass
from pathlib import Path

UTC = dt.timezone.utc

DEFAULT_MOMENTUM_DOMAIN = "berthoud_pass"
DEFAULT_MASS_DOMAIN = "berthoud_pass_mass"
EXCLUDED_RASTER_PREFIXES = ("PASTCAST-", "NOMADS-", "HEIGHT-HRRR")
TIMESTAMP_RE = re.compile(r"(?P<label>(?:\d{2}-\d{2}-\d{4}|\d{8})_\d{4})")


@dataclass(frozen=True)
class RasterPair:
    timestamp: dt.datetime
    speed_path: Path
    direction_path: Path
    run_dir: Path


@dataclass(frozen=True)
class SolverPair:
    timestamp: dt.datetime
    mass: RasterPair
    momentum: RasterPair


def parse_run_label(label: str) -> dt.datetime:
    for fmt in ("%m-%d-%Y_%H%M", "%Y%m%d_%H%M"):
        try:
            return dt.datetime.strptime(label, fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    raise ValueError(f"Unsupported raster timestamp: {label}")


def _timestamp_from_name(path: Path) -> dt.datetime | None:
    base = path.name.removesuffix("_vel.asc").removesuffix("_ang.asc")
    matches = list(TIMESTAMP_RE.finditer(base))
    if not matches:
        return None
    return parse_run_label(matches[-1].group("label"))


def _should_ignore_raster(path: Path) -> bool:
    return path.name.startswith(EXCLUDED_RASTER_PREFIXES)


def collect_windninja_rasters(run_dir: str | Path) -> dict[dt.datetime, RasterPair]:
    run_path = Path(run_dir)
    paired = {}
    for speed_path in sorted(run_path.glob("*_vel.asc")):
        if _should_ignore_raster(speed_path):
            continue
        timestamp = _timestamp_from_name(speed_path)
        if timestamp is None:
            continue
        direction_path = speed_path.with_name(speed_path.name.replace("_vel.asc", "_ang.asc"))
        if not direction_path.exists() or _should_ignore_raster(direction_path):
            continue
        paired[timestamp] = RasterPair(
            timestamp=timestamp,
            speed_path=speed_path,
            direction_path=direction_path,
            run_dir=run_path,
        )
    return paired


def _matches_run_dir(run_dir: Path, domain: str) -> bool:
    pattern = re.compile(
        rf"^{re.escape(domain)}_"
        r"\d{8}_\d{4}_reanalysis_\d+h_[A-Za-z0-9_-]+$"
    )
    return bool(pattern.match(run_dir.name))


def discover_solver_rasters(
    source_root: str | Path,
    *,
    momentum_domain: str = DEFAULT_MOMENTUM_DOMAIN,
    mass_domain: str = DEFAULT_MASS_DOMAIN,
) -> tuple[dict[dt.datetime, RasterPair], dict[dt.datetime, RasterPair]]:
    temp_root = Path(source_root) / "runtime" / "temp"
    if not temp_root.exists():
        raise FileNotFoundError(f"Missing runtime temp directory: {temp_root}")

    momentum: dict[dt.datetime, RasterPair] = {}
    mass: dict[dt.datetime, RasterPair] = {}

    for run_dir in sorted(temp_root.iterdir()):
        if not run_dir.is_dir():
            continue
        if _matches_run_dir(run_dir, mass_domain):
            destination = mass
        elif _matches_run_dir(run_dir, momentum_domain):
            destination = momentum
        else:
            continue
        for timestamp, pair in collect_windninja_rasters(run_dir).items():
            destination.setdefault(timestamp, pair)

    return mass, momentum


def pair_mass_momentum(
    source_root: str | Path,
    *,
    momentum_domain: str = DEFAULT_MOMENTUM_DOMAIN,
    mass_domain: str = DEFAULT_MASS_DOMAIN,
) -> list[SolverPair]:
    mass, momentum = discover_solver_rasters(
        source_root,
        momentum_domain=momentum_domain,
        mass_domain=mass_domain,
    )
    timestamps = sorted(set(mass) & set(momentum))
    return [
        SolverPair(timestamp=timestamp, mass=mass[timestamp], momentum=momentum[timestamp])
        for timestamp in timestamps
    ]


def blocked_day_split(timestamps: list[dt.datetime]) -> dict[dt.date, str]:
    """Assign whole days to train/val/test on a deterministic 10-day cycle."""
    days = sorted({timestamp.date() for timestamp in timestamps})
    split_by_day = {}
    for index, day in enumerate(days):
        remainder = index % 10
        if remainder == 8:
            split = "val"
        elif remainder == 9:
            split = "test"
        else:
            split = "train"
        split_by_day[day] = split
    return split_by_day
