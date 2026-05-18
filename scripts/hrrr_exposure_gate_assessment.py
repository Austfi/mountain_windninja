#!/usr/bin/env python3
"""HRRR-only station test for applying less adjustment at less exposed terrain."""
from __future__ import annotations

import csv
import datetime as dt
import html
import json
import math
import subprocess
from dataclasses import dataclass
from pathlib import Path

try:
    from . import synoptic_validation as sv
except ImportError:
    import synoptic_validation as sv


UTC = dt.timezone.utc
OUT_DIR = Path("runtime/validation/hrrr_exposure_gate")
K0CO_ROOT = Path("runtime/validation/berthoud_pass_k0co_height_hrrr")
BALANCED_ROOT = Path("runtime/validation/berthoud_pass_k0co_height_hrrr_balanced_300m_10_80_cap")
SUMMIT_ROOT = Path("runtime/validation/summit_caic_hrrr_adjusted")
SUMMIT_STAMP = "202601010000_202604010000"
SUMMIT_TARGET_SETTING = "blend_scale_300m_cap_10_80_low_0.75_high_1.10"
BEST_CANDIDATE = "Exposure_TPI_400m_floor_0.00"


@dataclass(frozen=True)
class StationInput:
    station_id: str
    label: str
    lon: float
    lat: float
    comparison_csv: Path
    gmted_grid: Path
    source: str


@dataclass(frozen=True)
class CandidateSetting:
    name: str
    base_weight: float
    boost_threshold_mph: float | None = None
    boost: float = 0.0
    min_base_weight_for_boost: float = 0.0
    directional_weights: dict[str, float] | None = None
    directional_mode: str = "none"


STATIONS = [
    StationInput(
        "K0CO",
        "Berthoud Pass / Mines Peak AWOS",
        -105.76393,
        39.79453,
        BALANCED_ROOT / "hrrr_comparison_samples.csv",
        K0CO_ROOT / "gmted_500m" / "elevation.asc",
        "wide",
    ),
    StationInput(
        "CABTP",
        "Berthoud Pass CAIC",
        -105.78389,
        39.80194,
        K0CO_ROOT / "cabtp_hrrr_adjusted" / "cabtp_hrrr_adjusted_samples.csv",
        K0CO_ROOT / "gmted_500m" / "elevation.asc",
        "wide",
    ),
    StationInput(
        "CABP8",
        "Breckenridge Ski Area Peak 8",
        -106.10255,
        39.47269,
        SUMMIT_ROOT / f"summit_caic_hrrr_adjusted_samples_{SUMMIT_STAMP}.csv",
        SUMMIT_ROOT / "gmted_500m" / "summit_caic_gmted_500m.tif",
        "summit_long",
    ),
    StationInput(
        "CAHSB",
        "Breckenridge Ski Area Horseshoe",
        -106.09150,
        39.47532,
        SUMMIT_ROOT / f"summit_caic_hrrr_adjusted_samples_{SUMMIT_STAMP}.csv",
        SUMMIT_ROOT / "gmted_500m" / "summit_caic_gmted_500m.tif",
        "summit_long",
    ),
]

EXPOSURE_SCALES_M = (150.0, 250.0, 300.0, 400.0, 500.0, 600.0, 800.0)
EXPOSURE_FLOORS = (0.0, 0.25, 0.50, 0.75)
RADIUS_EXPOSURE_RADII_M = (1500.0, 2000.0, 3000.0, 5000.0)
RADIUS_EXPOSURE_SCALES_M = (300.0, 400.0, 500.0, 600.0)
RADIUS_EXPOSURE_GAMMAS = (0.75, 1.0, 1.25)
HYBRID_BOOST_THRESHOLDS_MPH = (8.0, 10.0, 12.0)
HYBRID_BOOSTS = (0.10, 0.20, 0.30)
HYBRID_MIN_BASE_WEIGHTS = (0.25, 0.50)
DIRECTIONAL_EXPOSURE_SCALES_M = (250.0, 400.0, 600.0)
DIRECTIONAL_MODES = ("dir_only", "max_iso_dir", "mean_iso_dir")
SECTORS = ("N", "NE", "E", "SE", "S", "SW", "W", "NW")
SECTOR_ANGLES = {
    "N": 0.0,
    "NE": 45.0,
    "E": 90.0,
    "SE": 135.0,
    "S": 180.0,
    "SW": 225.0,
    "W": 270.0,
    "NW": 315.0,
}
HTML_SETTINGS = (
    "HRRR_10m",
    "Adjusted_HRRR_current",
    BEST_CANDIDATE,
    "RadiusTPI_1500m_scale_400m_gamma_1.00",
    "RadiusTPI_2000m_scale_400m_gamma_1.00",
    "RadiusTPI_5000m_scale_400m_gamma_1.00",
    "DirectionalMax_TPI_400m",
    "DirectionalOnly_TPI_400m",
    "Hybrid_TPI_400m_delta_10mph_boost_0.20_minbase_0.25",
    "Hybrid_TPI_400m_delta_10mph_boost_0.30_minbase_0.25",
    "Exposure_TPI_500m_floor_0.00",
    "Exposure_TPI_500m_floor_0.25",
    "Exposure_TPI_600m_floor_0.00",
)


def obs_to_uv(speed: float, direction_deg: float) -> tuple[float, float]:
    radians = math.radians(direction_deg)
    return -speed * math.sin(radians), -speed * math.cos(radians)


def speed_dir_from_uv(u: float, v: float) -> tuple[float, float]:
    speed = math.hypot(u, v)
    direction = (270.0 - math.degrees(math.atan2(v, u))) % 360.0
    return speed, direction


def circular_abs_error(model_dir: float, obs_dir: float) -> float:
    return abs(((model_dir - obs_dir + 180.0) % 360.0) - 180.0)


def angle_abs_diff(a: float, b: float) -> float:
    return abs(((a - b + 180.0) % 360.0) - 180.0)


def lonlat_to_utm13n(lon_deg: float, lat_deg: float) -> tuple[float, float]:
    """Convert WGS84 lon/lat to UTM 13N meters without adding a dependency."""
    semi_major = 6378137.0
    flattening = 1.0 / 298.257223563
    scale = 0.9996
    eccentricity_sq = flattening * (2.0 - flattening)
    second_ecc_sq = eccentricity_sq / (1.0 - eccentricity_sq)
    lat = math.radians(lat_deg)
    lon = math.radians(lon_deg)
    central_lon = math.radians(-105.0)
    sin_lat = math.sin(lat)
    cos_lat = math.cos(lat)
    n = semi_major / math.sqrt(1.0 - eccentricity_sq * sin_lat * sin_lat)
    tan_sq = math.tan(lat) ** 2
    c = second_ecc_sq * cos_lat * cos_lat
    a = cos_lat * (lon - central_lon)
    meridian = semi_major * (
        (1.0 - eccentricity_sq / 4.0 - 3.0 * eccentricity_sq**2 / 64.0 - 5.0 * eccentricity_sq**3 / 256.0) * lat
        - (3.0 * eccentricity_sq / 8.0 + 3.0 * eccentricity_sq**2 / 32.0 + 45.0 * eccentricity_sq**3 / 1024.0)
        * math.sin(2.0 * lat)
        + (15.0 * eccentricity_sq**2 / 256.0 + 45.0 * eccentricity_sq**3 / 1024.0) * math.sin(4.0 * lat)
        - (35.0 * eccentricity_sq**3 / 3072.0) * math.sin(6.0 * lat)
    )
    easting = scale * n * (
        a
        + (1.0 - tan_sq + c) * a**3 / 6.0
        + (5.0 - 18.0 * tan_sq + tan_sq**2 + 72.0 * c - 58.0 * second_ecc_sq) * a**5 / 120.0
    ) + 500000.0
    northing = scale * (
        meridian
        + n
        * math.tan(lat)
        * (
            a**2 / 2.0
            + (5.0 - tan_sq + 9.0 * c + 4.0 * c**2) * a**4 / 24.0
            + (61.0 - 58.0 * tan_sq + tan_sq**2 + 600.0 * c - 330.0 * second_ecc_sq) * a**6 / 720.0
        )
    )
    return easting, northing


def load_xyz(path: Path) -> list[tuple[float, float, float]]:
    result = subprocess.run(
        ["gdal_translate", "-q", "-of", "XYZ", str(path), "/vsistdout/"],
        text=True,
        capture_output=True,
        check=True,
    )
    points = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        x, y, z = map(float, parts[:3])
        if z > -9990.0:
            points.append((x, y, z))
    return points


def terrain_exposure(
    station: StationInput,
    *,
    radius_m: float = 3000.0,
    inner_skip_m: float = 500.0,
) -> dict:
    x0, y0 = lonlat_to_utm13n(station.lon, station.lat)
    points = load_xyz(station.gmted_grid)
    center = min(points, key=lambda point: math.hypot(point[0] - x0, point[1] - y0))[2]
    radius_values = {}
    for test_radius_m in RADIUS_EXPOSURE_RADII_M:
        surrounding = [
            z
            for x, y, z in points
            if inner_skip_m < math.hypot(x - x0, y - y0) <= test_radius_m
        ]
        if not surrounding:
            raise RuntimeError(f"No surrounding GMTED cells for {station.station_id} at radius {test_radius_m}")
        surrounding_mean = sv.mean(surrounding)
        radius_values[int(test_radius_m)] = {
            "surrounding_mean_m": surrounding_mean,
            "tpi_m": center - surrounding_mean,
            "surrounding_cell_count": len(surrounding),
        }
    default_radius = int(radius_m)
    tpi = radius_values[default_radius]["tpi_m"]
    sector_tpi = {}
    sector_counts = {}
    for sector, center_angle in SECTOR_ANGLES.items():
        sector_values = []
        for x, y, z in points:
            distance = math.hypot(x - x0, y - y0)
            if not inner_skip_m < distance <= radius_m:
                continue
            bearing = (math.degrees(math.atan2(x - x0, y - y0)) + 360.0) % 360.0
            if angle_abs_diff(bearing, center_angle) <= 45.0:
                sector_values.append(z)
        if sector_values:
            sector_tpi[sector] = center - sv.mean(sector_values)
            sector_counts[sector] = len(sector_values)
        else:
            sector_tpi[sector] = 0.0
            sector_counts[sector] = 0
    return {
        "station_id": station.station_id,
        "label": station.label,
        "gmted_elevation_m": center,
        "surrounding_mean_m": radius_values[default_radius]["surrounding_mean_m"],
        "tpi_m": tpi,
        "radius_m": radius_m,
        "surrounding_cell_count": radius_values[default_radius]["surrounding_cell_count"],
        **{
            f"surrounding_mean_r{radius}m_m": values["surrounding_mean_m"]
            for radius, values in radius_values.items()
        },
        **{
            f"tpi_r{radius}m_m": values["tpi_m"]
            for radius, values in radius_values.items()
        },
        **{
            f"surrounding_cell_count_r{radius}m": values["surrounding_cell_count"]
            for radius, values in radius_values.items()
        },
        **{
            f"weight_tpi_{int(scale)}m": max(0.0, min(tpi / scale, 1.0))
            for scale in EXPOSURE_SCALES_M
        },
        **{
            f"sector_tpi_{sector}_m": sector_tpi[sector]
            for sector in SECTORS
        },
        **{
            f"sector_count_{sector}": sector_counts[sector]
            for sector in SECTORS
        },
    }


def wide_records(station: StationInput) -> list[dict]:
    rows = []
    with station.comparison_csv.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("station_id") != station.station_id:
                continue
            rows.append({
                "station_id": station.station_id,
                "sample_time_utc": row["sample_time_utc"],
                "obs_time_utc": row["obs_time_utc"],
                "observed_speed": float(row["observed_speed"]),
                "observed_dir_deg": float(row["observed_dir_deg"]),
                "raw_speed": float(row["hrrr_speed"]),
                "raw_dir_deg": float(row["hrrr_dir_deg"]),
                "adjusted_speed": float(row["adjusted_hrrr_speed"]),
                "adjusted_dir_deg": float(row["adjusted_hrrr_dir_deg"]),
            })
    return rows


def summit_long_records(station: StationInput) -> list[dict]:
    by_time: dict[str, dict[str, dict]] = {}
    with station.comparison_csv.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("station_id") != station.station_id:
                continue
            setting = row["setting"]
            if setting not in {"HRRR_10m", SUMMIT_TARGET_SETTING}:
                continue
            by_time.setdefault(row["sample_time_utc"], {})[setting] = row

    rows = []
    for sample_time, grouped in sorted(by_time.items()):
        raw = grouped.get("HRRR_10m")
        adjusted = grouped.get(SUMMIT_TARGET_SETTING)
        if raw is None or adjusted is None:
            continue
        rows.append({
            "station_id": station.station_id,
            "sample_time_utc": sample_time,
            "obs_time_utc": raw["obs_time_utc"],
            "observed_speed": float(raw["observed_speed"]),
            "observed_dir_deg": float(raw["observed_dir_deg"]),
            "raw_speed": float(raw["speed_mph"]),
            "raw_dir_deg": float(raw["direction_deg"]),
            "adjusted_speed": float(adjusted["speed_mph"]),
            "adjusted_dir_deg": float(adjusted["direction_deg"]),
        })
    return rows


def load_records(station: StationInput) -> list[dict]:
    if not station.comparison_csv.exists():
        raise FileNotFoundError(station.comparison_csv)
    if station.source == "wide":
        return wide_records(station)
    if station.source == "summit_long":
        return summit_long_records(station)
    raise ValueError(f"Unknown source {station.source}")


def setting_weight(setting: CandidateSetting, record: dict) -> float:
    weight = setting.base_weight
    if setting.directional_weights is not None:
        sector, _sector_order = direction_sector(float(record["raw_dir_deg"]))
        directional_weight = setting.directional_weights[sector]
        if setting.directional_mode == "dir_only":
            weight = directional_weight
        elif setting.directional_mode == "max_iso_dir":
            weight = max(setting.base_weight, directional_weight)
        elif setting.directional_mode == "mean_iso_dir":
            weight = 0.5 * (setting.base_weight + directional_weight)
        else:
            raise ValueError(f"Unknown directional mode {setting.directional_mode}")
    if setting.boost_threshold_mph is None:
        return weight
    adjustment_delta = float(record["adjusted_speed"]) - float(record["raw_speed"])
    if setting.base_weight >= setting.min_base_weight_for_boost and adjustment_delta >= setting.boost_threshold_mph:
        weight = min(1.0, weight + setting.boost)
    return weight


def evaluate_record(record: dict, setting: CandidateSetting) -> dict:
    obs_speed = record["observed_speed"]
    obs_dir = record["observed_dir_deg"]
    obs_u, obs_v = obs_to_uv(obs_speed, obs_dir)
    raw_u, raw_v = obs_to_uv(record["raw_speed"], record["raw_dir_deg"])
    adjusted_u, adjusted_v = obs_to_uv(record["adjusted_speed"], record["adjusted_dir_deg"])
    adjustment_weight = setting_weight(setting, record)
    u = raw_u + adjustment_weight * (adjusted_u - raw_u)
    v = raw_v + adjustment_weight * (adjusted_v - raw_v)
    speed, direction = speed_dir_from_uv(u, v)
    return {
        "station_id": record["station_id"],
        "sample_time_utc": record["sample_time_utc"],
        "obs_time_utc": record["obs_time_utc"],
        "setting": setting.name,
        "observed_speed": obs_speed,
        "observed_dir_deg": obs_dir,
        "raw_hrrr_speed_mph": record["raw_speed"],
        "raw_hrrr_dir_deg": record["raw_dir_deg"],
        "current_adjusted_speed_mph": record["adjusted_speed"],
        "current_adjusted_minus_raw_mph": record["adjusted_speed"] - record["raw_speed"],
        "speed_mph": speed,
        "direction_deg": direction,
        "speed_error_mph": speed - obs_speed,
        "dir_abs_error_deg": circular_abs_error(direction, obs_dir),
        "vector_error_mph": math.hypot(u - obs_u, v - obs_v),
        "adjustment_weight": adjustment_weight,
    }


def station_settings(exposure: dict) -> list[CandidateSetting]:
    settings = [
        CandidateSetting("HRRR_10m", 0.0),
        CandidateSetting("Adjusted_HRRR_current", 1.0),
    ]
    for scale in EXPOSURE_SCALES_M:
        base_weight = float(exposure[f"weight_tpi_{int(scale)}m"])
        for floor in EXPOSURE_FLOORS:
            weight = floor + (1.0 - floor) * base_weight
            settings.append(CandidateSetting(f"Exposure_TPI_{int(scale)}m_floor_{floor:.2f}", weight))
    for radius in RADIUS_EXPOSURE_RADII_M:
        tpi = float(exposure[f"tpi_r{int(radius)}m_m"])
        for scale in RADIUS_EXPOSURE_SCALES_M:
            linear_weight = max(0.0, min(tpi / scale, 1.0))
            for gamma in RADIUS_EXPOSURE_GAMMAS:
                weight = linear_weight ** gamma
                settings.append(
                    CandidateSetting(
                        f"RadiusTPI_{int(radius)}m_scale_{int(scale)}m_gamma_{gamma:.2f}",
                        weight,
                    )
                )
    base_400m_weight = float(exposure["weight_tpi_400m"])
    for scale in DIRECTIONAL_EXPOSURE_SCALES_M:
        directional_weights = {
            sector: max(0.0, min(float(exposure[f"sector_tpi_{sector}_m"]) / scale, 1.0))
            for sector in SECTORS
        }
        for mode in DIRECTIONAL_MODES:
            if mode == "dir_only":
                name = f"DirectionalOnly_TPI_{int(scale)}m"
            elif mode == "max_iso_dir":
                name = f"DirectionalMax_TPI_{int(scale)}m"
            elif mode == "mean_iso_dir":
                name = f"DirectionalMean_TPI_{int(scale)}m"
            else:
                raise ValueError(mode)
            settings.append(
                CandidateSetting(
                    name,
                    base_400m_weight,
                    directional_weights=directional_weights,
                    directional_mode=mode,
                )
            )
    for threshold in HYBRID_BOOST_THRESHOLDS_MPH:
        for boost in HYBRID_BOOSTS:
            for min_base_weight in HYBRID_MIN_BASE_WEIGHTS:
                settings.append(
                    CandidateSetting(
                        (
                            f"Hybrid_TPI_400m_delta_{int(threshold)}mph"
                            f"_boost_{boost:.2f}_minbase_{min_base_weight:.2f}"
                        ),
                        base_400m_weight,
                        boost_threshold_mph=threshold,
                        boost=boost,
                        min_base_weight_for_boost=min_base_weight,
                    )
                )
    return settings


def summarize(station_id: str, setting: str, values: list[dict], baseline: dict | None) -> dict:
    speed_errors = [row["speed_error_mph"] for row in values]
    vector_errors = [row["vector_error_mph"] for row in values]
    speed_mae = sv.mean([abs(value) for value in speed_errors])
    vector_rmse = sv.rmse(vector_errors)
    return {
        "station_id": station_id,
        "setting": setting,
        "sample_count": len(values),
        "speed_mae_mph": speed_mae,
        "speed_bias_mph": sv.mean(speed_errors),
        "speed_rmse_mph": sv.rmse(speed_errors),
        "direction_mae_deg": sv.mean([row["dir_abs_error_deg"] for row in values]),
        "vector_rmse_mph": vector_rmse,
        "adjustment_weight": sv.mean([row["adjustment_weight"] for row in values]),
        "speed_mae_improvement_mph": None if baseline is None else baseline["speed_mae_mph"] - speed_mae,
        "vector_rmse_improvement_mph": None if baseline is None else baseline["vector_rmse_mph"] - vector_rmse,
    }


def evaluate_station(station: StationInput) -> tuple[list[dict], list[dict], dict]:
    exposure = terrain_exposure(station)
    records = load_records(station)
    if not records:
        raise RuntimeError(f"No comparison records for {station.station_id}")

    grouped: dict[str, list[dict]] = {}
    for setting in station_settings(exposure):
        grouped[setting.name] = [evaluate_record(record, setting) for record in records]

    baseline = summarize(station.station_id, "HRRR_10m", grouped["HRRR_10m"], None)
    metrics = [baseline]
    samples = []
    for setting, values in grouped.items():
        if setting != "HRRR_10m":
            metrics.append(summarize(station.station_id, setting, values, baseline))
        samples.extend(values)
    return metrics, samples, exposure


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                key: round(value, 6) if isinstance(value, float) else value
                for key, value in row.items()
            })


def aggregate_tuning(metrics: list[dict]) -> list[dict]:
    by_station = {
        row["station_id"]: row
        for row in metrics
        if row["setting"] == "HRRR_10m"
    }
    by_setting: dict[str, list[dict]] = {}
    for row in metrics:
        by_setting.setdefault(row["setting"], []).append(row)

    rows = []
    for setting, setting_rows in by_setting.items():
        station_count = len(setting_rows)
        speed_mae = sv.mean([float(row["speed_mae_mph"]) for row in setting_rows])
        vector_rmse = sv.mean([float(row["vector_rmse_mph"]) for row in setting_rows])
        abs_bias = sv.mean([abs(float(row["speed_bias_mph"])) for row in setting_rows])
        direction_mae = sv.mean([float(row["direction_mae_deg"]) for row in setting_rows])
        improvements = []
        vector_improvements = []
        for row in setting_rows:
            baseline = by_station[row["station_id"]]
            improvements.append(float(baseline["speed_mae_mph"]) - float(row["speed_mae_mph"]))
            vector_improvements.append(float(baseline["vector_rmse_mph"]) - float(row["vector_rmse_mph"]))
        max_degradation = max([max(-value, 0.0) for value in improvements])
        vector_max_degradation = max([max(-value, 0.0) for value in vector_improvements])
        speed_improved_count = sum(1 for value in improvements if value > 0.0)
        vector_improved_count = sum(1 for value in vector_improvements if value > 0.0)
        rows.append({
            "setting": setting,
            "station_count": station_count,
            "station_mean_speed_mae_mph": speed_mae,
            "station_mean_abs_bias_mph": abs_bias,
            "station_mean_direction_mae_deg": direction_mae,
            "station_mean_vector_rmse_mph": vector_rmse,
            "speed_improved_station_count": speed_improved_count,
            "vector_improved_station_count": vector_improved_count,
            "max_speed_mae_degradation_mph": max_degradation,
            "max_vector_rmse_degradation_mph": vector_max_degradation,
            "mean_adjustment_weight": sv.mean([float(row["adjustment_weight"]) for row in setting_rows]),
            "k0co_speed_mae_mph": next(
                float(row["speed_mae_mph"]) for row in setting_rows if row["station_id"] == "K0CO"
            ),
            "cabtp_speed_mae_mph": next(
                float(row["speed_mae_mph"]) for row in setting_rows if row["station_id"] == "CABTP"
            ),
            "cabp8_speed_mae_mph": next(
                float(row["speed_mae_mph"]) for row in setting_rows if row["station_id"] == "CABP8"
            ),
            "cahsb_speed_mae_mph": next(
                float(row["speed_mae_mph"]) for row in setting_rows if row["station_id"] == "CAHSB"
            ),
        })
    rows.sort(
        key=lambda row: (
            row["max_speed_mae_degradation_mph"] > 0.10,
            row["station_mean_speed_mae_mph"],
            row["station_mean_vector_rmse_mph"],
        )
    )
    return rows


def observed_speed_bin(speed_mph: float) -> tuple[str, int]:
    bins = [
        (0.0, 5.0, "00-05 mph"),
        (5.0, 10.0, "05-10 mph"),
        (10.0, 15.0, "10-15 mph"),
        (15.0, 20.0, "15-20 mph"),
        (20.0, 30.0, "20-30 mph"),
    ]
    for index, (low, high, label) in enumerate(bins):
        if low <= speed_mph < high:
            return label, index
    return "30+ mph", len(bins)


def adjustment_delta_bin(delta_mph: float) -> tuple[str, int]:
    bins = [
        (-999.0, -5.0, "< -5 mph"),
        (-5.0, -2.0, "-5 to -2 mph"),
        (-2.0, 0.0, "-2 to 0 mph"),
        (0.0, 2.0, "0 to 2 mph"),
        (2.0, 5.0, "2 to 5 mph"),
        (5.0, 10.0, "5 to 10 mph"),
    ]
    for index, (low, high, label) in enumerate(bins):
        if low <= delta_mph < high:
            return label, index
    return "10+ mph", len(bins)


def direction_sector(direction_deg: float) -> tuple[str, int]:
    index = int(((direction_deg % 360.0) + 22.5) // 45.0) % 8
    return SECTORS[index], index


def sample_bins(row: dict) -> list[tuple[str, str, int]]:
    speed_label, speed_order = observed_speed_bin(float(row["observed_speed"]))
    raw_speed_label, raw_speed_order = observed_speed_bin(float(row["raw_hrrr_speed_mph"]))
    delta_label, delta_order = adjustment_delta_bin(float(row["current_adjusted_minus_raw_mph"]))
    sector_label, sector_order = direction_sector(float(row["observed_dir_deg"]))
    raw_sector_label, raw_sector_order = direction_sector(float(row["raw_hrrr_dir_deg"]))
    sample_time = sv.parse_iso_time(row["sample_time_utc"])
    return [
        ("observed_speed", speed_label, speed_order),
        ("raw_hrrr_speed", raw_speed_label, raw_speed_order),
        ("current_adjustment_delta", delta_label, delta_order),
        ("observed_direction", sector_label, sector_order),
        ("raw_hrrr_direction", raw_sector_label, raw_sector_order),
        ("utc_hour", f"{sample_time.hour:02d}Z", sample_time.hour),
        ("month", sample_time.strftime("%Y-%m"), sample_time.month),
    ]


def summarize_values(station_id: str, setting: str, values: list[dict]) -> dict:
    speed_errors = [float(row["speed_error_mph"]) for row in values]
    vector_errors = [float(row["vector_error_mph"]) for row in values]
    return {
        "station_id": station_id,
        "setting": setting,
        "sample_count": len(values),
        "speed_mae_mph": sv.mean([abs(value) for value in speed_errors]),
        "speed_bias_mph": sv.mean(speed_errors),
        "speed_rmse_mph": sv.rmse(speed_errors),
        "direction_mae_deg": sv.mean([float(row["dir_abs_error_deg"]) for row in values]),
        "vector_rmse_mph": sv.rmse(vector_errors),
        "adjustment_weight": sv.mean([float(row["adjustment_weight"]) for row in values]),
    }


def binned_diagnostics(samples: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str, str, str], dict] = {}
    for row in samples:
        for bin_type, bin_label, bin_order in sample_bins(row):
            key = (row["station_id"], row["setting"], bin_type, bin_label)
            bucket = grouped.setdefault(
                key,
                {"bin_order": bin_order, "rows": []},
            )
            bucket["rows"].append(row)

    summaries = []
    for (station_id, setting, bin_type, bin_label), bucket in grouped.items():
        summary = summarize_values(station_id, setting, bucket["rows"])
        summary.update({
            "bin_type": bin_type,
            "bin_label": bin_label,
            "bin_order": bucket["bin_order"],
        })
        summaries.append(summary)

    raw_by_bin = {
        (row["station_id"], row["bin_type"], row["bin_label"]): row
        for row in summaries
        if row["setting"] == "HRRR_10m"
    }
    adjusted_by_bin = {
        (row["station_id"], row["bin_type"], row["bin_label"]): row
        for row in summaries
        if row["setting"] == "Adjusted_HRRR_current"
    }
    for row in summaries:
        key = (row["station_id"], row["bin_type"], row["bin_label"])
        raw = raw_by_bin.get(key)
        adjusted = adjusted_by_bin.get(key)
        row["speed_mae_improvement_vs_hrrr_mph"] = (
            None if raw is None else raw["speed_mae_mph"] - row["speed_mae_mph"]
        )
        row["speed_mae_improvement_vs_current_adjusted_mph"] = (
            None if adjusted is None else adjusted["speed_mae_mph"] - row["speed_mae_mph"]
        )
        row["vector_rmse_improvement_vs_hrrr_mph"] = (
            None if raw is None else raw["vector_rmse_mph"] - row["vector_rmse_mph"]
        )
        row["vector_rmse_improvement_vs_current_adjusted_mph"] = (
            None if adjusted is None else adjusted["vector_rmse_mph"] - row["vector_rmse_mph"]
        )
    summaries.sort(key=lambda row: (row["station_id"], row["bin_type"], row["bin_order"], row["setting"]))
    return summaries


def candidate_bin_comparison(binned_rows: list[dict], candidate_setting: str) -> list[dict]:
    by_key = {
        (row["station_id"], row["setting"], row["bin_type"], row["bin_label"]): row
        for row in binned_rows
    }
    keys = sorted({
        (row["station_id"], row["bin_type"], row["bin_label"])
        for row in binned_rows
    })
    rows = []
    for station_id, bin_type, bin_label in keys:
        raw = by_key.get((station_id, "HRRR_10m", bin_type, bin_label))
        adjusted = by_key.get((station_id, "Adjusted_HRRR_current", bin_type, bin_label))
        candidate = by_key.get((station_id, candidate_setting, bin_type, bin_label))
        if raw is None or adjusted is None or candidate is None:
            continue
        rows.append({
            "station_id": station_id,
            "bin_type": bin_type,
            "bin_label": bin_label,
            "sample_count": candidate["sample_count"],
            "hrrr_speed_mae_mph": raw["speed_mae_mph"],
            "current_adjusted_speed_mae_mph": adjusted["speed_mae_mph"],
            "candidate_speed_mae_mph": candidate["speed_mae_mph"],
            "candidate_vs_hrrr_mph": raw["speed_mae_mph"] - candidate["speed_mae_mph"],
            "candidate_vs_current_adjusted_mph": adjusted["speed_mae_mph"] - candidate["speed_mae_mph"],
            "hrrr_vector_rmse_mph": raw["vector_rmse_mph"],
            "current_adjusted_vector_rmse_mph": adjusted["vector_rmse_mph"],
            "candidate_vector_rmse_mph": candidate["vector_rmse_mph"],
            "candidate_vector_vs_current_adjusted_mph": adjusted["vector_rmse_mph"] - candidate["vector_rmse_mph"],
            "candidate_adjustment_weight": candidate["adjustment_weight"],
        })
    rows.sort(key=lambda row: (row["station_id"], row["bin_type"], row["bin_label"]))
    return rows


def format_value(value: object, digits: int = 2) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, str):
        return html.escape(value)
    return f"{float(value):.{digits}f}"


def comparison_table(rows: list[dict]) -> str:
    fields = [
        "station_id",
        "bin_type",
        "bin_label",
        "sample_count",
        "hrrr_speed_mae_mph",
        "current_adjusted_speed_mae_mph",
        "candidate_speed_mae_mph",
        "candidate_vs_current_adjusted_mph",
        "candidate_vs_hrrr_mph",
        "candidate_adjustment_weight",
    ]
    body = []
    for row in rows:
        body.append(
            "<tr>"
            + "".join(
                f"<td>{html.escape(str(row[field]))}</td>"
                if field in {"station_id", "bin_type", "bin_label", "sample_count"}
                else f"<td>{format_value(row[field])}</td>"
                for field in fields
            )
            + "</tr>"
        )
    return (
        f"<table><thead><tr>{''.join(f'<th>{html.escape(field)}</th>' for field in fields)}</tr></thead>"
        f"<tbody>{''.join(body)}</tbody></table>"
    )


def write_html(
    path: Path,
    metrics: list[dict],
    exposures: list[dict],
    tuning_rows: list[dict],
    comparison_rows: list[dict],
    best_candidate: str,
) -> None:
    by_station: dict[str, list[dict]] = {}
    exposure_by_station = {row["station_id"]: row for row in exposures}
    for row in metrics:
        by_station.setdefault(row["station_id"], []).append(row)
    fields = [
        "setting",
        "sample_count",
        "speed_mae_mph",
        "speed_bias_mph",
        "direction_mae_deg",
        "vector_rmse_mph",
        "adjustment_weight",
        "speed_mae_improvement_mph",
    ]
    sections = []
    tuning_fields = [
        "setting",
        "station_mean_speed_mae_mph",
        "station_mean_abs_bias_mph",
        "station_mean_vector_rmse_mph",
        "speed_improved_station_count",
        "max_speed_mae_degradation_mph",
        "k0co_speed_mae_mph",
        "cabtp_speed_mae_mph",
        "cabp8_speed_mae_mph",
        "cahsb_speed_mae_mph",
    ]
    tuning_body = []
    for row in tuning_rows[:12]:
        tuning_body.append(
            "<tr>"
            + "".join(
                f"<td>{html.escape(str(row[field]))}</td>"
                if field in {"setting", "speed_improved_station_count"}
                else f"<td>{format_value(row[field])}</td>"
                for field in tuning_fields
            )
            + "</tr>"
        )
    comparison_filtered = [row for row in comparison_rows if int(row["sample_count"]) >= 40]
    candidate_wins = sorted(
        comparison_filtered,
        key=lambda row: row["candidate_vs_current_adjusted_mph"],
        reverse=True,
    )[:14]
    candidate_losses = sorted(
        comparison_filtered,
        key=lambda row: row["candidate_vs_current_adjusted_mph"],
    )[:14]
    candidate_vs_raw_losses = sorted(
        comparison_filtered,
        key=lambda row: row["candidate_vs_hrrr_mph"],
    )[:10]
    for station_id, rows in by_station.items():
        exposure = exposure_by_station[station_id]
        body = []
        visible_settings = set(HTML_SETTINGS)
        visible_settings.add(best_candidate)
        visible_rows = [row for row in rows if row["setting"] in visible_settings]
        for row in sorted(visible_rows, key=lambda item: item["speed_mae_mph"]):
            body.append(
                "<tr>"
                + "".join(
                    f"<td>{html.escape(str(row[field]))}</td>"
                    if field in {"setting", "sample_count"}
                    else f"<td>{format_value(row[field])}</td>"
                    for field in fields
                )
                + "</tr>"
            )
        weight_text = ", ".join(
            f"TPI/{int(scale)}={float(exposure[f'weight_tpi_{int(scale)}m']):.2f}"
            for scale in EXPOSURE_SCALES_M
        )
        sections.append(
            f"""
<h2>{html.escape(station_id)} / {html.escape(exposure["label"])}</h2>
<p class="note">
  GMTED {float(exposure["gmted_elevation_m"]):.1f} m; 3 km TPI
  {float(exposure["tpi_m"]):.1f} m; exposure weights: {html.escape(weight_text)}.
</p>
<table>
  <thead><tr>{''.join(f'<th>{html.escape(field)}</th>' for field in fields)}</tr></thead>
  <tbody>{''.join(body)}</tbody>
</table>
"""
        )
    path.write_text(
        f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>HRRR Exposure Gate Assessment</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #111827; }}
    .wrap {{ max-width: 1180px; }}
    .note {{ color: #4b5563; line-height: 1.45; }}
    table {{ border-collapse: collapse; width: 100%; margin: 16px 0 28px; font-size: 13px; }}
    th, td {{ border: 1px solid #d1d5db; padding: 6px 8px; text-align: right; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ background: #f3f4f6; }}
  </style>
</head>
<body>
<div class="wrap">
  <h1>HRRR Exposure Gate Assessment</h1>
  <p class="note">
    HRRR-only point validation for K0CO, CABTP, CABP8, and CAHSB. The exposure
    variants start with raw HRRR 10 m, then apply only a fraction of the current
    adjusted-HRRR change. That fraction is based on station TPI from the 500 m
    GMTED grid inside a 3 km radius. Floor variants use
    floor + (1 - floor) * clamp(TPI / scale, 0, 1).
  </p>
  <h2>Top Tuning Candidates</h2>
  <table>
    <thead><tr>{''.join(f'<th>{html.escape(field)}</th>' for field in tuning_fields)}</tr></thead>
    <tbody>{''.join(tuning_body)}</tbody>
  </table>
  <h2>{html.escape(best_candidate)} Bin Wins vs Current Adjusted HRRR</h2>
  <p class="note">Positive candidate_vs_current_adjusted_mph means the exposure candidate has lower speed MAE than the full current adjustment in that bin.</p>
  {comparison_table(candidate_wins)}
  <h2>{html.escape(best_candidate)} Bin Losses vs Current Adjusted HRRR</h2>
  {comparison_table(candidate_losses)}
  <h2>{html.escape(best_candidate)} Weakest Bins vs Raw HRRR</h2>
  <p class="note">Negative candidate_vs_hrrr_mph means the exposure candidate is worse than raw HRRR in that bin.</p>
  {comparison_table(candidate_vs_raw_losses)}
  {''.join(sections)}
</div>
</body>
</html>
""",
        encoding="utf-8",
    )


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    metrics: list[dict] = []
    samples: list[dict] = []
    exposures: list[dict] = []
    for station in STATIONS:
        station_metrics, station_samples, station_exposure = evaluate_station(station)
        metrics.extend(station_metrics)
        samples.extend(station_samples)
        exposures.append(station_exposure)
    tuning_rows = aggregate_tuning(metrics)
    binned_rows = binned_diagnostics(samples)
    best_candidate = str(tuning_rows[0]["setting"])
    comparison_rows = candidate_bin_comparison(binned_rows, best_candidate)
    summary = {
        "generated_at_utc": dt.datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "method": "candidate = HRRR_10m + exposure_weight * (Adjusted_HRRR_current - HRRR_10m)",
        "exposure_weights": "exposure_weight = floor + (1 - floor) * clamp(TPI_3km / scale_m, 0, 1)",
        "best_candidate": best_candidate,
        "target_adjusted_hrrr": {
            "k0co_cabtp": "existing wide HRRR vs adjusted-HRRR point sample CSVs",
            "summit": SUMMIT_TARGET_SETTING,
        },
        "tuning": tuning_rows,
        "candidate_bin_comparison": comparison_rows,
        "metrics": metrics,
        "exposures": exposures,
    }
    metrics_csv = OUT_DIR / "exposure_gate_metrics.csv"
    tuning_csv = OUT_DIR / "exposure_gate_tuning.csv"
    bins_csv = OUT_DIR / "exposure_gate_bins.csv"
    comparison_csv = OUT_DIR / "exposure_gate_bin_comparison.csv"
    samples_csv = OUT_DIR / "exposure_gate_samples.csv"
    exposures_csv = OUT_DIR / "exposure_gate_terrain.csv"
    summary_json = OUT_DIR / "exposure_gate_summary.json"
    html_path = OUT_DIR / "exposure_gate.html"
    write_csv(metrics_csv, metrics)
    write_csv(tuning_csv, tuning_rows)
    write_csv(bins_csv, binned_rows)
    write_csv(comparison_csv, comparison_rows)
    write_csv(samples_csv, samples)
    write_csv(exposures_csv, exposures)
    summary_json.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    write_html(html_path, metrics, exposures, tuning_rows, comparison_rows, best_candidate)
    print(json.dumps({
        "metrics_csv": str(metrics_csv),
        "tuning_csv": str(tuning_csv),
        "bins_csv": str(bins_csv),
        "comparison_csv": str(comparison_csv),
        "samples_csv": str(samples_csv),
        "exposures_csv": str(exposures_csv),
        "summary_json": str(summary_json),
        "html": str(html_path),
        "top_tuning": tuning_rows[:10],
        "exposures": exposures,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
