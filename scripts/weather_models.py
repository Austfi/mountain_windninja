"""Weather model mappings used by WindNinja CLI runs."""
from __future__ import annotations


FORECAST_MODEL_MAP = {
    "HRRR": "NOMADS-HRRR-CONUS-3-KM",
    "NBM": "NOMADS-NBM-CONUS-2.5-KM",
    "NAM": "NOMADS-NAM-NEST-CONUS-3-KM",
    "NAM-CONUS": "NOMADS-NAM-CONUS-12-KM",
    "NAM-ALASKA": "NOMADS-NAM-ALASKA-11.25-KM",
    "RAP": "NOMADS-RAP-CONUS-13-KM",
    "GFS": "NOMADS-GFS-GLOBAL-0.25-DEG",
}

PASTCAST_MODEL_MAP = {
    "HRRR": "PASTCAST-GCP-HRRR-CONUS-3-KM",
}

ALL_MODEL_NAMES = sorted(set(list(FORECAST_MODEL_MAP) + list(PASTCAST_MODEL_MAP)))


def resolve_weather_model(model: str, run_type: str) -> str:
    if run_type == "forecast":
        return FORECAST_MODEL_MAP[model]
    if run_type == "reanalysis":
        if model not in PASTCAST_MODEL_MAP:
            raise ValueError(
                f"Reanalysis only supports: {', '.join(sorted(PASTCAST_MODEL_MAP))}."
            )
        return PASTCAST_MODEL_MAP[model]
    raise ValueError(f"Unsupported run type: {run_type}")
