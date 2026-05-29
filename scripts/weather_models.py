"""Weather model mappings used by WindNinja CLI runs."""
from __future__ import annotations

from dataclasses import dataclass


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

WEATHER_SOURCES = ("native", "herbie")


@dataclass(frozen=True)
class HerbieModelSpec:
    """Herbie model settings plus field search hints for WindNinja handoff."""

    name: str
    model: str
    product: str | None = None
    member: str | int | None = None
    domain: str | None = None
    resolution_km: float = 3.0
    cycle_interval_hours: int = 1
    default_extra: dict[str, str | int | float | bool] | None = None
    field_extra: dict[str, dict[str, str | int | float | bool]] | None = None
    analysis: bool = False
    windninja_notes: str = ""
    search_patterns: dict[str, tuple[str | None, ...]] | None = None
    variable_aliases: dict[str, tuple[str, ...]] | None = None

    def herbie_kwargs(
        self,
        *,
        product: str | None = None,
        member: str | int | None = None,
        domain: str | None = None,
        extra: dict[str, str | int | float | bool] | None = None,
    ) -> dict[str, str | int | float | bool]:
        kwargs: dict[str, str | int | float | bool] = {"model": self.model}
        if self.product:
            kwargs["product"] = product or self.product
        elif product:
            kwargs["product"] = product
        if self.member is not None:
            kwargs["member"] = member if member is not None else self.member
        elif member is not None:
            kwargs["member"] = member
        if self.domain:
            kwargs["domain"] = domain or self.domain
        elif domain:
            kwargs["domain"] = domain
        kwargs.update(self.default_extra or {})
        kwargs.update(extra or {})
        return kwargs


_NCEP_SEARCH_PATTERNS = {
    "u10": (":UGRD:10 m above ground", ":UGRD:10 m"),
    "v10": (":VGRD:10 m above ground", ":VGRD:10 m"),
    "t2m": (":TMP:2 m above ground", ":TMP:2 m"),
    "tcc": (":TCDC:", ":TCDC:entire atmosphere", ":TCDC:surface"),
}

_FULL_FILE_SEARCH_PATTERNS = {
    "u10": (None,),
    "v10": (None,),
    "t2m": (None,),
    "tcc": (None,),
}

_MSC_FIELD_EXTRA = {
    "u10": {"variable": "UGRD", "level": "TGL_10"},
    "v10": {"variable": "VGRD", "level": "TGL_10"},
    "t2m": {"variable": "TMP", "level": "TGL_2"},
    "tcc": {"variable": "TCDC", "level": "EATM"},
}

_COMMON_ALIASES = {
    "u10": ("u10", "u", "ugrd", "10u"),
    "v10": ("v10", "v", "vgrd", "10v"),
    "t2m": ("t2m", "t", "tmp", "2t"),
    "tcc": ("tcc", "tcdc", "total_cloud_cover"),
}

HERBIE_MODEL_MAP = {
    "HRRR": HerbieModelSpec(
        name="HRRR",
        model="hrrr",
        product="sfc",
        resolution_km=3.0,
        search_patterns=_NCEP_SEARCH_PATTERNS,
        variable_aliases=_COMMON_ALIASES,
    ),
    "HRRRAK": HerbieModelSpec(
        name="HRRRAK",
        model="hrrrak",
        product="sfc",
        resolution_km=3.0,
        search_patterns=_NCEP_SEARCH_PATTERNS,
        variable_aliases=_COMMON_ALIASES,
    ),
    "RAP": HerbieModelSpec(
        name="RAP",
        model="rap",
        product="awp130pgrb",
        resolution_km=13.0,
        search_patterns=_NCEP_SEARCH_PATTERNS,
        variable_aliases=_COMMON_ALIASES,
    ),
    "GFS": HerbieModelSpec(
        name="GFS",
        model="gfs",
        product="pgrb2.0p25",
        resolution_km=28.0,
        cycle_interval_hours=6,
        search_patterns=_NCEP_SEARCH_PATTERNS,
        variable_aliases=_COMMON_ALIASES,
    ),
    "GRAPHCAST": HerbieModelSpec(
        name="GRAPHCAST",
        model="graphcast",
        product="pgrb2.0p25",
        resolution_km=28.0,
        cycle_interval_hours=6,
        search_patterns=_NCEP_SEARCH_PATTERNS,
        variable_aliases=_COMMON_ALIASES,
    ),
    "NAM": HerbieModelSpec(
        name="NAM",
        model="nam",
        product="conusnest.hiresf",
        resolution_km=5.0,
        cycle_interval_hours=6,
        search_patterns=_NCEP_SEARCH_PATTERNS,
        variable_aliases=_COMMON_ALIASES,
    ),
    "NAM-CONUS": HerbieModelSpec(
        name="NAM-CONUS",
        model="nam",
        product="awip12",
        resolution_km=12.0,
        cycle_interval_hours=6,
        search_patterns=_NCEP_SEARCH_PATTERNS,
        variable_aliases=_COMMON_ALIASES,
    ),
    "NAM-ALASKA": HerbieModelSpec(
        name="NAM-ALASKA",
        model="nam",
        product="alaskanest.hiresf",
        resolution_km=6.0,
        cycle_interval_hours=6,
        search_patterns=_NCEP_SEARCH_PATTERNS,
        variable_aliases=_COMMON_ALIASES,
    ),
    "NBM": HerbieModelSpec(
        name="NBM",
        model="nbm",
        product="co",
        resolution_km=2.5,
        cycle_interval_hours=1,
        search_patterns=_NCEP_SEARCH_PATTERNS,
        variable_aliases=_COMMON_ALIASES,
    ),
    "RRFS": HerbieModelSpec(
        name="RRFS",
        model="rrfs",
        product="prslev",
        member="control",
        domain="conus",
        resolution_km=3.0,
        cycle_interval_hours=3,
        search_patterns=_NCEP_SEARCH_PATTERNS,
        variable_aliases=_COMMON_ALIASES,
    ),
    "HIRESW": HerbieModelSpec(
        name="HIRESW",
        model="hiresw",
        product="arw_2p5km",
        member=1,
        domain="conus",
        resolution_km=2.5,
        cycle_interval_hours=6,
        search_patterns=_NCEP_SEARCH_PATTERNS,
        variable_aliases=_COMMON_ALIASES,
    ),
    "HREF": HerbieModelSpec(
        name="HREF",
        model="href",
        product="mean",
        domain="conus",
        resolution_km=3.0,
        cycle_interval_hours=6,
        search_patterns=_NCEP_SEARCH_PATTERNS,
        variable_aliases=_COMMON_ALIASES,
    ),
    "HRDPS": HerbieModelSpec(
        name="HRDPS",
        model="hrdps",
        product="continental/2.5km",
        resolution_km=2.5,
        cycle_interval_hours=6,
        field_extra=_MSC_FIELD_EXTRA,
        search_patterns=_FULL_FILE_SEARCH_PATTERNS,
        variable_aliases=_COMMON_ALIASES,
    ),
    "HRDPS-NORTH": HerbieModelSpec(
        name="HRDPS-NORTH",
        model="hrdps_north",
        product="north/grib2",
        resolution_km=2.5,
        cycle_interval_hours=6,
        field_extra=_MSC_FIELD_EXTRA,
        search_patterns=_FULL_FILE_SEARCH_PATTERNS,
        variable_aliases=_COMMON_ALIASES,
    ),
    "RDPS": HerbieModelSpec(
        name="RDPS",
        model="rdps",
        product="10km/grib2/",
        resolution_km=10.0,
        cycle_interval_hours=6,
        field_extra=_MSC_FIELD_EXTRA,
        search_patterns=_FULL_FILE_SEARCH_PATTERNS,
        variable_aliases=_COMMON_ALIASES,
    ),
    "GDPS": HerbieModelSpec(
        name="GDPS",
        model="gdps",
        product="15km/grib2/lat_lon",
        resolution_km=15.0,
        cycle_interval_hours=12,
        field_extra=_MSC_FIELD_EXTRA,
        search_patterns=_FULL_FILE_SEARCH_PATTERNS,
        variable_aliases=_COMMON_ALIASES,
    ),
}

ALL_MODEL_NAMES = sorted(
    set(list(FORECAST_MODEL_MAP) + list(PASTCAST_MODEL_MAP) + list(HERBIE_MODEL_MAP))
)


def normalize_weather_source(weather_source: str) -> str:
    source = weather_source.strip().lower()
    if source not in WEATHER_SOURCES:
        raise ValueError(f"--weather-source must be one of: {', '.join(WEATHER_SOURCES)}.")
    return source


def resolve_herbie_model(model: str) -> HerbieModelSpec:
    model = model.upper()
    if model not in HERBIE_MODEL_MAP:
        raise ValueError(
            f"Herbie weather source only supports: {', '.join(sorted(HERBIE_MODEL_MAP))}."
        )
    return HERBIE_MODEL_MAP[model]


def resolve_weather_model(model: str, run_type: str, weather_source: str = "native") -> str:
    weather_source = normalize_weather_source(weather_source)
    model = model.upper()
    if weather_source == "herbie":
        spec = resolve_herbie_model(model)
        return f"Herbie {spec.name}"

    if run_type == "forecast":
        if model not in FORECAST_MODEL_MAP:
            raise ValueError(
                f"{model} is only available with --weather-source herbie."
            )
        return FORECAST_MODEL_MAP[model]
    if run_type == "reanalysis":
        if model not in PASTCAST_MODEL_MAP:
            raise ValueError(
                f"Reanalysis only supports: {', '.join(sorted(PASTCAST_MODEL_MAP))}."
            )
        return PASTCAST_MODEL_MAP[model]
    raise ValueError(f"Unsupported run type: {run_type}")
