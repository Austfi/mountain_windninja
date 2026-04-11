"""Loads runtime configuration from environment and config/domains.json.

This is the single source of settings for all scripts. It reads
config/runtime.env (via python-dotenv), then resolves paths, loads
the domain catalog, and exposes everything as simple module-level
constants.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent


def _load_env_files() -> None:
    load_dotenv(BASE_DIR / "config" / "runtime.env", override=False)
    load_dotenv(BASE_DIR / ".env", override=False)


def _resolve(raw: str) -> Path:
    p = Path(raw)
    return p if p.is_absolute() else (BASE_DIR / p).resolve()


def _bool(val: str | None, default: bool) -> bool:
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "on"}


# ---------------------------------------------------------------------------
# Domain dataclass
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class DomainConfig:
    key: str
    label: str
    template_path: Path
    elevation_file: Path


def _load_domains(static_data_root: Path) -> tuple[str, dict[str, DomainConfig]]:
    catalog_path = BASE_DIR / "config" / "domains.json"
    with catalog_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    domains: dict[str, DomainConfig] = {}
    for key, raw in payload.get("domains", {}).items():
        tpl = Path(raw["template"])
        if not tpl.is_absolute():
            tpl = (BASE_DIR / tpl).resolve()
        elev = Path(raw["elevation_file"])
        if not elev.is_absolute():
            elev = (static_data_root / elev).resolve()
        domains[key] = DomainConfig(key=key, label=raw.get("label", key),
                                    template_path=tpl, elevation_file=elev)

    if not domains:
        raise ValueError("config/domains.json has no domains.")

    default = (os.getenv("MWN_DOMAIN_ID")
               or payload.get("default_domain")
               or next(iter(domains)))
    if default not in domains:
        raise ValueError(f"Domain '{default}' not in domains.json.")
    return default, domains


# ---------------------------------------------------------------------------
# Load everything once at import time
# ---------------------------------------------------------------------------
_load_env_files()

SCRIPTS_DIR = BASE_DIR / "scripts"
CONFIG_DIR = BASE_DIR / "config"

RUNTIME_DIR = _resolve(os.getenv("MWN_RUNTIME_ROOT", "runtime"))
STATIC_DATA_DIR = _resolve(os.getenv("MWN_STATIC_DATA_ROOT", "static_data"))
TEMP_DIR = RUNTIME_DIR / "temp"
ARCHIVE_DIR = RUNTIME_DIR / "archives"
LOGS_DIR = RUNTIME_DIR / "logs"

WINDNINJA_CLI = os.getenv("MWN_WINDNINJA_CLI",
                          os.getenv("WINDNINJA_CLI", "/usr/local/bin/WindNinja_cli"))
OPENFOAM_BASHRC = os.getenv("MWN_OPENFOAM_BASHRC", "/opt/openfoam9/etc/bashrc")
PYTHON_BIN = os.getenv("MWN_PYTHON_BIN", str(BASE_DIR / ".venv" / "bin" / "python"))
SURFACE_VEGETATION = os.getenv("MWN_SURFACE_VEGETATION", "trees").strip().lower()

GCS_BUCKET = os.getenv("MWN_GCS_BUCKET", os.getenv("GCS_BUCKET", ""))
GCS_UPLOAD_ENABLED = _bool(os.getenv("MWN_GCS_UPLOAD_ENABLED",
                                     os.getenv("GCS_UPLOAD_ENABLED")), default=False)
GCS_PUBLIC_URL_BASE = f"https://storage.googleapis.com/{GCS_BUCKET}" if GCS_BUCKET else ""

DEFAULT_DOMAIN, _DOMAIN_CATALOG = _load_domains(STATIC_DATA_DIR)


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------
def list_domains() -> list[str]:
    return sorted(_DOMAIN_CATALOG)


def get_domain_config(domain_key: str | None = None) -> DomainConfig:
    return _DOMAIN_CATALOG[domain_key or DEFAULT_DOMAIN]


def init_directories() -> None:
    for d in (RUNTIME_DIR, STATIC_DATA_DIR, TEMP_DIR, ARCHIVE_DIR, LOGS_DIR):
        d.mkdir(parents=True, exist_ok=True)
