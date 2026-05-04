#!/usr/bin/env python3
"""Helpers for validating terrain bounds and registering domains."""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = BASE_DIR / "config"
DOMAINS_PATH = CONFIG_DIR / "domains.json"
RUNTIME_ENV_PATH = CONFIG_DIR / "runtime.env"
DEFAULT_TEMPLATE = "config/template.cfg"
DOMAIN_KEY_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]*$")
DEM_SOURCES = {"us", "srtm", "gmted"}
TERRAIN_SOURCES = DEM_SOURCES | {"lcp"}


class DomainRegistryError(ValueError):
    """Raised for user-fixable domain registration problems."""


def _read_runtime_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _static_data_root(base_dir: Path = BASE_DIR, runtime_env_path: Path = RUNTIME_ENV_PATH) -> Path:
    env = _read_runtime_env(runtime_env_path)
    raw = env.get("MWN_STATIC_DATA_ROOT", "static_data")
    path = Path(raw)
    return path if path.is_absolute() else (base_dir / path).resolve()


def _default_label(domain_key: str) -> str:
    return domain_key.replace("_", " ").replace("-", " ").title()


def _load_domains(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"default_domain": None, "domains": {}}


def _write_domains(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def load_domains(path: Path = DOMAINS_PATH) -> dict:
    """Load config/domains.json, returning an empty catalog if missing."""
    return _load_domains(path)


def save_domains(payload: dict, path: Path = DOMAINS_PATH) -> None:
    """Save a domains.json payload with stable pretty JSON formatting."""
    _write_domains(path, payload)


def set_env_value(path: Path, key: str, value: str) -> None:
    """Set or append KEY=VALUE in a simple dotenv-style file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = path.read_text(encoding="utf-8").splitlines() if path.exists() else []
    replacement = f"{key}={value}"
    for idx, line in enumerate(lines):
        if line.strip().startswith(f"{key}="):
            lines[idx] = replacement
            break
    else:
        if lines and lines[-1].strip():
            lines.append("")
        lines.append(replacement)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def validate_domain_key(domain_key: str) -> str:
    """Validate and return a domain key suitable for domains.json."""
    if not domain_key or not DOMAIN_KEY_RE.match(domain_key):
        raise DomainRegistryError(
            "Domain key must start with a letter, number, or underscore and "
            "contain only letters, numbers, underscores, and hyphens."
        )
    return domain_key


def validate_bbox(north: str, east: str, south: str, west: str) -> tuple[float, float, float, float]:
    """Validate and return bbox coordinates as north, east, south, west floats."""
    try:
        n = float(north)
        e = float(east)
        s = float(south)
        w = float(west)
    except ValueError as exc:
        raise DomainRegistryError("Bounding box values must be numeric.") from exc

    if not (-90.0 <= n <= 90.0 and -90.0 <= s <= 90.0):
        raise DomainRegistryError("Latitude values must be between -90 and 90.")
    if not (-180.0 <= e <= 180.0 and -180.0 <= w <= 180.0):
        raise DomainRegistryError("Longitude values must be between -180 and 180.")
    if n <= s:
        raise DomainRegistryError("North latitude must be greater than south latitude.")
    if e <= w:
        raise DomainRegistryError("East longitude must be greater than west longitude.")
    return n, e, s, w


def default_terrain_output_path(
    domain_key: str,
    terrain_source: str,
    *,
    base_dir: Path = BASE_DIR,
    runtime_env_path: Path = RUNTIME_ENV_PATH,
) -> str:
    """Return the default output path used by `mwn.sh domain create`."""
    validate_domain_key(domain_key)
    if terrain_source not in TERRAIN_SOURCES:
        raise DomainRegistryError(
            "Terrain source must be one of: gmted, lcp, srtm, us."
        )
    suffix = ".lcp" if terrain_source == "lcp" else ".tif"
    output = _static_data_root(base_dir, runtime_env_path) / f"{domain_key}{suffix}"
    try:
        return output.relative_to(base_dir).as_posix()
    except ValueError:
        return output.as_posix()


def terrain_path_for_config(
    terrain_path: str | Path,
    *,
    base_dir: Path = BASE_DIR,
    runtime_env_path: Path = RUNTIME_ENV_PATH,
) -> str:
    """Return the domains.json elevation_file value for a downloaded terrain file."""
    terrain = Path(terrain_path)
    terrain_abs = terrain if terrain.is_absolute() else (base_dir / terrain).resolve()
    static_root = _static_data_root(base_dir, runtime_env_path)
    try:
        return terrain_abs.relative_to(static_root).as_posix()
    except ValueError as exc:
        raise DomainRegistryError(
            f"Registered terrain must be inside {static_root}."
        ) from exc


def register_domain(
    domain_key: str,
    terrain_path: str | Path,
    *,
    label: str | None = None,
    set_default: bool = False,
    base_dir: Path = BASE_DIR,
    domains_path: Path | None = None,
    runtime_env_path: Path | None = None,
) -> dict:
    """Add or update one domains.json entry and optionally set it as default."""
    validate_domain_key(domain_key)

    domains_path = domains_path or (base_dir / "config" / "domains.json")
    runtime_env_path = runtime_env_path or (base_dir / "config" / "runtime.env")
    payload = _load_domains(domains_path)
    domains = payload.setdefault("domains", {})
    existing = domains.get(domain_key, {})
    terrain_config_path = terrain_path_for_config(
        terrain_path, base_dir=base_dir, runtime_env_path=runtime_env_path,
    )

    domains[domain_key] = {
        "label": label or existing.get("label") or _default_label(domain_key),
        "template": existing.get("template", DEFAULT_TEMPLATE),
        "elevation_file": terrain_config_path,
    }
    if set_default:
        payload["default_domain"] = domain_key

    _write_domains(domains_path, payload)

    if set_default:
        set_env_value(runtime_env_path, "MWN_DOMAIN_ID", domain_key)

    return domains[domain_key]


def upsert_domain(
    domain_id: str,
    label: str | None,
    elevation_file: str | Path,
    template: str = DEFAULT_TEMPLATE,
    *,
    set_default: bool = False,
    base_dir: Path = BASE_DIR,
    domains_path: Path | None = None,
    runtime_env_path: Path | None = None,
) -> dict:
    """Add or update one domain while preserving register_domain path rules."""
    domains_path = domains_path or (base_dir / "config" / "domains.json")
    runtime_env_path = runtime_env_path or (base_dir / "config" / "runtime.env")
    entry = register_domain(
        domain_id,
        elevation_file,
        label=label,
        set_default=set_default,
        base_dir=base_dir,
        domains_path=domains_path,
        runtime_env_path=runtime_env_path,
    )
    payload = _load_domains(domains_path)
    payload["domains"][domain_id]["template"] = template
    _write_domains(domains_path, payload)
    entry["template"] = template
    return entry


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    bbox_parser = subparsers.add_parser("validate-bbox")
    bbox_parser.add_argument("north")
    bbox_parser.add_argument("east")
    bbox_parser.add_argument("south")
    bbox_parser.add_argument("west")

    key_parser = subparsers.add_parser("validate-key")
    key_parser.add_argument("domain_key")

    output_parser = subparsers.add_parser("default-output")
    output_parser.add_argument("domain_key")
    output_parser.add_argument("terrain_source")

    register_parser = subparsers.add_parser("register-domain")
    register_parser.add_argument("domain_key")
    register_parser.add_argument("terrain_path")
    register_parser.add_argument("--label")
    register_parser.add_argument("--set-default", action="store_true")

    env_parser = subparsers.add_parser("set-env")
    env_parser.add_argument("key")
    env_parser.add_argument("value")

    args = parser.parse_args(argv)

    try:
        if args.command == "validate-bbox":
            validate_bbox(args.north, args.east, args.south, args.west)
        elif args.command == "validate-key":
            validate_domain_key(args.domain_key)
        elif args.command == "default-output":
            print(default_terrain_output_path(args.domain_key, args.terrain_source))
        elif args.command == "register-domain":
            entry = register_domain(
                args.domain_key,
                args.terrain_path,
                label=args.label,
                set_default=args.set_default,
            )
            print(
                f"Registered domain {args.domain_key}: "
                f"{entry['label']} -> {entry['elevation_file']}"
            )
        elif args.command == "set-env":
            set_env_value(RUNTIME_ENV_PATH, args.key, args.value)
            print(f"Set {args.key} in {RUNTIME_ENV_PATH}")
    except DomainRegistryError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
