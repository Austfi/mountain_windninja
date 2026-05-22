"""Small config loader for the residual U-Net command-line tools."""
from __future__ import annotations

from pathlib import Path
from typing import Any


def _parse_scalar(raw_value: str) -> Any:
    value = raw_value.strip()
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"none", "null"}:
        return None
    if (
        (value.startswith('"') and value.endswith('"'))
        or (value.startswith("'") and value.endswith("'"))
    ):
        return value[1:-1]
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def load_config(path: str | Path) -> dict[str, Any]:
    """Load the simple nested YAML used by this experiment without PyYAML."""
    config_path = Path(path)
    config: dict[str, Any] = {}
    current_section: dict[str, Any] | None = None

    for raw_line in config_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        if not raw_line.startswith(" "):
            key, sep, value = line.partition(":")
            if not sep:
                raise ValueError(f"Invalid config line in {config_path}: {raw_line!r}")
            key = key.strip()
            if value.strip():
                config[key] = _parse_scalar(value)
                current_section = None
            else:
                current_section = {}
                config[key] = current_section
            continue

        if current_section is None:
            raise ValueError(f"Nested config value without a section: {raw_line!r}")
        key, sep, value = line.strip().partition(":")
        if not sep:
            raise ValueError(f"Invalid config line in {config_path}: {raw_line!r}")
        current_section[key.strip()] = _parse_scalar(value)

    return config


def apply_overrides(config: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    """Apply dotted-key overrides to a config copy."""
    updated = {key: value.copy() if isinstance(value, dict) else value for key, value in config.items()}
    for dotted_key, value in overrides.items():
        section, sep, key = dotted_key.partition(".")
        if not sep:
            updated[section] = value
            continue
        nested = updated.setdefault(section, {})
        if not isinstance(nested, dict):
            raise ValueError(f"Cannot apply nested override to non-section {section!r}")
        nested[key] = value
    return updated

