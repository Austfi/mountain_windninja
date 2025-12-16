#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path
from typing import Any

import requests

from caic_tabular import build_caic_tabular_url, fetch_caic_tabular, parse_caic_tabular, to_hourly_station_obs
from windninja_stationfile import build_windninja_station_row, write_windninja_station_csv

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore


STATION_REGISTRY: list[dict[str, Any]] = [
    {
        "code": "CAKWS",
        "name": "Keystone SA - Wind Study",
        "height_m": 10.0,
        "lat": 39.56216,
        "lon": -105.91444,
    },
    {
        "code": "CAKWP",
        "name": "Keystone SA - Wapiti",
        "height_m": 10.0,
        "lat": 39.54505,
        "lon": -105.91913,
    },
]


def _parse_end_utc(s: str) -> dt.datetime:
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    d = dt.datetime.fromisoformat(s)
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(dt.timezone.utc)


def _align_to_top_of_hour(d: dt.datetime) -> dt.datetime:
    return d.replace(minute=0, second=0, microsecond=0)


def _iso(d: dt.datetime | None) -> str | None:
    if d is None:
        return None
    if d.tzinfo is None:
        return d.replace(tzinfo=dt.timezone.utc).isoformat()
    return d.astimezone(dt.timezone.utc).isoformat()


def _pick_numeric(rec: dict[str, Any] | None, candidates: list[str]) -> float | None:
    if not rec:
        return None
    lower_to_key = {k.lower(): k for k in rec.keys()}
    for name in candidates:
        key = lower_to_key.get(name.lower())
        if key is None:
            continue
        v = rec.get(key)
        if isinstance(v, (int, float)):
            return float(v)
        try:
            if v is None or v == "":
                continue
            return float(v)
        except Exception:
            continue
    return None


def build_station_inputs(
    end_utc: dt.datetime,
    hours: int,
    out_dir: str | Path,
    template_csv: str | Path,
    *,
    unit: str = "e",
    range_hours: int = 48,
    cache_raw: bool = False,
    best_effort: bool = False,
    area: str = "caic",
    tz_local: str = "America/Denver",
) -> list[str]:
    """
    Build per-hour WindNinja station CSVs (point initialization inputs) for a UTC window.

    Returns list of output CSV paths. Always writes manifest.json in out_dir.
    Raises RuntimeError if any fetch fails and best_effort=False.
    """
    if ZoneInfo is None:
        raise RuntimeError("zoneinfo.ZoneInfo is required (Python 3.9+).")

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    end_utc = end_utc.astimezone(dt.timezone.utc)
    end_utc_aligned = _align_to_top_of_hour(end_utc)

    if hours <= 0:
        raise ValueError("--hours must be > 0")

    target_hours_utc = [
        (end_utc_aligned - dt.timedelta(hours=i)).astimezone(dt.timezone.utc)
        for i in reversed(range(hours))
    ]
    start_utc = target_hours_utc[0]

    tz = ZoneInfo(tz_local)
    end_local = end_utc_aligned.astimezone(tz)

    # Output units assumptions:
    # - unit='e' typically means mph + Fahrenheit.
    # - for unit != 'e', confirm CAIC output units and adjust if needed.
    speed_units = "mph" if unit == "e" else "m/s"
    temp_units = "F" if unit == "e" else "C"
    roi_units = "miles" if unit == "e" else "km"

    session = requests.Session()

    manifest: dict[str, Any] = {
        "created_utc": _iso(dt.datetime.now(dt.timezone.utc)),
        "requested": {
            "end_utc": _iso(end_utc),
            "end_utc_aligned": _iso(end_utc_aligned),
            "hours": hours,
            "range_hours": range_hours,
            "unit": unit,
            "area": area,
            "tz_local": tz_local,
            "best_effort": best_effort,
            "cache_raw": cache_raw,
        },
        "window": {
            "start_utc": _iso(start_utc),
            "end_utc": _iso(end_utc_aligned),
            "hours_utc": [_iso(h) for h in target_hours_utc],
        },
        "stations": [],
        "outputs": {
            "out_dir": str(out_dir),
            "template_csv": str(Path(template_csv)),
            "csv_files": [],
            "raw_html_files": [],
            "manifest_path": str(out_dir / "manifest.json"),
        },
        "errors": [],
    }

    station_hourly: dict[str, dict[dt.datetime, dict[str, Any] | None]] = {}

    for st in STATION_REGISTRY:
        code = st["code"]
        st_entry: dict[str, Any] = {
            "code": code,
            "name": st["name"],
            "lat": st["lat"],
            "lon": st["lon"],
            "height_m": st["height_m"],
            "caic_url": None,
            "fetch_ok": False,
            "fetch_error": None,
            "parsed_records": 0,
            "parsed_columns": [],
            "parsed_first_dt_utc": None,
            "parsed_last_dt_utc": None,
            "hourly_missing_hours_utc": [],
        }

        url = build_caic_tabular_url(code, end_local, range_hours, unit=unit, area=area)
        st_entry["caic_url"] = url

        raw_text: str | None = None
        try:
            raw_text = fetch_caic_tabular(code, end_local, range_hours, unit=unit, area=area, session=session)
            st_entry["fetch_ok"] = True
        except Exception as e:
            st_entry["fetch_ok"] = False
            st_entry["fetch_error"] = str(e)
            manifest["errors"].append({"station": code, "step": "fetch", "error": str(e), "url": url})

        if cache_raw and raw_text is not None:
            raw_name = f"caic_{code}_end_{end_utc_aligned.strftime('%Y%m%d_%H00Z')}.html"
            raw_path = out_dir / raw_name
            raw_path.write_text(raw_text)
            manifest["outputs"]["raw_html_files"].append(str(raw_path))

        records: list[dict[str, Any]] = []
        if raw_text is not None:
            try:
                records = parse_caic_tabular(raw_text, tz_local=tz_local)
            except Exception as e:
                manifest["errors"].append({"station": code, "step": "parse", "error": str(e), "url": url})

        st_entry["parsed_records"] = len(records)
        if records:
            cols = [k for k in records[0].keys() if k not in {"dt_local", "dt_utc"} and not k.startswith("_")]
            st_entry["parsed_columns"] = cols
            st_entry["parsed_first_dt_utc"] = _iso(records[0].get("dt_utc"))
            st_entry["parsed_last_dt_utc"] = _iso(records[-1].get("dt_utc"))

        hourly_map = to_hourly_station_obs(records, target_hours_utc)
        station_hourly[code] = hourly_map

        missing = [h for h, v in hourly_map.items() if v is None]
        st_entry["hourly_missing_hours_utc"] = [_iso(h) for h in missing]

        manifest["stations"].append(st_entry)

        # extra gentle throttle across stations
        time.sleep(0.2)

    csv_paths: list[str] = []
    for hour in target_hours_utc:
        stamp = hour.strftime("%Y%m%d_%H00Z")
        out_csv = out_dir / f"stations_{stamp}.csv"

        rows: list[dict[str, Any]] = []
        for st in STATION_REGISTRY:
            code = st["code"]
            rec = station_hourly.get(code, {}).get(hour)

            speed = _pick_numeric(rec, ["Spd", "SPD", "Speed", "WindSpeed", "WSpd"])
            direction = _pick_numeric(rec, ["Dir", "DIR", "Direction", "WindDir", "WDir"])
            temperature = _pick_numeric(rec, ["Temp", "TEMP", "Temperature", "AirTemp", "T"])

            rows.append(
                build_windninja_station_row(
                    station_name=code,
                    lat=st["lat"],
                    lon=st["lon"],
                    height=st["height_m"],
                    height_units="meters",
                    speed=speed,
                    speed_units=speed_units,
                    direction=direction,
                    temperature=temperature,
                    temperature_units=temp_units,
                    cloud_cover=0,
                    radius_of_influence=-1.0,
                    radius_units=roi_units,
                )
            )

        write_windninja_station_csv(template_csv, out_csv, rows)
        csv_paths.append(str(out_csv))

    manifest["outputs"]["csv_files"] = csv_paths

    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))

    fetch_failures = [e for e in manifest["errors"] if e.get("step") == "fetch"]
    if fetch_failures and not best_effort:
        raise RuntimeError(
            f"{len(fetch_failures)} CAIC fetch failure(s). See manifest: {out_dir / 'manifest.json'}"
        )

    return csv_paths


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Scrape CAIC tabular station data and write WindNinja point-init station CSVs for a UTC window."
    )
    p.add_argument("--end-utc", required=True, help="ISO8601 UTC end time, e.g. 2025-12-14T06:00:00Z")
    p.add_argument("--hours", type=int, default=18, help="Number of UTC hours to generate (default 18)")
    p.add_argument("--out-dir", required=True, help="Output directory")
    p.add_argument(
        "--template-csv",
        default="templates/windninja_station_template.csv",
        help="Path to station CSV template (default templates/windninja_station_template.csv)",
    )
    p.add_argument("--unit", default="e", help="CAIC units query param (default 'e')")
    p.add_argument("--range-hours", type=int, default=48, help="CAIC lookback range (default 48)")
    p.add_argument("--cache-raw", action="store_true", help="Save raw CAIC HTML to out-dir")
    p.add_argument("--best-effort", action="store_true", help="Do not fail the run if a station fetch fails")

    args = p.parse_args(argv)

    try:
        end_utc = _parse_end_utc(args.end_utc)
        build_station_inputs(
            end_utc=end_utc,
            hours=args.hours,
            out_dir=args.out_dir,
            template_csv=args.template_csv,
            unit=args.unit,
            range_hours=args.range_hours,
            cache_raw=args.cache_raw,
            best_effort=args.best_effort,
        )
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
