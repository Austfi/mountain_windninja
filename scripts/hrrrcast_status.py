#!/usr/bin/env python3
"""Report the latest usable HRRRCast cycle for a requested forecast window."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys

try:
    from . import config_loader
    from .hrrrcast_wx_model import (
        HrrrCastWeatherError,
        expand_hrrrcast_members,
        normalize_hrrrcast_member,
        resolve_hrrrcast_cycle_plan,
    )
except ImportError:
    import config_loader
    from hrrrcast_wx_model import (
        HrrrCastWeatherError,
        expand_hrrrcast_members,
        normalize_hrrrcast_member,
        resolve_hrrrcast_cycle_plan,
    )


def parse_utc_timestamp(raw: str) -> dt.datetime:
    value = raw.strip().replace("Z", "")
    for fmt in ("%Y%m%d%H%M", "%Y%m%d%H", "%Y-%m-%dT%H:%M", "%Y-%m-%dT%H"):
        try:
            return dt.datetime.strptime(value, fmt)
        except ValueError:
            pass
    raise argparse.ArgumentTypeError(
        "Use UTC as YYYYMMDDHHMM, YYYYMMDDHH, YYYY-MM-DDTHH:MM, or YYYY-MM-DDTHH."
    )


def default_start_time() -> dt.datetime:
    now = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    return now.replace(minute=0, second=0, microsecond=0)


def evaluate_member(
    *,
    member: str,
    start_time: dt.datetime,
    stop_time: dt.datetime,
    base_url: str,
    max_cycle_rewind: int,
) -> dict:
    try:
        plan = resolve_hrrrcast_cycle_plan(
            start_time=start_time,
            stop_time=stop_time,
            member=member,
            base_url=base_url,
            max_cycle_rewind=max_cycle_rewind,
            log_failures=False,
        )
    except HrrrCastWeatherError as exc:
        return {
            "member": member,
            "ok": False,
            "error": str(exc),
        }

    return {
        "member": member,
        "ok": True,
        "cycle": plan.cycle.strftime("%Y%m%d%H%M"),
        "cycle_iso": plan.cycle.strftime("%Y-%m-%dT%H:%MZ"),
        "forecast_hours": list(plan.forecast_hours),
        "fxx_min": min(plan.forecast_hours),
        "fxx_max": max(plan.forecast_hours),
    }


def build_payload(args: argparse.Namespace) -> dict:
    start_time = args.start or default_start_time()
    stop_time = start_time + dt.timedelta(hours=args.hours)
    if args.members:
        members = expand_hrrrcast_members(args.members)
    else:
        members = [normalize_hrrrcast_member(args.member)]

    results = [
        evaluate_member(
            member=member,
            start_time=start_time,
            stop_time=stop_time,
            base_url=args.base_url,
            max_cycle_rewind=args.max_cycle_rewind,
        )
        for member in members
    ]
    return {
        "source": "hrrrcast",
        "start": start_time.strftime("%Y-%m-%dT%H:%MZ"),
        "stop": stop_time.strftime("%Y-%m-%dT%H:%MZ"),
        "hours": args.hours,
        "max_cycle_rewind": args.max_cycle_rewind,
        "base_url": args.base_url,
        "results": results,
        "ok": all(result["ok"] for result in results),
    }


def print_text(payload: dict) -> None:
    print("HRRRCast status")
    print(f"  start: {payload['start']}")
    print(f"  stop:  {payload['stop']}")
    print(f"  rewind window: {payload['max_cycle_rewind']}h")
    for result in payload["results"]:
        if result["ok"]:
            print(
                "  "
                f"{result['member']}: ok "
                f"cycle={result['cycle_iso']} "
                f"f{result['fxx_min']:02d}..f{result['fxx_max']:02d}"
            )
        else:
            print(f"  {result['member']}: failed")
            print(f"    {result['error']}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check which HRRRCast cycle can satisfy a forecast window."
    )
    parser.add_argument("--start", type=parse_utc_timestamp, help="UTC start time.")
    parser.add_argument("--hours", type=int, default=1, help="Forecast hours to check.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--member", default="avg", help="HRRRCast member: avg or m00..m08.")
    group.add_argument("--members", help="Comma-separated members, or all.")
    parser.add_argument(
        "--base-url",
        default=config_loader.HRRRCAST_BASE_URL,
        help="HRRRCast bucket base URL.",
    )
    parser.add_argument(
        "--max-cycle-rewind",
        type=int,
        default=config_loader.HRRRCAST_MAX_CYCLE_REWIND,
        help="Maximum hours to rewind when searching for complete cycles.",
    )
    parser.add_argument("--json", action="store_true", help="Write machine-readable JSON.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.hours < 1:
        parser.error("--hours must be >= 1")
    if args.max_cycle_rewind < 0:
        parser.error("--max-cycle-rewind must be >= 0")

    try:
        payload = build_payload(args)
    except HrrrCastWeatherError as exc:
        parser.error(str(exc))

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print_text(payload)
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
