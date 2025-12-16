"""
CAIC tabular.php scraper utilities.

Key points:
- Builds tabular.php URLs programmatically (no hard-coded full URLs).
- Fetches with requests + retry logic.
- Parses the CAIC <pre> tabular text with dynamic header detection.
- Converts America/Denver local timestamps (with DST) to UTC using zoneinfo.
"""

from __future__ import annotations

import datetime as dt
import re
import time
from html.parser import HTMLParser
from urllib.parse import urlencode

import requests

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore


CAIC_TABULAR_ENDPOINT = "https://stations.avalanche.state.co.us/tabular.php"
_MISSING_TOKENS = {
    "",
    "M",
    "m",
    "NA",
    "N/A",
    "nan",
    "NaN",
    "---",
    "--",
    "////",
    "null",
    "NULL",
    "T",
}


class _PreTextParser(HTMLParser):
    """Extracts text contained inside <pre> tags."""

    def __init__(self) -> None:
        super().__init__()
        self._in_pre = False
        self.chunks: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() == "pre":
            self._in_pre = True

    def handle_endtag(self, tag):
        if tag.lower() == "pre":
            self._in_pre = False

    def handle_data(self, data):
        if self._in_pre:
            self.chunks.append(data)


def _extract_pre_text(html: str) -> str:
    parser = _PreTextParser()
    try:
        parser.feed(html)
    except Exception:
        return html
    pre = "".join(parser.chunks).strip("\n")
    return pre if pre.strip() else html


def build_caic_tabular_url(
    st_code: str,
    end_dt_local: dt.datetime,
    range_hours: int,
    unit: str = "e",
    area: str = "caic",
) -> str:
    """
    Builds a CAIC tabular.php station URL.

    CAIC expects:
      st=STATIONCODE
      date=YYYY-MM-DD HH   (space will be encoded as '+')
      range=HOURS_BACK
      unit=e|m ...
      area=caic
    """
    if end_dt_local.tzinfo is None:
        raise ValueError("end_dt_local must be timezone-aware (America/Denver).")
    if range_hours <= 0:
        raise ValueError("range_hours must be > 0.")

    date_str = end_dt_local.strftime("%Y-%m-%d %H")

    qs = urlencode(
        {"st": st_code, "date": date_str, "unit": unit, "area": area, "range": str(range_hours)}
    )
    return f"{CAIC_TABULAR_ENDPOINT}?{qs}"


def fetch_caic_tabular(
    st_code: str,
    end_dt_local: dt.datetime,
    range_hours: int,
    unit: str = "e",
    area: str = "caic",
    session: requests.Session | None = None,
    timeout_s: float = 20.0,
    max_tries: int = 3,
    throttle_s: float = 0.6,
) -> str:
    """
    Fetches the CAIC tabular.php HTML.
    Includes basic retry + backoff and a small per-fetch throttle.
    """
    url = build_caic_tabular_url(st_code, end_dt_local, range_hours, unit=unit, area=area)

    sess = session or requests.Session()
    headers = {
        "User-Agent": "mountain_windninja/caic_tabular (+https://github.com/Austfi/mountain_windninja)"
    }

    last_exc: Exception | None = None
    for attempt in range(max_tries):
        try:
            resp = sess.get(url, headers=headers, timeout=timeout_s)
            resp.raise_for_status()
            text = resp.text
            if throttle_s > 0:
                time.sleep(throttle_s)
            return text
        except Exception as e:
            last_exc = e
            if attempt < max_tries - 1:
                time.sleep(0.75 * (2 ** attempt))
            else:
                break

    raise RuntimeError(f"Failed to fetch CAIC tabular for {st_code} after {max_tries} tries: {url}") from last_exc


def _parse_float(tok: str) -> float | None:
    tok = tok.strip()
    if tok in _MISSING_TOKENS:
        return None
    try:
        return float(tok)
    except Exception:
        return None


def parse_caic_tabular(text: str, tz_local: str = "America/Denver") -> list[dict]:
    """
    Parses CAIC tabular.php HTML into a list of dict records.

    Each record includes:
      dt_local (tz-aware)
      dt_utc   (tz-aware UTC)
      plus numeric columns keyed by whatever CAIC's header row provides.

    Header detection is dynamic: we look for the line that starts with "Date Time".
    """
    if ZoneInfo is None:
        raise RuntimeError("zoneinfo.ZoneInfo is required (Python 3.9+).")
    tz = ZoneInfo(tz_local)

    body = _extract_pre_text(text)
    lines = [ln.rstrip() for ln in body.splitlines()]

    header_tokens: list[str] | None = None
    for ln in lines:
        toks = ln.split()
        if len(toks) >= 2 and toks[0].lower() == "date" and toks[1].lower() == "time":
            header_tokens = toks
            break
        low = ln.lower()
        if "date" in low and "time" in low and ("spd" in low or "dir" in low):
            header_tokens = toks
            break

    if not header_tokens:
        raise ValueError("Could not detect CAIC header line containing 'Date' and 'Time'.")

    colnames = header_tokens[2:]

    records: list[dict] = []
    for ln in lines:
        ln = ln.strip()
        if not ln:
            continue
        if not re.match(r"^\d{4}\s", ln):
            continue

        parts = ln.split()
        if len(parts) < 5:
            continue

        # Date/time parsing:
        # 12h format: YYYY Mon DD HH:MM am/pm ...
        # 24h format fallback: YYYY Mon DD HH:MM ...
        try:
            if len(parts) >= 5 and parts[4].lower() in {"am", "pm"}:
                dt_str = f"{parts[0]} {parts[1]} {parts[2]} {parts[3]} {parts[4]}"
                dt_naive = dt.datetime.strptime(dt_str, "%Y %b %d %I:%M %p")
                data_start = 5
            else:
                dt_str = f"{parts[0]} {parts[1]} {parts[2]} {parts[3]}"
                dt_naive = dt.datetime.strptime(dt_str, "%Y %b %d %H:%M")
                data_start = 4
            dt_local = dt_naive.replace(tzinfo=tz)
        except Exception:
            continue

        dt_utc = dt_local.astimezone(dt.timezone.utc)

        values = parts[data_start:]
        rec: dict = {"dt_local": dt_local, "dt_utc": dt_utc}
        for i, col in enumerate(colnames):
            tok = values[i] if i < len(values) else ""
            rec[col] = _parse_float(tok)

        records.append(rec)

    records.sort(key=lambda r: r["dt_utc"])
    return records


def to_hourly_station_obs(
    records: list[dict],
    target_hours_utc: list[dt.datetime],
    max_offset_minutes: int = 30,
) -> dict[dt.datetime, dict | None]:
    """
    For each target UTC hour:
      - choose an exact matching sample (dt_utc == target), else
      - choose the nearest sample within +/- max_offset_minutes, else None.
    """
    samples: list[tuple[dt.datetime, dict]] = []
    for r in records:
        dtu = r.get("dt_utc")
        if isinstance(dtu, dt.datetime):
            if dtu.tzinfo is None:
                dtu = dtu.replace(tzinfo=dt.timezone.utc)
            else:
                dtu = dtu.astimezone(dt.timezone.utc)
            samples.append((dtu, r))
    samples.sort(key=lambda t: t[0])

    out: dict[dt.datetime, dict | None] = {}
    for hour in target_hours_utc:
        if hour.tzinfo is None:
            hour = hour.replace(tzinfo=dt.timezone.utc)
        else:
            hour = hour.astimezone(dt.timezone.utc)

        best: tuple[float, dict, str] | None = None  # (abs_seconds, record, kind)

        for dtu, rec in samples:
            delta_s = abs((dtu - hour).total_seconds())
            if delta_s == 0:
                best = (0.0, rec, "exact")
                break
            if delta_s <= max_offset_minutes * 60:
                if best is None or delta_s < best[0]:
                    best = (delta_s, rec, "nearest")

        if best is None:
            out[hour] = None
        else:
            matched = dict(best[1])
            matched["_match_kind"] = best[2]
            matched["_match_delta_minutes"] = round(best[0] / 60.0, 3)
            out[hour] = matched

    return out
