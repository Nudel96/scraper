"""Utilities to normalize raw event payloads."""
from __future__ import annotations

from dataclasses import asdict
from typing import Dict, Optional

from dateutil import parser, tz


def normalize_units(payload: Dict) -> Dict:
    """Normalize units such as percentages to ratios."""

    unit = payload.get("unit", "").lower()
    if unit in {"%", "percent", "percentage"}:
        for key in ["actual", "consensus", "previous"]:
            if payload.get(key) is not None:
                payload[key] = float(payload[key]) / 100.0
        payload["unit"] = "ratio"
    return payload


def to_utc(local_ts: str, source_tz: Optional[str] = None) -> str:
    """Convert a timestamp in ``source_tz`` to UTC (DST safe)."""

    dt = parser.isoparse(local_ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz.gettz(source_tz or "UTC"))
    return dt.astimezone(tz.UTC).isoformat().replace("+00:00", "Z")
