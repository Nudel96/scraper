"""Scoring utilities turning events into pillar/column points."""
from __future__ import annotations

import math
import statistics
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Tuple

from event_store import Event, EventStore

IMPACT_WEIGHT = {"high": 3.0, "mid": 1.5, "low": 0.75}
HALF_LIFE = {"w": 14, "m": 45, "q": 90, "policy": 90}


def surprise(event: Event, direction: str) -> float:
    if event.consensus is None:
        return 0.0
    if direction == "relative":
        return (event.actual / event.consensus) - 1
    return event.actual - event.consensus


def z_score(series_id: str, surprises: List[float]) -> float:
    if not surprises:
        return 0.0
    mu = statistics.mean(surprises)
    sigma = statistics.pstdev(surprises) or 1.0
    return (surprises[-1] - mu) / sigma


def decay_weight(release_time: str, freq: str, impact: str) -> float:
    rt = datetime.fromisoformat(release_time)
    days = (datetime.now(timezone.utc) - rt).days
    hl = HALF_LIFE.get(freq, 30)
    decay = 0.5 ** (days / hl)
    return decay * IMPACT_WEIGHT.get(impact, 1.0)


def point_from_z(z: float) -> float:
    if abs(z) < 0.25:
        return 0.0
    return max(-2.0, min(2.0, 2 * math.tanh(z / 1.5)))


def category_score(events: Iterable[Tuple[Event, str]]) -> float:
    surprises = []
    weights = []
    for ev, freq in events:
        s = surprise(ev, "relative") if "%" in str(ev.actual) else surprise(ev, "absolute")
        z = z_score(ev.series_id, surprises + [s])
        w = decay_weight(ev.release_time_utc, freq, ev.impact)
        surprises.append(s)
        weights.append(z * w)
    if not weights:
        return 0.0
    z_cat = sum(weights) / len(weights)
    return point_from_z(z_cat)


def pair_point(base: float, quote: float) -> int:
    if base > 0 and quote < 0:
        return 2
    if base < 0 and quote > 0:
        return -2
    if base > 0 and quote == 0:
        return 1
    if base < 0 and quote == 0:
        return -1
    if base == 0 and quote < 0:
        return 1
    if base == 0 and quote > 0:
        return -1
    return 0
