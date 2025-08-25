"""Provider registry supporting primary and fallback data sources.

The module provides small provider implementations for the World Bank and
FRED APIs.  Both expose a ``fetch`` method returning :class:`EventPayload` and
can be combined via :class:`ProviderRegistry` to create multi-source consensus
logic.  No Trading Economics usage per specs.
"""
from __future__ import annotations

import os
import statistics
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests


@dataclass
class EventPayload:
    series_id: str
    actual: float
    consensus: Optional[float]
    previous: Optional[float]
    impact: str
    release_time_local: str  # local timestamp in source tz
    release_time_utc: str    # converted to UTC
    provider: str
    unit: str = ""


class WorldBankProvider:
    """Fetches indicator values from the World Bank API."""

    BASE_URL = (
        "https://api.worldbank.org/v2/country/{country}/indicator/{indicator}?format=json"
    )

    def __init__(self, mapping: Dict[str, Tuple[str, str]]):
        self.mapping = mapping

    def fetch(self, series_id: str) -> EventPayload:
        country, indicator = self.mapping[series_id]
        resp = requests.get(self.BASE_URL.format(country=country, indicator=indicator))
        resp.raise_for_status()
        payload = resp.json()[1]
        data = payload[0]
        value = float(data["value"])
        date = f"{data['date']}-12-31"  # annual data at year end
        previous = float(payload[1]["value"]) if len(payload) > 1 else None
        consensus = previous  # naive nowcast
        return EventPayload(
            series_id=series_id,
            actual=value,
            consensus=consensus,
            previous=previous,
            impact="mid",
            release_time_local=f"{date}T00:00:00",
            release_time_utc=f"{date}T00:00:00Z",
            provider="worldbank",
            unit="percent",
        )


class FredProvider:
    """Fetches data from the FRED API (https://fred.stlouisfed.org)."""

    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

    def __init__(self, mapping: Dict[str, str], api_key: Optional[str] = None):
        self.mapping = mapping
        self.api_key = api_key or os.getenv("FRED_API_KEY")

    def fetch(self, series_id: str) -> EventPayload:
        if not self.api_key:
            raise RuntimeError("FRED API key missing")
        fred_id = self.mapping[series_id]
        params = {
            "series_id": fred_id,
            "api_key": self.api_key,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 2,
        }
        resp = requests.get(self.BASE_URL, params=params)
        resp.raise_for_status()
        data = resp.json()["observations"]
        latest, prev = data[0], data[1] if len(data) > 1 else ({"value": None, "date": None})
        value = float(latest["value"])
        previous = float(prev["value"]) if prev["value"] not in (None, ".") else None
        date = latest["date"]
        return EventPayload(
            series_id=series_id,
            actual=value,
            consensus=previous,  # proxy consensus
            previous=previous,
            impact="mid",
            release_time_local=f"{date}T00:00:00",
            release_time_utc=f"{date}T00:00:00Z",
            provider="fred",
            unit="index",
        )


class ProviderRegistry:
    """Registry mapping series IDs to a list of providers (primary first)."""

    def __init__(self) -> None:
        self._providers: Dict[str, List[object]] = {}

    def register(self, series_id: str, providers: List[object]) -> None:
        self._providers[series_id] = providers

    def fetch(self, series_id: str) -> EventPayload:
        for provider in self._providers.get(series_id, []):
            try:
                return provider.fetch(series_id)
            except Exception:
                continue
        raise RuntimeError(f"no provider could fetch series {series_id}")
