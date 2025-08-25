"""Example workflow fetching data, storing events and computing scores."""
from __future__ import annotations

from dataclasses import asdict

from event_store import Event, EventStore
from normalizer import normalize_units
from providers import FredProvider, ProviderRegistry, WorldBankProvider
from scoring import category_score


SERIES_ID = "US_CPI"


def run() -> None:
    registry = ProviderRegistry()
    wb_map = {SERIES_ID: ("USA", "FP.CPI.TOTL.ZG")}
    fred_map = {SERIES_ID: "CPIAUCSL"}
    registry.register(SERIES_ID, [FredProvider(fred_map), WorldBankProvider(wb_map)])
    store = EventStore()

    payload = normalize_units(asdict(registry.fetch(SERIES_ID)))
    event = Event(
        series_id=SERIES_ID,
        release_date=payload["release_time_utc"][:10],
        vintage="final",
        actual=payload["actual"],
        consensus=payload["consensus"],
        previous=payload["previous"],
        impact=payload["impact"],
        release_time_utc=payload["release_time_utc"],
        provider=payload["provider"],
    )
    store.add_event(event)

    events = list(store.fetch_events(SERIES_ID))
    score = category_score([(ev, "q") for ev in events])
    print(f"Score for {SERIES_ID}: {score:.2f}")


if __name__ == "__main__":
    run()
