"""Revision-aware event storage for economic releases.

Each event is stored with its vintage (flash/prelim/final) so that
revisions never overwrite prior releases.  Only events with
``release_time_utc`` in the past can be retrieved for scoring.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Optional


@dataclass
class Event:
    series_id: str
    release_date: str  # YYYY-MM-DD
    vintage: str       # flash/prelim/final
    actual: float
    consensus: Optional[float]
    previous: Optional[float]
    impact: str
    release_time_utc: str  # ISO timestamp
    provider: str


class EventStore:
    """SQLite backed store tracking event revisions."""

    def __init__(self, path: str = "events.db") -> None:
        self.conn = sqlite3.connect(path)
        self._init_db()

    def _init_db(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                series_id TEXT,
                release_date TEXT,
                vintage TEXT,
                actual REAL,
                consensus REAL,
                previous REAL,
                impact TEXT,
                release_time_utc TEXT,
                provider TEXT,
                PRIMARY KEY(series_id, release_date, vintage)
            )
            """
        )
        self.conn.commit()

    def add_event(self, event: Event) -> None:
        """Insert a new event if its release time is not in the future."""
        rt = datetime.fromisoformat(event.release_time_utc)
        if rt > datetime.now(timezone.utc):
            raise ValueError("release_time_utc is in the future")
        self.conn.execute(
            """
            INSERT OR REPLACE INTO events
            (series_id, release_date, vintage, actual, consensus, previous,
             impact, release_time_utc, provider)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (
                event.series_id,
                event.release_date,
                event.vintage,
                event.actual,
                event.consensus,
                event.previous,
                event.impact,
                event.release_time_utc,
                event.provider,
            ),
        )
        self.conn.commit()

    def fetch_events(
        self, series_id: str, as_of: Optional[datetime] = None
    ) -> Iterable[Event]:
        """Yield events for ``series_id`` with release_time_utc <= ``as_of``."""
        if as_of is None:
            as_of = datetime.now(timezone.utc)
        cur = self.conn.execute(
            """
            SELECT series_id, release_date, vintage, actual, consensus, previous,
                   impact, release_time_utc, provider
            FROM events
            WHERE series_id = ? AND release_time_utc <= ?
            ORDER BY release_date ASC, vintage ASC
            """,
            (series_id, as_of.isoformat()),
        )
        for row in cur.fetchall():
            yield Event(*row)
