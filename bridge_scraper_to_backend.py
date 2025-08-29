#!/usr/bin/env python3
"""
Bridge Script: Scraper → Backend Integration
============================================

Transforms Scraper SQLite events into Backend PostgreSQL format.
Handles asset mapping, pillar categorization, and score normalization.

Usage:
    python bridge_scraper_to_backend.py [--dry-run] [--series-id SERIES_ID]
"""

import sqlite3
import logging
import argparse
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from uuid import uuid4
from dataclasses import dataclass

import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Import backend models
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend-scraper'))

from infra.db import Base, Asset, Event as BackendEvent, Indicator, Score
from infra.settings import settings
from core.scoring.engine import compute_score

# Import asset mapping system
from asset_mapping_system import AssetMappingSystem

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class ScraperEvent:
    """Scraper event structure from SQLite"""
    series_id: str
    release_date: str
    vintage: str
    actual: float
    consensus: Optional[float]
    previous: Optional[float]
    impact: str
    release_time_utc: str
    provider: str

class AssetMapper:
    """Maps scraper series_ids to trading assets and pillars using the Asset Mapping System"""

    def __init__(self, config_path: str = "asset_mapping_config.yaml"):
        self.mapping_system = AssetMappingSystem(config_path)

    def get_mapping(self, series_id: str) -> Optional[Tuple[str, str, str]]:
        """Get (asset, pillar, key) for series_id"""
        mapping_info = self.mapping_system.get_mapping(series_id)
        if mapping_info:
            return (mapping_info.asset, mapping_info.pillar, mapping_info.key)
        return None

    def get_mapping_info(self, series_id: str):
        """Get complete mapping information"""
        return self.mapping_system.get_mapping(series_id)

    def get_supported_assets(self) -> List[str]:
        """Get list of all supported asset symbols"""
        return self.mapping_system.get_supported_assets()

    def get_impact_multiplier(self, impact: str) -> float:
        """Get impact multiplier for scoring"""
        return self.mapping_system.get_impact_multiplier(impact)

    def get_frequency_decay(self, frequency: str) -> int:
        """Get decay half-life in days for frequency"""
        return self.mapping_system.get_frequency_decay(frequency)

class ScoreConverter:
    """Converts scraper scores (-2 to +2) to backend scores (-24 to +24)"""
    
    @staticmethod
    def convert_score(scraper_score: float) -> int:
        """Convert scraper score (-2 to +2) to backend score (-24 to +24)"""
        # Clamp input to valid range
        scraper_score = max(-2.0, min(2.0, scraper_score))
        
        # Scale: -2→-24, 0→0, +2→+24
        backend_score = int(scraper_score * 12)
        
        # Ensure within backend range
        return max(-24, min(24, backend_score))

class BridgeProcessor:
    """Main processor for bridging scraper data to backend"""

    def __init__(self, scraper_db_path: str = "scraper/events.db", backend_db_url: Optional[str] = None,
                 asset_config_path: str = "asset_mapping_config.yaml"):
        self.scraper_db_path = scraper_db_path
        self.backend_db_url = backend_db_url or settings.database_url

        # Setup asset mapper
        self.asset_mapper = AssetMapper(asset_config_path)

        # Setup backend database connection
        self.backend_engine = create_engine(self.backend_db_url)
        self.BackendSession = sessionmaker(bind=self.backend_engine)

        # Create tables if they don't exist
        Base.metadata.create_all(bind=self.backend_engine)
        
    def load_scraper_events(self, series_id: Optional[str] = None) -> List[ScraperEvent]:
        """Load events from scraper SQLite database"""
        try:
            conn = sqlite3.connect(self.scraper_db_path)
            
            if series_id:
                query = """
                SELECT series_id, release_date, vintage, actual, consensus, previous,
                       impact, release_time_utc, provider
                FROM events 
                WHERE series_id = ?
                ORDER BY release_date DESC, vintage DESC
                """
                cursor = conn.execute(query, (series_id,))
            else:
                query = """
                SELECT series_id, release_date, vintage, actual, consensus, previous,
                       impact, release_time_utc, provider
                FROM events 
                ORDER BY release_date DESC, vintage DESC
                """
                cursor = conn.execute(query)
            
            events = []
            for row in cursor.fetchall():
                events.append(ScraperEvent(*row))
            
            conn.close()
            logger.info(f"Loaded {len(events)} events from scraper database")
            return events
            
        except sqlite3.Error as e:
            logger.error(f"Error loading scraper events: {e}")
            return []
    
    def ensure_asset_exists(self, session, symbol: str) -> Asset:
        """Ensure asset exists in backend database"""
        asset = session.query(Asset).filter_by(symbol=symbol).first()
        if not asset:
            asset = Asset(symbol=symbol, kind="currency" if len(symbol) == 3 else "commodity")
            session.add(asset)
            session.commit()
            session.refresh(asset)
            logger.info(f"Created new asset: {symbol}")
        return asset
    
    def transform_event(self, scraper_event: ScraperEvent) -> Optional[Dict]:
        """Transform scraper event to backend format using enhanced mapping system"""
        mapping_info = self.asset_mapper.get_mapping_info(scraper_event.series_id)
        if not mapping_info:
            logger.warning(f"No mapping found for series_id: {scraper_event.series_id}")
            return None

        # Calculate enhanced score using mapping configuration
        surprise = 0.0
        if scraper_event.consensus is not None:
            surprise = scraper_event.actual - scraper_event.consensus

        # Enhanced scoring with impact and frequency considerations
        impact_multiplier = self.asset_mapper.get_impact_multiplier(mapping_info.impact)
        base_weight = mapping_info.weight

        # Normalize surprise based on actual value magnitude
        if abs(scraper_event.actual) > 0:
            normalized_surprise = surprise / max(abs(scraper_event.actual), 1.0)
        else:
            normalized_surprise = 0.0

        # Apply impact and weight multipliers
        score_raw = normalized_surprise * impact_multiplier * base_weight

        # Clamp to scraper range and convert to backend range
        score_raw = max(-2.0, min(2.0, score_raw))
        score_backend = ScoreConverter.convert_score(score_raw)

        return {
            "asset_symbol": mapping_info.asset,
            "pillar": mapping_info.pillar,
            "key": mapping_info.key,
            "value": score_backend,
            "timestamp": datetime.fromisoformat(scraper_event.release_time_utc.replace('Z', '+00:00')),
            "metadata": {
                "series_id": scraper_event.series_id,
                "actual": scraper_event.actual,
                "consensus": scraper_event.consensus,
                "previous": scraper_event.previous,
                "impact": scraper_event.impact,
                "provider": scraper_event.provider,
                "vintage": scraper_event.vintage,
                "mapping_weight": mapping_info.weight,
                "impact_multiplier": impact_multiplier,
                "frequency": mapping_info.frequency,
                "description": mapping_info.description
            }
        }
    
    def process_events(self, events: List[ScraperEvent], dry_run: bool = False) -> Dict[str, int]:
        """Process scraper events and insert into backend"""
        stats = {"processed": 0, "skipped": 0, "errors": 0}
        
        with self.BackendSession() as session:
            for event in events:
                try:
                    transformed = self.transform_event(event)
                    if not transformed:
                        stats["skipped"] += 1
                        continue
                    
                    if dry_run:
                        logger.info(f"DRY RUN: Would process {event.series_id} → {transformed['asset_symbol']}")
                        stats["processed"] += 1
                        continue
                    
                    # Ensure asset exists
                    asset = self.ensure_asset_exists(session, transformed["asset_symbol"])
                    
                    # Create backend event
                    backend_event = BackendEvent(
                        trace_id=str(uuid4()),
                        source="scraper_bridge",
                        asset_id=asset.id,
                        kind="indicator",
                        ingested_at=transformed["timestamp"],
                        payload={
                            "key": transformed["key"],
                            "value": transformed["value"],
                            "metadata": transformed["metadata"]
                        }
                    )
                    
                    # Check if event already exists
                    existing = session.query(BackendEvent).filter_by(
                        asset_id=asset.id,
                        kind="indicator",
                        ingested_at=transformed["timestamp"]
                    ).first()
                    
                    if existing:
                        logger.debug(f"Event already exists for {transformed['asset_symbol']} at {transformed['timestamp']}")
                        stats["skipped"] += 1
                        continue
                    
                    session.add(backend_event)
                    session.commit()
                    session.refresh(backend_event)
                    
                    # Create indicator
                    indicator = Indicator(
                        asset_id=asset.id,
                        key=transformed["key"],
                        ts=transformed["timestamp"],
                        value=transformed["value"],
                        meta=transformed["metadata"]
                    )
                    session.merge(indicator)  # Use merge to handle duplicates
                    session.commit()
                    
                    # Recompute score for asset
                    compute_score(session, asset.id)
                    
                    stats["processed"] += 1
                    logger.info(f"Processed {event.series_id} → {transformed['asset_symbol']} (score: {transformed['value']})")
                    
                except Exception as e:
                    logger.error(f"Error processing event {event.series_id}: {e}")
                    stats["errors"] += 1
                    session.rollback()
        
        return stats

def main():
    parser = argparse.ArgumentParser(description="Bridge scraper data to backend")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed without making changes")
    parser.add_argument("--series-id", help="Process only specific series ID")
    parser.add_argument("--scraper-db", default="scraper/events.db", help="Path to scraper SQLite database")
    parser.add_argument("--backend-db", help="Backend database URL (default from settings)")
    
    args = parser.parse_args()
    
    logger.info("Starting scraper → backend bridge process")
    
    # Initialize processor
    processor = BridgeProcessor(
        scraper_db_path=args.scraper_db,
        backend_db_url=args.backend_db
    )
    
    # Load events
    events = processor.load_scraper_events(args.series_id)
    if not events:
        logger.warning("No events found to process")
        return
    
    # Process events
    stats = processor.process_events(events, dry_run=args.dry_run)
    
    # Report results
    logger.info(f"Bridge process completed:")
    logger.info(f"  Processed: {stats['processed']}")
    logger.info(f"  Skipped: {stats['skipped']}")
    logger.info(f"  Errors: {stats['errors']}")
    
    if args.dry_run:
        logger.info("DRY RUN - No changes were made")

if __name__ == "__main__":
    main()
