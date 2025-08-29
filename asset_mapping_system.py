#!/usr/bin/env python3
"""
Asset Mapping System
===================

Manages the mapping between scraper series IDs and trading assets.
Handles pillar categorization and scoring configuration.

Usage:
    from asset_mapping_system import AssetMappingSystem
    
    mapper = AssetMappingSystem()
    mapping = mapper.get_mapping("US_CPI")
    assets = mapper.get_supported_assets()
"""

import yaml
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

@dataclass
class AssetInfo:
    """Information about a trading asset"""
    symbol: str
    name: str
    type: str
    major_pair: bool
    description: str

@dataclass
class PillarInfo:
    """Information about a scoring pillar"""
    name: str
    description: str
    weight: float
    indicators: List[str]

@dataclass
class MappingInfo:
    """Complete mapping information for a series ID"""
    series_id: str
    asset: str
    pillar: str
    key: str
    weight: float
    frequency: str
    description: str
    impact: str

class AssetMappingSystem:
    """Manages asset mappings and scoring configuration"""
    
    def __init__(self, config_path: str = "asset_mapping_config.yaml"):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self._validate_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            logger.info(f"Loaded asset mapping config version {config.get('version')}")
            return config
        except FileNotFoundError:
            logger.error(f"Config file not found: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML config: {e}")
            raise
    
    def _validate_config(self) -> None:
        """Validate configuration structure"""
        required_sections = ['assets', 'pillars', 'mappings', 'scoring_rules']
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Missing required config section: {section}")
        
        logger.info("Asset mapping configuration validated successfully")
    
    def get_asset_info(self, symbol: str) -> Optional[AssetInfo]:
        """Get information about an asset"""
        asset_data = self.config['assets'].get(symbol)
        if not asset_data:
            return None
        
        return AssetInfo(
            symbol=symbol,
            name=asset_data['name'],
            type=asset_data['type'],
            major_pair=asset_data.get('major_pair', False),
            description=asset_data.get('description', '')
        )
    
    def get_pillar_info(self, pillar_name: str) -> Optional[PillarInfo]:
        """Get information about a pillar"""
        pillar_data = self.config['pillars'].get(pillar_name)
        if not pillar_data:
            return None
        
        return PillarInfo(
            name=pillar_data['name'],
            description=pillar_data['description'],
            weight=pillar_data.get('weight', 1.0),
            indicators=pillar_data.get('indicators', [])
        )
    
    def get_mapping(self, series_id: str) -> Optional[MappingInfo]:
        """Get complete mapping information for a series ID"""
        mapping_data = self.config['mappings'].get(series_id)
        if not mapping_data:
            return None
        
        return MappingInfo(
            series_id=series_id,
            asset=mapping_data['asset'],
            pillar=mapping_data['pillar'],
            key=mapping_data['key'],
            weight=mapping_data.get('weight', 1.0),
            frequency=mapping_data.get('frequency', 'monthly'),
            description=mapping_data.get('description', ''),
            impact=mapping_data.get('impact', 'medium')
        )
    
    def get_supported_assets(self) -> List[str]:
        """Get list of all supported asset symbols"""
        return list(self.config['assets'].keys())
    
    def get_supported_pillars(self) -> List[str]:
        """Get list of all supported pillars"""
        return list(self.config['pillars'].keys())
    
    def get_series_for_asset(self, asset_symbol: str) -> List[str]:
        """Get all series IDs that map to a specific asset"""
        series_ids = []
        for series_id, mapping_data in self.config['mappings'].items():
            if mapping_data['asset'] == asset_symbol:
                series_ids.append(series_id)
        return series_ids
    
    def get_series_for_pillar(self, pillar_name: str) -> List[str]:
        """Get all series IDs that belong to a specific pillar"""
        series_ids = []
        for series_id, mapping_data in self.config['mappings'].items():
            if mapping_data['pillar'] == pillar_name:
                series_ids.append(series_id)
        return series_ids
    
    def get_impact_multiplier(self, impact: str) -> float:
        """Get impact multiplier for scoring"""
        multipliers = self.config['scoring_rules']['impact_multipliers']
        return multipliers.get(impact, 1.0)
    
    def get_frequency_decay(self, frequency: str) -> int:
        """Get decay half-life in days for frequency"""
        decay_rules = self.config['scoring_rules']['frequency_decay']
        return decay_rules.get(frequency, 30)
    
    def get_pillar_weight(self, pillar_name: str) -> float:
        """Get weight for pillar in final score calculation"""
        weights = self.config['scoring_rules']['pillar_weights']
        return weights.get(pillar_name, 1.0)
    
    def validate_series_id(self, series_id: str) -> bool:
        """Check if a series ID is supported"""
        return series_id in self.config['mappings']
    
    def validate_asset(self, asset_symbol: str) -> bool:
        """Check if an asset is supported"""
        return asset_symbol in self.config['assets']
    
    def get_mapping_summary(self) -> Dict[str, Any]:
        """Get summary of all mappings"""
        summary = {
            'total_series': len(self.config['mappings']),
            'total_assets': len(self.config['assets']),
            'total_pillars': len(self.config['pillars']),
            'assets_by_type': {},
            'series_by_pillar': {},
            'series_by_asset': {}
        }
        
        # Count assets by type
        for asset_data in self.config['assets'].values():
            asset_type = asset_data['type']
            summary['assets_by_type'][asset_type] = summary['assets_by_type'].get(asset_type, 0) + 1
        
        # Count series by pillar and asset
        for series_id, mapping_data in self.config['mappings'].items():
            pillar = mapping_data['pillar']
            asset = mapping_data['asset']
            
            summary['series_by_pillar'][pillar] = summary['series_by_pillar'].get(pillar, 0) + 1
            summary['series_by_asset'][asset] = summary['series_by_asset'].get(asset, 0) + 1
        
        return summary
    
    def export_backend_weights(self) -> Dict[str, Any]:
        """Export weights configuration for backend scoring system"""
        weights = {
            'version': self.config.get('version', '1.0.0'),
            'pillars': {}
        }
        
        # Group mappings by pillar and asset
        for pillar_name in self.config['pillars']:
            weights['pillars'][pillar_name] = {'components': {}}
        
        for series_id, mapping_data in self.config['mappings'].items():
            pillar = mapping_data['pillar']
            key = mapping_data['key']
            weight = mapping_data['weight']
            
            if pillar in weights['pillars']:
                weights['pillars'][pillar]['components'][key] = weight
        
        return weights
    
    def update_backend_weights_file(self, output_path: str = "backend-scraper/core/scoring/weights.yaml") -> None:
        """Update the backend weights.yaml file with current mappings"""
        weights = self.export_backend_weights()
        
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            yaml.dump(weights, f, default_flow_style=False, sort_keys=False)
        
        logger.info(f"Updated backend weights file: {output_file}")

def main():
    """CLI interface for asset mapping system"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Asset Mapping System CLI")
    parser.add_argument("--config", default="asset_mapping_config.yaml", help="Config file path")
    parser.add_argument("--summary", action="store_true", help="Show mapping summary")
    parser.add_argument("--validate", help="Validate specific series ID")
    parser.add_argument("--asset-info", help="Show info for specific asset")
    parser.add_argument("--update-backend", action="store_true", help="Update backend weights file")
    
    args = parser.parse_args()
    
    # Initialize system
    try:
        mapper = AssetMappingSystem(args.config)
    except Exception as e:
        print(f"Error loading config: {e}")
        return 1
    
    # Handle commands
    if args.summary:
        summary = mapper.get_mapping_summary()
        print("Asset Mapping Summary:")
        print(f"  Total Series: {summary['total_series']}")
        print(f"  Total Assets: {summary['total_assets']}")
        print(f"  Total Pillars: {summary['total_pillars']}")
        print(f"  Assets by Type: {summary['assets_by_type']}")
        print(f"  Series by Pillar: {summary['series_by_pillar']}")
        print(f"  Series by Asset: {summary['series_by_asset']}")
    
    elif args.validate:
        if mapper.validate_series_id(args.validate):
            mapping = mapper.get_mapping(args.validate)
            print(f"✅ {args.validate} is valid")
            print(f"   Asset: {mapping.asset}")
            print(f"   Pillar: {mapping.pillar}")
            print(f"   Key: {mapping.key}")
            print(f"   Weight: {mapping.weight}")
        else:
            print(f"❌ {args.validate} is not supported")
    
    elif args.asset_info:
        asset_info = mapper.get_asset_info(args.asset_info)
        if asset_info:
            print(f"Asset: {asset_info.symbol}")
            print(f"  Name: {asset_info.name}")
            print(f"  Type: {asset_info.type}")
            print(f"  Major Pair: {asset_info.major_pair}")
            print(f"  Description: {asset_info.description}")
            
            series_list = mapper.get_series_for_asset(args.asset_info)
            print(f"  Related Series ({len(series_list)}): {', '.join(series_list)}")
        else:
            print(f"❌ Asset {args.asset_info} not found")
    
    elif args.update_backend:
        mapper.update_backend_weights_file()
        print("✅ Backend weights file updated")
    
    else:
        print("Use --help for available commands")
    
    return 0

if __name__ == "__main__":
    exit(main())
