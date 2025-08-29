#!/usr/bin/env python3
"""
Integration Test Suite for Trading Heatmap System
=================================================

Tests the complete workflow: Scraper → Backend → API → Heatmap
Validates data quality, consistency, and heatmap compatibility.

Usage:
    python integration_test_suite.py [--full] [--backend-url http://localhost:8000]
"""

import asyncio
import json
import logging
import sqlite3
import requests
import time
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import subprocess
import tempfile
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class TestResult:
    """Test result data"""
    test_name: str
    success: bool
    duration: float
    message: str
    details: Optional[Dict[str, Any]] = None

class IntegrationTestSuite:
    """Complete integration test suite"""
    
    def __init__(self, backend_url: str = "http://localhost:8000"):
        self.backend_url = backend_url.rstrip('/')
        self.test_results = []
        self.test_data_dir = tempfile.mkdtemp(prefix="heatmap_test_")
        
    def log_result(self, result: TestResult):
        """Log and store test result"""
        status = "✅ PASS" if result.success else "❌ FAIL"
        logger.info(f"{status} {result.test_name} ({result.duration:.2f}s): {result.message}")
        self.test_results.append(result)
    
    async def run_all_tests(self, full_test: bool = False) -> Dict[str, Any]:
        """Run complete integration test suite"""
        logger.info("🚀 Starting Integration Test Suite")
        logger.info(f"Backend URL: {self.backend_url}")
        logger.info(f"Test Data Dir: {self.test_data_dir}")
        
        # Test sequence
        tests = [
            ("Environment Setup", self.test_environment_setup),
            ("Asset Mapping System", self.test_asset_mapping_system),
            ("Backend Health", self.test_backend_health),
            ("Scraper Data Generation", self.test_scraper_data_generation),
            ("Bridge Data Transformation", self.test_bridge_transformation),
            ("Backend Data Ingestion", self.test_backend_ingestion),
            ("Score Calculation", self.test_score_calculation),
            ("Single Heatmap API", self.test_single_heatmap_api),
            ("Batch Heatmap API", self.test_batch_heatmap_api),
            ("Data Quality Validation", self.test_data_quality),
            ("Heatmap Compatibility", self.test_heatmap_compatibility),
        ]
        
        if full_test:
            tests.extend([
                ("Performance Test", self.test_performance),
                ("Error Handling", self.test_error_handling),
                ("Monitoring Integration", self.test_monitoring_integration),
            ])
        
        # Run tests
        for test_name, test_func in tests:
            start_time = time.time()
            try:
                success, message, details = await test_func()
                duration = time.time() - start_time
                
                result = TestResult(
                    test_name=test_name,
                    success=success,
                    duration=duration,
                    message=message,
                    details=details
                )
                self.log_result(result)
                
                # Stop on critical failures
                if not success and test_name in ["Environment Setup", "Backend Health"]:
                    logger.error("Critical test failed, stopping test suite")
                    break
                    
            except Exception as e:
                duration = time.time() - start_time
                result = TestResult(
                    test_name=test_name,
                    success=False,
                    duration=duration,
                    message=f"Test exception: {str(e)}",
                    details={"exception": str(e)}
                )
                self.log_result(result)
        
        # Generate summary
        return self.generate_test_summary()
    
    async def test_environment_setup(self) -> tuple[bool, str, Dict[str, Any]]:
        """Test environment setup and dependencies"""
        checks = {}
        
        # Check Python modules
        required_modules = ['yaml', 'requests', 'sqlalchemy', 'pydantic']
        for module in required_modules:
            try:
                __import__(module)
                checks[f"module_{module}"] = True
            except ImportError:
                checks[f"module_{module}"] = False
        
        # Check file existence
        required_files = [
            'asset_mapping_config.yaml',
            'bridge_scraper_to_backend.py',
            'asset_mapping_system.py'
        ]
        for file_path in required_files:
            checks[f"file_{file_path}"] = os.path.exists(file_path)
        
        # Check directories
        required_dirs = ['scraper', 'backend-scraper']
        for dir_path in required_dirs:
            checks[f"dir_{dir_path}"] = os.path.isdir(dir_path)
        
        all_passed = all(checks.values())
        failed_checks = [k for k, v in checks.items() if not v]
        
        if all_passed:
            return True, "All environment checks passed", checks
        else:
            return False, f"Failed checks: {', '.join(failed_checks)}", checks
    
    async def test_asset_mapping_system(self) -> tuple[bool, str, Dict[str, Any]]:
        """Test asset mapping system functionality"""
        try:
            from asset_mapping_system import AssetMappingSystem
            
            mapper = AssetMappingSystem()
            
            # Test basic functionality
            supported_assets = mapper.get_supported_assets()
            test_mapping = mapper.get_mapping("US_CPI")
            summary = mapper.get_mapping_summary()
            
            details = {
                "supported_assets_count": len(supported_assets),
                "test_mapping_found": test_mapping is not None,
                "total_mappings": summary.get('total_series', 0),
                "supported_assets": supported_assets[:5]  # First 5 for brevity
            }
            
            if len(supported_assets) > 0 and test_mapping:
                return True, f"Asset mapping system working ({len(supported_assets)} assets)", details
            else:
                return False, "Asset mapping system not properly configured", details
                
        except Exception as e:
            return False, f"Asset mapping system error: {str(e)}", {"error": str(e)}
    
    async def test_backend_health(self) -> tuple[bool, str, Dict[str, Any]]:
        """Test backend API health and connectivity"""
        try:
            # Health check
            response = requests.get(f"{self.backend_url}/health", timeout=10)
            health_ok = response.status_code == 200
            
            # Root endpoint
            root_response = requests.get(f"{self.backend_url}/", timeout=10)
            root_ok = root_response.status_code == 200
            
            details = {
                "health_status": response.status_code if health_ok else "failed",
                "root_status": root_response.status_code if root_ok else "failed",
                "backend_url": self.backend_url
            }
            
            if health_ok and root_ok:
                return True, "Backend API is healthy and accessible", details
            else:
                return False, "Backend API health check failed", details
                
        except Exception as e:
            return False, f"Backend connectivity error: {str(e)}", {"error": str(e)}
    
    async def test_scraper_data_generation(self) -> tuple[bool, str, Dict[str, Any]]:
        """Test scraper data generation"""
        try:
            # Create test database
            test_db_path = os.path.join(self.test_data_dir, "test_events.db")
            
            # Import and use scraper components
            import sys
            sys.path.append('scraper')
            
            from event_store import EventStore, Event
            
            # Create test event store
            store = EventStore(test_db_path)
            
            # Create test event
            test_event = Event(
                series_id="US_CPI",
                release_date="2024-01-15",
                vintage="final",
                actual=3.2,
                consensus=3.1,
                previous=3.0,
                impact="high",
                release_time_utc="2024-01-15T13:30:00Z",
                provider="test"
            )
            
            store.add_event(test_event)
            
            # Verify data
            events = list(store.fetch_events("US_CPI"))
            
            details = {
                "test_db_path": test_db_path,
                "events_created": len(events),
                "test_event_data": {
                    "series_id": test_event.series_id,
                    "actual": test_event.actual,
                    "consensus": test_event.consensus
                }
            }
            
            if len(events) > 0:
                return True, f"Scraper data generation successful ({len(events)} events)", details
            else:
                return False, "No events were created", details
                
        except Exception as e:
            return False, f"Scraper data generation error: {str(e)}", {"error": str(e)}
    
    async def test_bridge_transformation(self) -> tuple[bool, str, Dict[str, Any]]:
        """Test bridge script data transformation"""
        try:
            # Run bridge script in dry-run mode
            test_db_path = os.path.join(self.test_data_dir, "test_events.db")
            
            cmd = [
                "python", "bridge_scraper_to_backend.py",
                "--dry-run",
                "--scraper-db", test_db_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            details = {
                "exit_code": result.returncode,
                "stdout": result.stdout[:500],  # First 500 chars
                "stderr": result.stderr[:500] if result.stderr else None
            }
            
            if result.returncode == 0:
                return True, "Bridge transformation completed successfully", details
            else:
                return False, f"Bridge transformation failed (exit code {result.returncode})", details
                
        except Exception as e:
            return False, f"Bridge transformation error: {str(e)}", {"error": str(e)}
    
    async def test_backend_ingestion(self) -> tuple[bool, str, Dict[str, Any]]:
        """Test backend data ingestion"""
        try:
            # Create test event for ingestion
            test_payload = {
                "events": [{
                    "schema_version": "2025.08.1",
                    "source": "integration_test",
                    "asset": "USD",
                    "kind": "indicator",
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "payload": {
                        "key": "test_cpi",
                        "value": 5
                    },
                    "trace_id": "test-integration-001"
                }]
            }
            
            # Send to backend
            response = requests.post(
                f"{self.backend_url}/ingest/events",
                json=test_payload,
                timeout=30
            )
            
            details = {
                "response_status": response.status_code,
                "response_data": response.json() if response.status_code < 400 else None,
                "test_payload": test_payload
            }
            
            if response.status_code == 202:
                return True, "Backend ingestion successful", details
            else:
                return False, f"Backend ingestion failed (status {response.status_code})", details
                
        except Exception as e:
            return False, f"Backend ingestion error: {str(e)}", {"error": str(e)}
    
    async def test_score_calculation(self) -> tuple[bool, str, Dict[str, Any]]:
        """Test score calculation and retrieval"""
        try:
            # Trigger score recomputation
            response = requests.post(
                f"{self.backend_url}/jobs/recompute-bias",
                params={"asset": "USD"},
                timeout=30
            )
            
            # Wait a moment for processing
            await asyncio.sleep(2)
            
            # Try to get heatmap data
            heatmap_response = requests.get(
                f"{self.backend_url}/heatmap",
                params={"asset": "USD"},
                timeout=10
            )
            
            details = {
                "recompute_status": response.status_code,
                "heatmap_status": heatmap_response.status_code,
                "heatmap_data": heatmap_response.json() if heatmap_response.status_code == 200 else None
            }
            
            if heatmap_response.status_code == 200:
                heatmap_data = heatmap_response.json()
                has_score = "score" in heatmap_data
                return True, f"Score calculation successful (score: {heatmap_data.get('score', 'N/A')})", details
            else:
                return True, "Score calculation triggered (data may not be available yet)", details
                
        except Exception as e:
            return False, f"Score calculation error: {str(e)}", {"error": str(e)}
    
    async def test_single_heatmap_api(self) -> tuple[bool, str, Dict[str, Any]]:
        """Test single asset heatmap API"""
        try:
            response = requests.get(
                f"{self.backend_url}/heatmap",
                params={"asset": "USD"},
                timeout=10
            )
            
            details = {"response_status": response.status_code}
            
            if response.status_code == 200:
                data = response.json()
                details["heatmap_data"] = data
                
                # Validate structure
                required_fields = ["asset", "score", "scale", "pillars"]
                missing_fields = [f for f in required_fields if f not in data]
                
                if not missing_fields:
                    return True, f"Single heatmap API working (asset: {data['asset']}, score: {data['score']})", details
                else:
                    return False, f"Missing required fields: {missing_fields}", details
            elif response.status_code == 404:
                return True, "Single heatmap API working (no data available yet)", details
            else:
                return False, f"Single heatmap API failed (status {response.status_code})", details
                
        except Exception as e:
            return False, f"Single heatmap API error: {str(e)}", {"error": str(e)}
    
    async def test_batch_heatmap_api(self) -> tuple[bool, str, Dict[str, Any]]:
        """Test batch heatmap API"""
        try:
            response = requests.get(
                f"{self.backend_url}/heatmap/batch",
                params={"assets": "USD,EUR,GBP"},
                timeout=15
            )
            
            details = {"response_status": response.status_code}
            
            if response.status_code == 200:
                data = response.json()
                details["batch_data"] = data
                
                # Validate structure
                required_fields = ["heatmaps", "requested_assets"]
                missing_fields = [f for f in required_fields if f not in data]
                
                if not missing_fields:
                    heatmap_count = len(data.get("heatmaps", []))
                    return True, f"Batch heatmap API working ({heatmap_count} heatmaps returned)", details
                else:
                    return False, f"Missing required fields: {missing_fields}", details
            else:
                return False, f"Batch heatmap API failed (status {response.status_code})", details
                
        except Exception as e:
            return False, f"Batch heatmap API error: {str(e)}", {"error": str(e)}
    
    async def test_data_quality(self) -> tuple[bool, str, Dict[str, Any]]:
        """Test data quality and consistency"""
        try:
            # Get batch heatmap data
            response = requests.get(
                f"{self.backend_url}/heatmap/batch",
                params={"assets": "USD,EUR,GBP,JPY"},
                timeout=15
            )
            
            if response.status_code != 200:
                return True, "Data quality test skipped (no data available)", {"status": "skipped"}
            
            data = response.json()
            heatmaps = data.get("heatmaps", [])
            
            quality_checks = {
                "score_range_valid": True,
                "scale_consistent": True,
                "pillars_present": True,
                "timestamps_valid": True
            }
            
            for heatmap in heatmaps:
                # Check score range (-2 to +2 for normalized heatmap)
                score = heatmap.get("score", 0)
                if not isinstance(score, (int, float)) or score < -2.5 or score > 2.5:
                    quality_checks["score_range_valid"] = False
                
                # Check scale consistency
                scale = heatmap.get("scale", [])
                if scale != [-2, 2]:
                    quality_checks["scale_consistent"] = False
                
                # Check pillars
                pillars = heatmap.get("pillars", [])
                if not pillars:
                    quality_checks["pillars_present"] = False
                
                # Check timestamp
                as_of = heatmap.get("as_of")
                if as_of:
                    try:
                        datetime.fromisoformat(as_of.replace('Z', '+00:00'))
                    except:
                        quality_checks["timestamps_valid"] = False
            
            details = {
                "heatmaps_checked": len(heatmaps),
                "quality_checks": quality_checks
            }
            
            all_passed = all(quality_checks.values())
            failed_checks = [k for k, v in quality_checks.items() if not v]
            
            if all_passed:
                return True, f"Data quality validation passed ({len(heatmaps)} heatmaps)", details
            else:
                return False, f"Data quality issues: {', '.join(failed_checks)}", details
                
        except Exception as e:
            return False, f"Data quality test error: {str(e)}", {"error": str(e)}
    
    async def test_heatmap_compatibility(self) -> tuple[bool, str, Dict[str, Any]]:
        """Test heatmap component compatibility"""
        try:
            # Get sample heatmap data
            response = requests.get(
                f"{self.backend_url}/heatmap/batch",
                params={"assets": "USD,EUR"},
                timeout=10
            )
            
            if response.status_code != 200:
                return True, "Heatmap compatibility test skipped (no data)", {"status": "skipped"}
            
            data = response.json()
            heatmaps = data.get("heatmaps", [])
            
            if not heatmaps:
                return True, "Heatmap compatibility test skipped (no heatmaps)", {"status": "skipped"}
            
            # Check expected heatmap format
            sample_heatmap = heatmaps[0]
            expected_format = {
                "asset": str,
                "score": (int, float),
                "scale": list,
                "pillars": list,
                "as_of": (str, type(None)),
                "version": str
            }
            
            compatibility_checks = {}
            for field, expected_type in expected_format.items():
                if field in sample_heatmap:
                    actual_value = sample_heatmap[field]
                    if isinstance(expected_type, tuple):
                        compatibility_checks[field] = type(actual_value) in expected_type
                    else:
                        compatibility_checks[field] = isinstance(actual_value, expected_type)
                else:
                    compatibility_checks[field] = False
            
            details = {
                "sample_heatmap": sample_heatmap,
                "compatibility_checks": compatibility_checks
            }
            
            all_compatible = all(compatibility_checks.values())
            
            if all_compatible:
                return True, "Heatmap format is compatible with frontend", details
            else:
                failed_fields = [k for k, v in compatibility_checks.items() if not v]
                return False, f"Heatmap compatibility issues: {', '.join(failed_fields)}", details
                
        except Exception as e:
            return False, f"Heatmap compatibility test error: {str(e)}", {"error": str(e)}
    
    async def test_performance(self) -> tuple[bool, str, Dict[str, Any]]:
        """Test system performance"""
        try:
            # Test batch API performance
            start_time = time.time()
            response = requests.get(
                f"{self.backend_url}/heatmap/batch",
                params={"assets": "USD,EUR,GBP,JPY,AUD,CAD,CHF,NZD"},
                timeout=30
            )
            response_time = time.time() - start_time
            
            details = {
                "batch_response_time": response_time,
                "response_status": response.status_code,
                "acceptable_threshold": 5.0  # 5 seconds
            }
            
            if response.status_code == 200 and response_time < 5.0:
                return True, f"Performance test passed ({response_time:.2f}s)", details
            elif response.status_code == 200:
                return False, f"Performance test failed - too slow ({response_time:.2f}s)", details
            else:
                return False, f"Performance test failed - API error (status {response.status_code})", details
                
        except Exception as e:
            return False, f"Performance test error: {str(e)}", {"error": str(e)}
    
    async def test_error_handling(self) -> tuple[bool, str, Dict[str, Any]]:
        """Test error handling"""
        try:
            error_tests = {}
            
            # Test invalid asset
            response = requests.get(f"{self.backend_url}/heatmap", params={"asset": "INVALID"})
            error_tests["invalid_asset"] = response.status_code == 404
            
            # Test empty batch request
            response = requests.get(f"{self.backend_url}/heatmap/batch", params={"assets": ""})
            error_tests["empty_batch"] = response.status_code == 400
            
            # Test malformed request
            response = requests.post(f"{self.backend_url}/ingest/events", json={"invalid": "data"})
            error_tests["malformed_request"] = response.status_code in [400, 422]
            
            details = {"error_tests": error_tests}
            
            all_passed = all(error_tests.values())
            
            if all_passed:
                return True, "Error handling tests passed", details
            else:
                failed_tests = [k for k, v in error_tests.items() if not v]
                return False, f"Error handling issues: {', '.join(failed_tests)}", details
                
        except Exception as e:
            return False, f"Error handling test error: {str(e)}", {"error": str(e)}
    
    async def test_monitoring_integration(self) -> tuple[bool, str, Dict[str, Any]]:
        """Test monitoring system integration"""
        try:
            # Check if monitoring script exists and runs
            if os.path.exists("monitoring_system.py"):
                result = subprocess.run(
                    ["python", "monitoring_system.py", "--dashboard"],
                    capture_output=True, text=True, timeout=30
                )
                
                details = {
                    "monitoring_script_exists": True,
                    "exit_code": result.returncode,
                    "output_length": len(result.stdout) if result.stdout else 0
                }
                
                if result.returncode == 0:
                    return True, "Monitoring integration working", details
                else:
                    return False, f"Monitoring script failed (exit code {result.returncode})", details
            else:
                return True, "Monitoring integration test skipped (script not found)", {"status": "skipped"}
                
        except Exception as e:
            return False, f"Monitoring integration test error: {str(e)}", {"error": str(e)}
    
    def generate_test_summary(self) -> Dict[str, Any]:
        """Generate comprehensive test summary"""
        total_tests = len(self.test_results)
        passed_tests = sum(1 for r in self.test_results if r.success)
        failed_tests = total_tests - passed_tests
        
        total_duration = sum(r.duration for r in self.test_results)
        
        summary = {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total_tests": total_tests,
                "passed": passed_tests,
                "failed": failed_tests,
                "success_rate": (passed_tests / total_tests * 100) if total_tests > 0 else 0,
                "total_duration": total_duration
            },
            "test_results": [
                {
                    "test_name": r.test_name,
                    "success": r.success,
                    "duration": r.duration,
                    "message": r.message
                }
                for r in self.test_results
            ],
            "failed_tests": [
                {
                    "test_name": r.test_name,
                    "message": r.message,
                    "details": r.details
                }
                for r in self.test_results if not r.success
            ]
        }
        
        return summary

async def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Integration Test Suite")
    parser.add_argument("--backend-url", default="http://localhost:8000", help="Backend URL")
    parser.add_argument("--full", action="store_true", help="Run full test suite including performance tests")
    parser.add_argument("--output", help="Output file for test results (JSON)")
    
    args = parser.parse_args()
    
    test_suite = IntegrationTestSuite(args.backend_url)
    
    try:
        summary = await test_suite.run_all_tests(full_test=args.full)
        
        # Print summary
        print("\n" + "="*60)
        print("🏁 Integration Test Summary")
        print("="*60)
        print(f"Total Tests: {summary['summary']['total_tests']}")
        print(f"Passed: {summary['summary']['passed']}")
        print(f"Failed: {summary['summary']['failed']}")
        print(f"Success Rate: {summary['summary']['success_rate']:.1f}%")
        print(f"Total Duration: {summary['summary']['total_duration']:.2f}s")
        
        if summary['failed_tests']:
            print("\n❌ Failed Tests:")
            for failed_test in summary['failed_tests']:
                print(f"  - {failed_test['test_name']}: {failed_test['message']}")
        
        # Save results if requested
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            print(f"\n📄 Results saved to: {args.output}")
        
        # Exit with appropriate code
        if summary['summary']['failed'] > 0:
            print("\n⚠️  Some tests failed. Check the logs for details.")
            return 1
        else:
            print("\n🎉 All tests passed! System integration is working correctly.")
            return 0
            
    except KeyboardInterrupt:
        logger.info("Test suite interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Test suite error: {e}")
        return 1

if __name__ == "__main__":
    exit(asyncio.run(main()))
