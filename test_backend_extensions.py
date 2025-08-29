#!/usr/bin/env python3
"""
Test Script for Backend Extensions
==================================

Tests the new backend functionality including:
- Batch heatmap endpoint
- CORS configuration
- Asset management endpoints
- Score normalization

Usage:
    python test_backend_extensions.py [--base-url http://localhost:8000]
"""

import requests
import json
import argparse
from typing import Dict, List, Any

class BackendTester:
    """Test suite for backend extensions"""
    
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        
    def test_cors_headers(self) -> bool:
        """Test CORS configuration"""
        print("🔍 Testing CORS headers...")
        
        try:
            # Test preflight request
            response = self.session.options(
                f"{self.base_url}/heatmap",
                headers={
                    "Origin": "http://localhost:3000",
                    "Access-Control-Request-Method": "GET",
                    "Access-Control-Request-Headers": "Content-Type"
                }
            )
            
            cors_headers = {
                "Access-Control-Allow-Origin": response.headers.get("Access-Control-Allow-Origin"),
                "Access-Control-Allow-Methods": response.headers.get("Access-Control-Allow-Methods"),
                "Access-Control-Allow-Headers": response.headers.get("Access-Control-Allow-Headers"),
            }
            
            print(f"   CORS Headers: {cors_headers}")
            
            # Check if CORS is properly configured
            if cors_headers["Access-Control-Allow-Origin"]:
                print("   ✅ CORS is configured")
                return True
            else:
                print("   ❌ CORS not properly configured")
                return False
                
        except Exception as e:
            print(f"   ❌ CORS test failed: {e}")
            return False
    
    def test_root_endpoint(self) -> bool:
        """Test root endpoint with API information"""
        print("🔍 Testing root endpoint...")
        
        try:
            response = self.session.get(f"{self.base_url}/")
            response.raise_for_status()
            
            data = response.json()
            print(f"   API Name: {data.get('name')}")
            print(f"   Version: {data.get('version')}")
            print(f"   Supported Assets: {data.get('supported_assets')}")
            
            if "endpoints" in data and "supported_assets" in data:
                print("   ✅ Root endpoint working correctly")
                return True
            else:
                print("   ❌ Root endpoint missing required fields")
                return False
                
        except Exception as e:
            print(f"   ❌ Root endpoint test failed: {e}")
            return False
    
    def test_health_endpoint(self) -> bool:
        """Test health endpoint"""
        print("🔍 Testing health endpoint...")
        
        try:
            response = self.session.get(f"{self.base_url}/health")
            response.raise_for_status()
            
            data = response.json()
            if data.get("status") == "ok":
                print("   ✅ Health endpoint working")
                return True
            else:
                print(f"   ❌ Health endpoint returned: {data}")
                return False
                
        except Exception as e:
            print(f"   ❌ Health endpoint test failed: {e}")
            return False
    
    def test_assets_list(self) -> bool:
        """Test assets list endpoint"""
        print("🔍 Testing assets list endpoint...")
        
        try:
            response = self.session.get(f"{self.base_url}/assets/")
            response.raise_for_status()
            
            data = response.json()
            print(f"   Found {len(data)} assets")
            
            if isinstance(data, list):
                for asset in data[:3]:  # Show first 3 assets
                    print(f"   - {asset.get('asset', {}).get('symbol')}: {asset.get('indicator_count')} indicators")
                print("   ✅ Assets list endpoint working")
                return True
            else:
                print("   ❌ Assets list endpoint returned invalid format")
                return False
                
        except Exception as e:
            print(f"   ❌ Assets list test failed: {e}")
            return False
    
    def test_single_heatmap(self, asset: str = "USD") -> bool:
        """Test single asset heatmap endpoint"""
        print(f"🔍 Testing single heatmap for {asset}...")
        
        try:
            response = self.session.get(f"{self.base_url}/heatmap", params={"asset": asset})
            
            if response.status_code == 404:
                print(f"   ⚠️  Asset {asset} not found (expected if no data)")
                return True
            
            response.raise_for_status()
            data = response.json()
            
            print(f"   Asset: {data.get('asset')}")
            print(f"   Score: {data.get('score')}")
            print(f"   Scale: {data.get('scale')}")
            print(f"   Pillars: {len(data.get('pillars', []))}")
            
            if "asset" in data and "score" in data and "scale" in data:
                print("   ✅ Single heatmap endpoint working")
                return True
            else:
                print("   ❌ Single heatmap endpoint missing required fields")
                return False
                
        except Exception as e:
            print(f"   ❌ Single heatmap test failed: {e}")
            return False
    
    def test_batch_heatmap(self, assets: List[str] = ["USD", "EUR", "GBP"]) -> bool:
        """Test batch heatmap endpoint"""
        print(f"🔍 Testing batch heatmap for {assets}...")
        
        try:
            assets_param = ",".join(assets)
            response = self.session.get(f"{self.base_url}/heatmap/batch", params={"assets": assets_param})
            response.raise_for_status()
            
            data = response.json()
            
            print(f"   Requested: {data.get('requested_assets')}")
            print(f"   Returned: {len(data.get('heatmaps', []))} heatmaps")
            
            if data.get('errors'):
                print(f"   Errors: {data.get('errors')}")
            
            # Check structure
            if "heatmaps" in data and "requested_assets" in data:
                print("   ✅ Batch heatmap endpoint working")
                
                # Test score normalization
                for heatmap in data.get('heatmaps', []):
                    score = heatmap.get('score', 0)
                    scale = heatmap.get('scale', [])
                    if scale == [-2, 2]:
                        print(f"   ✅ Score normalization working: {heatmap.get('asset')} score={score}")
                    else:
                        print(f"   ⚠️  Unexpected scale for {heatmap.get('asset')}: {scale}")
                
                return True
            else:
                print("   ❌ Batch heatmap endpoint missing required fields")
                return False
                
        except Exception as e:
            print(f"   ❌ Batch heatmap test failed: {e}")
            return False
    
    def test_invalid_requests(self) -> bool:
        """Test error handling for invalid requests"""
        print("🔍 Testing error handling...")
        
        tests_passed = 0
        total_tests = 3
        
        # Test 1: Invalid asset
        try:
            response = self.session.get(f"{self.base_url}/heatmap", params={"asset": "INVALID"})
            if response.status_code == 404:
                print("   ✅ Invalid asset returns 404")
                tests_passed += 1
            else:
                print(f"   ❌ Invalid asset returned {response.status_code}")
        except Exception as e:
            print(f"   ❌ Invalid asset test failed: {e}")
        
        # Test 2: Empty batch request
        try:
            response = self.session.get(f"{self.base_url}/heatmap/batch", params={"assets": ""})
            if response.status_code == 400:
                print("   ✅ Empty batch request returns 400")
                tests_passed += 1
            else:
                print(f"   ❌ Empty batch request returned {response.status_code}")
        except Exception as e:
            print(f"   ❌ Empty batch test failed: {e}")
        
        # Test 3: Too many assets
        try:
            many_assets = ",".join([f"ASSET{i}" for i in range(25)])
            response = self.session.get(f"{self.base_url}/heatmap/batch", params={"assets": many_assets})
            if response.status_code == 400:
                print("   ✅ Too many assets returns 400")
                tests_passed += 1
            else:
                print(f"   ❌ Too many assets returned {response.status_code}")
        except Exception as e:
            print(f"   ❌ Too many assets test failed: {e}")
        
        return tests_passed == total_tests
    
    def run_all_tests(self) -> Dict[str, bool]:
        """Run all tests and return results"""
        print(f"🚀 Starting Backend Extension Tests for {self.base_url}")
        print("=" * 60)
        
        results = {
            "cors_headers": self.test_cors_headers(),
            "root_endpoint": self.test_root_endpoint(),
            "health_endpoint": self.test_health_endpoint(),
            "assets_list": self.test_assets_list(),
            "single_heatmap": self.test_single_heatmap(),
            "batch_heatmap": self.test_batch_heatmap(),
            "error_handling": self.test_invalid_requests(),
        }
        
        print("\n" + "=" * 60)
        print("📊 Test Results Summary:")
        
        passed = sum(results.values())
        total = len(results)
        
        for test_name, result in results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"   {test_name}: {status}")
        
        print(f"\nOverall: {passed}/{total} tests passed")
        
        if passed == total:
            print("🎉 All tests passed! Backend extensions are working correctly.")
        else:
            print("⚠️  Some tests failed. Check the backend configuration.")
        
        return results

def main():
    parser = argparse.ArgumentParser(description="Test backend extensions")
    parser.add_argument("--base-url", default="http://localhost:8000", help="Backend base URL")
    
    args = parser.parse_args()
    
    tester = BackendTester(args.base_url)
    results = tester.run_all_tests()
    
    # Exit with error code if any tests failed
    if not all(results.values()):
        exit(1)

if __name__ == "__main__":
    main()
