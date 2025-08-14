#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test script for performance optimization API
"""

import sys
import os

# Add the project root directory to the path so we can import minigu
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import minigu


def test_performance_api():
    """Test performance optimization API"""
    print("=== Testing Performance Optimization API ===")
    
    try:
        # Test using the performance API
        print("1. Testing performance API methods...")
        with minigu.connect() as db:
            # Test setting cache size
            db.set_cache_size(1000)
            print("   [PASS] Set cache size successfully")
            
            # Test setting thread count
            db.set_thread_count(4)
            print("   [PASS] Set thread count successfully")
            
            # Test enabling query logging
            db.enable_query_logging(True)
            print("   [PASS] Enabled query logging successfully")
            
            # Test getting performance stats
            stats = db.get_performance_stats()
            print(f"   [PASS] Got performance stats: {stats}")
            
        print("\n[PASS] All performance optimization API tests passed!")
        
    except Exception as e:
        print(f"[FAIL] Error during performance optimization API tests: {e}")
        raise


def main():
    """Main test function"""
    print("miniGU Python API Performance Optimization Test")
    print("=" * 50)
    
    try:
        test_performance_api()
        
        print("\n" + "=" * 50)
        print("All performance optimization API tests completed successfully!")
        
    except Exception as e:
        print(f"\n[FAIL] Test suite failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()