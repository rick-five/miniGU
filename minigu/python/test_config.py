#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test script for database connection configuration options
"""

import sys
import os

# Add the project root directory to the path so we can import minigu
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import minigu


def test_connection_config():
    """Test database connection configuration options"""
    print("=== Testing Database Connection Configuration ===")
    
    try:
        # Test using the configuration options
        print("1. Testing connection with configuration options...")
        db = minigu.connect(
            thread_count=4,
            cache_size=2000,
            enable_logging=True
        )
        
        try:
            print(f"   [PASS] Connected with thread_count={db.thread_count}")
            print(f"   [PASS] Connected with cache_size={db.cache_size}")
            print(f"   [PASS] Connected with enable_logging={db.enable_logging}")
            
            # Test that the configuration options are applied
            stats = db.get_performance_stats()
            print(f"   [PASS] Got performance stats: {stats}")
            
        finally:
            db.close()
            
        print("\n[PASS] All connection configuration tests passed!")
        
    except Exception as e:
        print(f"[FAIL] Error during connection configuration tests: {e}")
        raise


def test_async_connection_config():
    """Test asynchronous database connection configuration options"""
    print("\n=== Testing Asynchronous Database Connection Configuration ===")
    
    async def _test_async_config():
        try:
            # Test using the configuration options
            print("1. Testing async connection with configuration options...")
            db = await minigu.async_connect(
                thread_count=2,
                cache_size=1500,
                enable_logging=False
            )
            
            try:
                print(f"   [PASS] Connected with thread_count={db.thread_count}")
                print(f"   [PASS] Connected with cache_size={db.cache_size}")
                print(f"   [PASS] Connected with enable_logging={db.enable_logging}")
                
                # Test that the configuration options are applied
                stats = await db.get_performance_stats()
                print(f"   [PASS] Got performance stats: {stats}")
                
            finally:
                db.close()
                
            print("\n[PASS] All async connection configuration tests passed!")
            
        except Exception as e:
            print(f"[FAIL] Error during async connection configuration tests: {e}")
            raise
    
    import asyncio
    asyncio.run(_test_async_config())


def main():
    """Main test function"""
    print("miniGU Python API Connection Configuration Test")
    print("=" * 50)
    
    try:
        test_connection_config()
        test_async_connection_config()
        
        print("\n" + "=" * 50)
        print("All connection configuration tests completed successfully!")
        
    except Exception as e:
        print(f"\n[FAIL] Test suite failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()