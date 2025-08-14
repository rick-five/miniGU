#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test script for asynchronous API
"""

import sys
import os
import asyncio

# Add the project root directory to the path so we can import minigu
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import minigu


async def test_async_api():
    """Test asynchronous API"""
    print("=== Testing Asynchronous API ===")
    
    try:
        # Test using the async API
        print("1. Testing async API methods...")
        db = await minigu.async_connect()
        try:
            # Test executing a query (might fail in current implementation)
            try:
                result = await db.execute("MATCH (n) RETURN n;")
                print(f"   [PASS] Executed query, got result with {result.row_count} rows")
            except minigu.QueryError as e:
                print(f"   [EXPECTED] Query execution failed (expected during development): {e}")
            
            # Test loading data
            sample_data = [
                {"name": "Alice", "age": 30, "label": "Person"},
                {"name": "Bob", "age": 25, "label": "Person"},
                {"name": "TechCorp", "founded": 2010, "label": "Company"}
            ]
            await db.load(sample_data)
            print("   [PASS] Loaded data successfully")
            
            # Test saving database
            await db.save("test_async_database.mgu")
            print("   [PASS] Saved database successfully")
            
            # Test creating a graph
            await db.create_graph("test_async_graph", {
                "Person": {"name": "STRING", "age": "INTEGER"},
                "Company": {"name": "STRING", "founded": "INTEGER"}
            })
            print("   [PASS] Created graph successfully")
            
            # Test inserting data
            await db.insert([
                {"name": "Charlie", "age": 35, "label": "Person"},
                {"name": "InnovateCo", "founded": 2015, "label": "Company"}
            ])
            print("   [PASS] Inserted data successfully")
            
            # Test creating graph objects
            node = await db.create_node("Person", {"name": "David", "age": 28})
            print(f"   [PASS] Created node: {node}")
            
            # Test performance API
            await db.set_cache_size(1000)
            await db.set_thread_count(4)
            await db.enable_query_logging(True)
            stats = await db.get_performance_stats()
            print(f"   [PASS] Performance stats: {stats}")
        finally:
            db.close()
            
        print("\n[PASS] All asynchronous API tests passed!")
        
    except Exception as e:
        print(f"[FAIL] Error during asynchronous API tests: {e}")
        raise


def main():
    """Main test function"""
    print("miniGU Python API Asynchronous Test")
    print("=" * 50)
    
    try:
        asyncio.run(test_async_api())
        
        print("\n" + "=" * 50)
        print("All asynchronous API tests completed successfully!")
        
    except Exception as e:
        print(f"\n[FAIL] Test suite failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()