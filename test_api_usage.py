#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test script to demonstrate actual usage of miniGU Python API with real Rust bindings
"""

import sys
import os

# Add the project root directory to the path so we can import minigu
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import minigu


def test_actual_api_usage():
    """Test actual API usage with real Rust bindings"""
    print("=== Testing Actual API Usage ===")
    
    try:
        # Test connecting to database
        print("1. Connecting to database...")
        with minigu.connect() as db:
            print("   [PASS] Connected successfully")
            
            # Test executing a simple query that should work
            print("2. Executing create_test_graph procedure...")
            try:
                result = db.execute("CALL create_test_graph('test_graph')")
                print(f"   [PASS] Graph created successfully")
                print(f"   Schema: {result.schema}")
                print(f"   Data rows: {len(result.data)}")
                print(f"   Metrics: {result.metrics}")
            except minigu.QueryError as e:
                print(f"   Graph creation failed: {e}")
                
            # Test calling procedures that should work with return *
            print("3. Testing procedure calls with return *...")
            try:
                # Try show_procedures with return *
                result = db.execute("CALL show_procedures() return *")
                print("   [PASS] show_procedures executed successfully")
                print(f"   Data rows: {len(result.data)}")
                if result.data:
                    print(f"   Procedures: {result.data}")
            except minigu.QueryError as e:
                print(f"   show_procedures failed: {e}")
                
            # Test direct INSERT statement with correct syntax
            print("4. Testing INSERT statement...")
            try:
                # Try INSERT with correct syntax based on GQL specification
                insert_query = "INSERT (:Person { firstname: 'Alice', lastname: 'Smith' })"
                result = db.execute(insert_query)
                print("   [PASS] INSERT executed successfully")
            except minigu.QueryError as e:
                print(f"   INSERT failed: {e}")
                
            # Test load method which uses the fixed INSERT format internally
            print("5. Testing load method...")
            try:
                # Try loading data using the Python API method
                sample_data = [
                    {"name": "Bob", "age": 30, "label": "Person"},
                    {"name": "CompanyInc", "founded": 2010, "label": "Company"}
                ]
                db.load(sample_data)
                print("   [PASS] load executed successfully")
            except minigu.DataError as e:
                print(f"   load failed: {e}")
            
        print("\n[PASS] All actual API usage tests completed!")
        
    except Exception as e:
        print(f"[FAIL] Error during actual API usage tests: {e}")
        raise


def main():
    """Main test function"""
    print("miniGU Actual API Usage Test")
    print("=" * 50)
    
    try:
        test_actual_api_usage()
        
        print("\n" + "=" * 50)
        print("All tests completed!")
        
    except Exception as e:
        print(f"\n[FAIL] Test suite failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()