#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test script for create_graph and insert_data functionality
"""

import sys
import os

# Add the project root directory to the path so we can import minigu
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import minigu


def test_create_graph_and_insert():
    """Test create_graph and insert_data functionality"""
    print("=== Testing create_graph and insert_data functionality ===")
    
    try:
        # Test connecting to database
        print("1. Connecting to database...")
        with minigu.connect() as db:
            print("   [PASS] Connected successfully")
            
            # Test creating a graph
            print("2. Creating graph...")
            db.create_graph("test_graph", {
                "Person": {"name": "STRING", "age": "INTEGER"},
                "Company": {"name": "STRING", "founded": "INTEGER"}
            })
            print("   [PASS] Graph created successfully")
            
            # Test inserting data
            print("3. Inserting data...")
            db.insert([
                {"name": "Alice", "age": 30, "label": "Person"},
                {"name": "Bob", "age": 25, "label": "Person"},
                {"name": "TechCorp", "founded": 2010, "label": "Company"}
            ])
            print("   [PASS] Data inserted successfully")
            
        print("\n[PASS] All create_graph and insert_data tests passed!")
        
    except Exception as e:
        print(f"[FAIL] Error during tests: {e}")
        raise


def main():
    """Main test function"""
    print("miniGU Python API create_graph and insert_data Test")
    print("=" * 50)
    
    try:
        test_create_graph_and_insert()
        
        print("\n" + "=" * 50)
        print("All tests completed successfully!")
        
    except Exception as e:
        print(f"\n[FAIL] Test suite failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()