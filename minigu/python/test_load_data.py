#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test script for load_data functionality
"""

import sys
import os

# Add the project root directory to the path so we can import minigu
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import minigu


def test_load_data():
    """Test load_data functionality"""
    print("=== Testing load_data functionality ===")
    
    try:
        # Test connecting to database
        print("1. Connecting to database...")
        with minigu.connect() as db:
            print("   [PASS] Connected successfully")
            
            # Test loading data from Python objects
            print("2. Loading sample data...")
            sample_data = [
                {"name": "Alice", "age": 30, "label": "Person"},
                {"name": "Bob", "age": 25, "label": "Person"},
                {"name": "TechCorp", "founded": 2010, "label": "Company"}
            ]
            db.load(sample_data)
            print("   [PASS] Data loaded successfully")
            
        print("\n[PASS] All load_data tests passed!")
        
    except Exception as e:
        print(f"[FAIL] Error during load_data tests: {e}")
        raise


def main():
    """Main test function"""
    print("miniGU Python API load_data Test")
    print("=" * 50)
    
    try:
        test_load_data()
        
        print("\n" + "=" * 50)
        print("All load_data tests completed successfully!")
        
    except Exception as e:
        print(f"\n[FAIL] Test suite failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()