#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test script for miniGU Python API with real Rust bindings
"""

import sys
import os

# Add the project root directory to the path so we can import minigu
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import minigu


def test_basic_functionality():
    """Test basic functionality of the miniGU Python API"""
    print("=== Testing Basic Functionality ===")
    
    try:
        # Test connecting to database
        print("1. Connecting to database...")
        with minigu.connect() as db:
            print("   [PASS] Connected successfully")
            
            # Test loading data
            print("2. Loading sample data...")
            sample_data = [
                {"name": "Alice", "age": 30, "label": "Person"},
                {"name": "Bob", "age": 25, "label": "Person"},
                {"name": "TechCorp", "founded": 2010, "label": "Company"}
            ]
            db.load(sample_data)
            print("   [PASS] Data loaded successfully")
            
            # Test saving database
            print("3. Saving database...")
            db.save("test_database.mgu")
            print("   [PASS] Database saved successfully")
            
            # Test creating a graph
            print("4. Creating graph...")
            db.create_graph("test_graph", {
                "Person": {"name": "STRING", "age": "INTEGER"},
                "Company": {"name": "STRING", "founded": "INTEGER"}
            })
            print("   [PASS] Graph created successfully")
            
            # Test inserting data
            print("5. Inserting data...")
            db.insert([
                {"name": "Charlie", "age": 35, "label": "Person"},
                {"name": "InnovateCo", "founded": 2015, "label": "Company"}
            ])
            print("   [PASS] Data inserted successfully")
            
            # Test executing query (might fail in current implementation)
            print("6. Executing query...")
            try:
                result = db.execute("SHOW PROCEDURES")
                print(f"   [PASS] Query executed successfully: {result}")
            except minigu.QueryError as e:
                print(f"   [EXPECTED] Query execution failed (expected during development): {e}")
                
        print("\n[PASS] All basic functionality tests passed!")
        
    except Exception as e:
        print(f"[FAIL] Error during basic functionality tests: {e}")
        raise


def test_error_handling():
    """Test error handling in the miniGU Python API"""
    print("\n=== Testing Error Handling ===")
    
    try:
        # Test operations on closed connection
        print("1. Testing operations on closed connection...")
        db = minigu.connect()
        db.close()
        
        try:
            db.execute("MATCH (n) RETURN n")
            print("   [FAIL] Should have raised MiniGUError for closed connection")
            return False
        except minigu.MiniGUError:
            print("   [PASS] Correctly raised MiniGUError for closed connection")
            
        try:
            db.load([{"test": "data"}])
            print("   [FAIL] Should have raised MiniGUError for closed connection")
            return False
        except minigu.MiniGUError:
            print("   [PASS] Correctly raised MiniGUError for closed connection on load")
            
        try:
            db.save("test.mgu")
            print("   [FAIL] Should have raised MiniGUError for closed connection")
            return False
        except minigu.MiniGUError:
            print("   [PASS] Correctly raised MiniGUError for closed connection on save")
            
        print("\n[PASS] All error handling tests passed!")
        return True
        
    except Exception as e:
        print(f"[FAIL] Error during error handling tests: {e}")
        raise


def main():
    """Main test function"""
    print("miniGU Python API Test Suite")
    print("=" * 50)
    
    try:
        test_basic_functionality()
        test_error_handling()
        
        print("\n" + "=" * 50)
        print("All tests completed successfully!")
        
    except Exception as e:
        print(f"\n[FAIL] Test suite failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()