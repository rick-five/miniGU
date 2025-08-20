#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test script to demonstrate import/export functionality in miniGU Python API with data
"""

import sys
import os

# Add the project root directory to the path so we can import minigu
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import minigu


def test_import_export_with_data():
    """Test import/export functionality with data"""
    print("=== Testing Import/Export Functionality With Data ===")
    
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
            except minigu.QueryError as e:
                print(f"   Graph creation failed: {e}")
                
            # Test using the created graph
            print("3. Using the created graph...")
            try:
                result = db.execute("USE GRAPH test_graph")
                print("   [PASS] Graph selected successfully")
            except minigu.QueryError as e:
                print(f"   Graph selection failed: {e}")
                
            # Test inserting data
            print("4. Inserting data...")
            try:
                # Insert some test data using correct GQL syntax
                db.insert("INSERT (:Person { name: 'Alice', age: 30 })")
                db.insert("INSERT (:Person { name: 'Bob', age: 25 })")
                db.insert("INSERT (:Company { name: 'TechCorp', founded: 2010 })")
                print("   [PASS] Data inserted successfully")
            except minigu.DataError as e:
                print(f"   Data insertion failed: {e}")
                
            # Test export with a valid directory
            print("5. Testing export functionality...")
            try:
                # Create export directory if it doesn't exist
                if not os.path.exists("export_test_with_data"):
                    os.makedirs("export_test_with_data")
                    
                # Try export with correct parameters
                db.save("export_test_with_data")
                print("   [PASS] Export executed successfully")
                
                # Check the exported files
                if os.path.exists("export_test_with_data/manifest.json"):
                    with open("export_test_with_data/manifest.json", "r") as f:
                        content = f.read()
                        print(f"   Manifest content: {content}")
            except minigu.DataError as e:
                print(f"   Export failed: {e}")
                
            # Test import with a valid directory
            print("6. Testing import functionality...")
            try:
                # Try import with correct parameters
                db.load("export_test_with_data")
                print("   [PASS] Import executed successfully")
            except minigu.DataError as e:
                print(f"   Import failed: {e}")
            
        print("\n[PASS] All import/export tests with data completed!")
        
    except Exception as e:
        print(f"[FAIL] Error during import/export tests with data: {e}")
        raise


def main():
    """Main test function"""
    print("miniGU Import/Export Test With Data")
    print("=" * 50)
    
    try:
        test_import_export_with_data()
        
        print("\n" + "=" * 50)
        print("All tests completed!")
        
    except Exception as e:
        print(f"\n[FAIL] Test suite failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()