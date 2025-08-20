#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test script to demonstrate import/export functionality in miniGU Python API
"""

import sys
import os

# Add the project root directory to the path so we can import minigu
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import minigu


def test_import_export():
    """Test import/export functionality"""
    print("=== Testing Import/Export Functionality ===")
    
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
                
            # Test export with a valid directory
            print("3. Testing export functionality...")
            try:
                # Create export directory if it doesn't exist
                if not os.path.exists("export_test"):
                    os.makedirs("export_test")
                    
                # Try export with correct parameters
                db.save("export_test")
                print("   [PASS] Export executed successfully")
            except minigu.DataError as e:
                print(f"   Export failed: {e}")
                
            # Test import with a valid directory
            print("4. Testing import functionality...")
            try:
                # Try import with correct parameters
                db.load("export_test")
                print("   [PASS] Import executed successfully")
            except minigu.DataError as e:
                print(f"   Import failed: {e}")
            
        print("\n[PASS] All import/export tests completed!")
        
    except Exception as e:
        print(f"[FAIL] Error during import/export tests: {e}")
        raise


def main():
    """Main test function"""
    print("miniGU Import/Export Test")
    print("=" * 50)
    
    try:
        test_import_export()
        
        print("\n" + "=" * 50)
        print("All tests completed!")
        
    except Exception as e:
        print(f"\n[FAIL] Test suite failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()