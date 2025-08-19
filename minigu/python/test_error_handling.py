#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Test script for error handling
"""

import sys
import os

# Add the project root directory to the path so we can import minigu
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import minigu


def test_error_handling():
    """Test error handling in the miniGU Python API"""
    print("=== Testing Error Handling ===")
    
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
            
        try:
            db.create_graph("test_graph")
            print("   [FAIL] Should have raised MiniGUError for closed connection")
            return False
        except minigu.MiniGUError:
            print("   [PASS] Correctly raised MiniGUError for closed connection on create_graph")
            
        try:
            db.insert([{"name": "test"}])
            print("   [FAIL] Should have raised MiniGUError for closed connection")
            return False
        except minigu.MiniGUError:
            print("   [PASS] Correctly raised MiniGUError for closed connection on insert")
            
        try:
            db.update("UPDATE test SET name = 'new'")
            print("   [FAIL] Should have raised MiniGUError for closed connection")
            return False
        except minigu.MiniGUError:
            print("   [PASS] Correctly raised MiniGUError for closed connection on update")
            
        try:
            db.delete("DELETE FROM test")
            print("   [FAIL] Should have raised MiniGUError for closed connection")
            return False
        except minigu.MiniGUError:
            print("   [PASS] Correctly raised MiniGUError for closed connection on delete")
        
        # Test specific error types
        print("\n2. Testing specific error types...")
        db = minigu.connect()
        
        try:
            # This should raise a QueryError
            db.execute("INVALID QUERY")
            print("   [FAIL] Should have raised QueryError for invalid query")
            return False
        except minigu.QueryError:
            print("   [PASS] Correctly raised QueryError for invalid query")
        except Exception as e:
            print(f"   [FAIL] Raised {type(e).__name__} instead of QueryError: {e}")
            return False
            
        # Only test file loading error if we're not using Rust bindings
        if not minigu.HAS_RUST_BINDINGS:
            try:
                # This should raise a DataError
                db.load("nonexistent_file.json")
                print("   [FAIL] Should have raised DataError for nonexistent file")
                return False
            except minigu.DataError:
                print("   [PASS] Correctly raised DataError for nonexistent file")
            except Exception as e:
                print(f"   [FAIL] Raised {type(e).__name__} instead of DataError: {e}")
                return False
        else:
            print("   [SKIP] Skipping file loading error test (using Rust bindings)")
            
        # Only test graph creation error if we're not using Rust bindings
        if not minigu.HAS_RUST_BINDINGS:
            try:
                # This should raise a GraphError
                db.create_graph("test", {"InvalidType": {"invalid_prop": "INVALID"}})
                print("   [FAIL] Should have raised GraphError for invalid schema")
                return False
            except minigu.GraphError:
                print("   [PASS] Correctly raised GraphError for invalid schema")
            except Exception as e:
                print(f"   [FAIL] Raised {type(e).__name__} instead of GraphError: {e}")
                return False
        else:
            print("   [SKIP] Skipping graph creation error test (using Rust bindings)")
            
        db.close()
        
        print("\n[PASS] All error handling tests passed!")
        return True
        
    except Exception as e:
        print(f"[FAIL] Error during error handling tests: {e}")
        raise


def main():
    """Main test function"""
    print("miniGU Python API Error Handling Test")
    print("=" * 50)
    
    try:
        success = test_error_handling()
        
        if success:
            print("\n" + "=" * 50)
            print("All error handling tests completed successfully!")
        else:
            print("\n" + "=" * 50)
            print("[FAIL] Some error handling tests failed!")
            sys.exit(1)
        
    except Exception as e:
        print(f"\n[FAIL] Test suite failed with error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()