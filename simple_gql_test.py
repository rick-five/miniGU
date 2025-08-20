#!/usr/bin/env python3
"""
Simple test script to verify GQL statement execution
"""

import sys
import os

# Add the project root to the path so we can import minigu
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from minigu.python.minigu import MiniGU, HAS_RUST_BINDINGS

def test_simple_queries():
    """Test simple GQL queries"""
    print("Testing simple GQL queries...")
    
    db = MiniGU()
    
    try:
        # Try some simple queries that should work
        print("Executing RETURN query...")
        result = db.execute("RETURN 'Hello' AS greeting")
        print(f"✓ Query result: {result.data}")
        
        print("Executing another simple query...")
        result = db.execute("RETURN 42 AS number")
        print(f"✓ Query result: {result.data}")
        
    except Exception as e:
        print(f"✗ Error during simple queries: {e}")
        return False
    finally:
        db.close()
        
    return True

def test_show_procedures():
    """Test SHOW PROCEDURES query"""
    print("\nTesting SHOW PROCEDURES query...")
    
    db = MiniGU()
    
    try:
        print("Executing SHOW PROCEDURES...")
        result = db.execute("SHOW PROCEDURES")
        print(f"✓ Query executed. Row count: {result.row_count}")
        if result.data:
            print(f"Sample data: {result.data[:2]}")  # Show first 2 rows
            
    except Exception as e:
        print(f"✗ Error during SHOW PROCEDURES: {e}")
        return False
    finally:
        db.close()
        
    return True

def main():
    """Main test function"""
    print("miniGU Simple GQL Test")
    print("=" * 30)
    
    tests = [
        test_simple_queries,
        test_show_procedures
    ]
    
    passed = 0
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"✗ Test {test.__name__} failed with exception: {e}")
    
    print("\n" + "=" * 30)
    print(f"Tests passed: {passed}/{len(tests)}")
    
    if passed == len(tests):
        print("All tests completed successfully!")
        return 0
    else:
        print("Some tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())