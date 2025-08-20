#!/usr/bin/env python3
"""
Test script to directly test Rust bindings raw functionality
"""

import sys
import os

# Add the project root to the path so we can import minigu
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from minigu.python.minigu_python import PyMiniGU

def test_raw_rust_bindings():
    """Test raw Rust bindings functionality"""
    print("Testing raw Rust bindings...")
    
    try:
        # Create and initialize the Rust instance
        db = PyMiniGU()
        db.init()
        print("✓ Successfully created and initialized PyMiniGU")
        
        # Try simple query
        print("Executing simple RETURN query...")
        result = db.execute("RETURN 'test' AS result")
        print(f"✓ Query executed. Result: {result}")
        if 'data' in result:
            print(f"  Data: {result['data']}")
        
        # Try another simple query
        print("Executing another simple query...")
        result = db.execute("RETURN 123 AS number")
        print(f"✓ Query executed. Result: {result}")
        if 'data' in result:
            print(f"  Data: {result['data']}")
            
        # Close database
        db.close()
        print("✓ Database closed successfully")
        
    except Exception as e:
        print(f"✗ Error during raw Rust bindings test: {e}")
        return False
        
    return True

def main():
    """Main test function"""
    print("miniGU Raw Rust Bindings Test")
    print("=" * 35)
    
    if test_raw_rust_bindings():
        print("\n" + "=" * 35)
        print("Raw Rust bindings test completed successfully!")
        return 0
    else:
        print("\n" + "=" * 35)
        print("Raw Rust bindings test failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())