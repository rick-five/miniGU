#!/usr/bin/env python3
"""
Test script to test various GQL syntax variations
"""

import sys
import os

# Add the project root to the path so we can import minigu
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from minigu.python.minigu_python import PyMiniGU

def test_various_syntax():
    """Test various GQL syntax variations"""
    print("Testing various GQL syntax variations...")
    
    # Create and initialize the Rust instance
    db = PyMiniGU()
    db.init()
    print("✓ Successfully created and initialized PyMiniGU")
    
    # Test cases with different syntax
    test_cases = [
        # Basic RETURN statements
        ("RETURN 'hello' AS greeting", "Simple string return"),
        ("RETURN 42 AS number", "Simple number return"),
        ("RETURN true AS flag", "Simple boolean return"),
        
        # More complex expressions
        ("RETURN 1 + 2 AS sum", "Arithmetic expression"),
        ("RETURN 'hello' + 'world' AS concat", "String concatenation"),
        
        # With different quotes
        ('RETURN "hello" AS greeting', "Double quoted string"),
    ]
    
    passed = 0
    total = len(test_cases)
    
    for query, description in test_cases:
        print(f"\nTesting: {description}")
        print(f"  Query: {query}")
        try:
            result = db.execute(query)
            print(f"  ✓ Success: {result['data']}")
            passed += 1
        except Exception as e:
            print(f"  ✗ Failed: {e}")
    
    # Close database
    db.close()
    print("✓ Database closed successfully")
    
    print(f"\nResults: {passed}/{total} syntax variations passed")
    return passed == total

def main():
    """Main test function"""
    print("miniGU GQL Syntax Test")
    print("=" * 25)
    
    if test_various_syntax():
        print("\n" + "=" * 25)
        print("All GQL syntax tests completed successfully!")
        return 0
    else:
        print("\n" + "=" * 25)
        print("Some GQL syntax tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())