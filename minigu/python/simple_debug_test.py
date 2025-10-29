#!/usr/bin/env python3
"""
Very simple debug test for miniGU Python API on Mac.
"""

import sys
import os

# Add the python module to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

def test_import_and_basic_connection():
    """Test that we can import the module and establish basic connection"""
    print("Testing import and basic connection...")
    try:
        import minigu
        print("Import successful")
        
        db = minigu.MiniGU()
        print("Connection successful")
        return True
    except Exception as e:
        print(f"Failed: {e}")
        return False

def test_transaction_methods():
    """Test transaction methods that might cause segfaults"""
    print("Testing transaction methods...")
    try:
        import minigu
        db = minigu.MiniGU()
        
        # These methods have been known to cause segfaults on Mac
        db.begin_transaction()
        db.commit()
        db.rollback()
        
        print("All transaction methods passed")
        return True
    except Exception as e:
        print(f"Transaction methods failed: {e}")
        return False

def main():
    """Run minimal tests"""
    print("Running minimal debug tests...")
    print("=" * 40)
    
    tests = [
        test_import_and_basic_connection,
        test_transaction_methods
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
            print("-" * 30)
        except Exception as e:
            print(f"Test {test.__name__} crashed: {e}")
            print("-" * 30)
    
    print(f"Tests passed: {passed}/{total}")
    return 0 if passed == total else 1

if __name__ == "__main__":
    sys.exit(main())