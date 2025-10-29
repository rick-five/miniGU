#!/usr/bin/env python3
"""
Direct Rust binding debug test for miniGU on Mac.
"""

import sys
import os

# Add the python module to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

def test_rust_bindings():
    """Test direct Rust bindings"""
    print("Testing direct Rust bindings...")
    try:
        # Try to import the Rust module directly
        from minigu_python import PyMiniGU
        print("Direct Rust import successful")
        
        # Try to create an instance
        db = PyMiniGU()
        print("PyMiniGU instance created")
        
        # Try to initialize
        db.init()
        print("PyMiniGU initialized")
        
        return True
    except Exception as e:
        print(f"Direct Rust binding test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_rust_methods():
    """Test direct Rust methods that might cause segfaults"""
    print("Testing direct Rust methods...")
    try:
        from minigu_python import PyMiniGU
        db = PyMiniGU()
        db.init()
        
        # Test methods that have caused segfaults
        try:
            db.begin_transaction()
            print("begin_transaction: OK (not implemented)")
        except Exception as e:
            print(f"begin_transaction failed: {e}")
        
        try:
            db.commit()
            print("commit: OK (not implemented)")
        except Exception as e:
            print(f"commit failed: {e}")
            
        try:
            db.rollback()
            print("rollback: OK (not implemented)")
        except Exception as e:
            print(f"rollback failed: {e}")
            
        return True
    except Exception as e:
        print(f"Direct Rust methods test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Run Rust binding tests"""
    print("Running Rust binding debug tests...")
    print("=" * 40)
    
    tests = [
        test_rust_bindings,
        test_rust_methods
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
            import traceback
            traceback.print_exc()
            print("-" * 30)
    
    print(f"Tests passed: {passed}/{total}")
    return 0 if passed == total else 1

if __name__ == "__main__":
    sys.exit(main())