#!/usr/bin/env python3
"""
Test script to check if Rust bindings can be imported and used properly
"""

print("Testing Rust bindings import and functionality...")

# Try to import Rust bindings
try:
    from minigu.python import minigu_python
    from minigu.python.minigu_python import PyMiniGU
    print("✓ Successfully imported Rust bindings using 'from minigu.python import minigu_python'")
    
    # Test if we can create an instance
    try:
        db = PyMiniGU()
        print("✓ Successfully created PyMiniGU instance")
        
        # Test init method if it exists
        try:
            db.init()
            print("✓ Successfully called init() method")
        except Exception as e:
            print(f"⚠ init() method not available or failed: {e}")
            
    except Exception as e:
        print(f"✗ Failed to create PyMiniGU instance: {e}")

except ImportError as e:
    print(f"✗ Failed to import Rust bindings from minigu.python.minigu_python: {e}")
    
    # Try alternative import path
    try:
        import minigu_python
        from minigu_python import PyMiniGU
        print("✓ Successfully imported Rust bindings using direct import")
        
        # Test if we can create an instance
        try:
            db = PyMiniGU()
            print("✓ Successfully created PyMiniGU instance")
        except Exception as e:
            print(f"✗ Failed to create PyMiniGU instance: {e}")
            
    except ImportError as e:
        print(f"✗ Failed to import Rust bindings directly: {e}")
        print("ℹ Rust bindings are not available. The Python API will use simulated implementation.")

# Test the main API to see which implementation it's using
print("\nTesting main API implementation...")

try:
    from minigu.python.minigu import HAS_RUST_BINDINGS, MiniGU
    
    print(f"HAS_RUST_BINDINGS flag: {HAS_RUST_BINDINGS}")
    
    if HAS_RUST_BINDINGS:
        print("✓ Main API will use Rust bindings")
    else:
        print("ℹ Main API will use simulated implementation")
        
    # Try to create a connection
    try:
        db = MiniGU()
        print("✓ Successfully created MiniGU instance")
        print(f"Connection status: {db.is_connected}")
        if db._rust_instance:
            print("✓ Rust instance is available")
        else:
            print("ℹ No Rust instance available")
        db.close()
    except Exception as e:
        print(f"✗ Failed to create MiniGU instance: {e}")

except ImportError as e:
    print(f"✗ Failed to import main API: {e}")

print("\nTest completed.")