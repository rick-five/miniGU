#!/usr/bin/env python3
"""
Final validation test script to verify removal of simulation functionality
and ensure only real functionality remains
"""

import sys
import os

# Add the project root to the path so we can import minigu
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from minigu.python.minigu import MiniGU, AsyncMiniGU, HAS_RUST_BINDINGS, connect, async_connect
import asyncio

def test_no_simulation_mode():
    """Test that simulation mode has been removed"""
    print("Testing that simulation mode has been removed...")
    
    # Temporarily disable Rust bindings to test behavior
    original_flag = None
    if HAS_RUST_BINDINGS:
        import minigu.python.minigu as minigu_module
        original_flag = minigu_module.HAS_RUST_BINDINGS
        minigu_module.HAS_RUST_BINDINGS = False
    
    try:
        # Try to create a connection - this should now fail
        db = connect()
        print("✗ Simulation mode still available - this should not happen")
        db.close()
        return False
    except Exception as e:
        # This is expected - we removed simulation mode
        if "Rust bindings not available" in str(e) or "Rust bindings required" in str(e):
            print("✓ Simulation mode properly removed - correct error when Rust bindings unavailable")
            return True
        else:
            print(f"✗ Unexpected error: {e}")
            return False
    finally:
        # Restore original flag
        if original_flag is not None:
            import minigu.python.minigu as minigu_module
            minigu_module.HAS_RUST_BINDINGS = original_flag

def test_real_functionality():
    """Test that real functionality still works"""
    print("\nTesting that real functionality still works...")
    
    if not HAS_RUST_BINDINGS:
        print("✗ Rust bindings not available - cannot test real functionality")
        return False
    
    try:
        # Test synchronous functionality
        db = connect()
        print("✓ Synchronous connection works")
        
        # Test basic query
        result = db.execute("RETURN 'real_test' AS result")
        if result.data and result.data[0][0] == 'real_test':
            print("✓ Real query execution works")
        else:
            print("✗ Real query execution failed")
            return False
            
        # Test data formatting
        test_data = [{"name": "TestPerson", "age": 30, "label": "Person"}]
        formatted = db._format_insert_data(test_data)
        if "INSERT :Person" in formatted and "name: 'TestPerson'" in formatted:
            print("✓ Real data formatting works correctly")
        else:
            print("✗ Real data formatting failed")
            return False
            
        db.close()
        print("✓ Synchronous functionality works correctly")
        
        return True
    except Exception as e:
        print(f"✗ Error in real functionality: {e}")
        return False

async def test_async_real_functionality():
    """Test that async real functionality still works"""
    print("\nTesting that async real functionality still works...")
    
    if not HAS_RUST_BINDINGS:
        print("✗ Rust bindings not available - cannot test async real functionality")
        return False
    
    try:
        # Test asynchronous functionality
        db = await async_connect()
        print("✓ Asynchronous connection works")
        
        # Test basic query
        result = await db.execute("RETURN 'async_real_test' AS result")
        if result.data and result.data[0][0] == 'async_real_test':
            print("✓ Async real query execution works")
        else:
            print("✗ Async real query execution failed")
            return False
            
        # Test data formatting
        test_data = [{"name": "AsyncTestPerson", "age": 25, "label": "Person"}]
        formatted = db._format_insert_data(test_data)
        if "INSERT :Person" in formatted and "name: 'AsyncTestPerson'" in formatted:
            print("✓ Async real data formatting works correctly")
        else:
            print("✗ Async real data formatting failed")
            return False
            
        await db.close()
        print("✓ Asynchronous functionality works correctly")
        
        return True
    except Exception as e:
        print(f"✗ Error in async real functionality: {e}")
        return False

def test_gql_formatting_standards():
    """Test that GQL formatting follows standards"""
    print("\nTesting GQL formatting standards...")
    
    try:
        db = connect()
        
        # Test data
        test_data = [
            {"name": "Person1", "age": 30, "label": "Person"},
            {"name": "Company1", "founded": 2010, "label": "Company"}
        ]
        
        formatted = db._format_insert_data(test_data)
        print(f"Generated GQL: {formatted}")
        
        # Check GQL standards from our memory
        checks = [
            ("Uses :Label syntax", "INSERT :" in formatted and "INSERT (" not in formatted),
            ("Separate INSERT statements", formatted.count("INSERT") == 2),
            ("Person label present", "Person" in formatted),
            ("Company label present", "Company" in formatted),
            ("Properties present", "name:" in formatted and "age:" in formatted),
        ]
        
        all_passed = True
        for desc, check in checks:
            if check:
                print(f"✓ {desc}")
            else:
                print(f"✗ {desc}")
                all_passed = False
                
        db.close()
        return all_passed
    except Exception as e:
        print(f"✗ Error in GQL formatting standards test: {e}")
        return False

async def run_all_tests():
    """Run all tests"""
    print("miniGU Final Validation Test")
    print("=" * 35)
    
    tests = [
        test_no_simulation_mode,
        test_real_functionality,
        test_async_real_functionality,
        test_gql_formatting_standards
    ]
    
    passed = 0
    for test in tests:
        try:
            if asyncio.iscoroutinefunction(test):
                result = await test()
            else:
                result = test()
                
            if result:
                passed += 1
        except Exception as e:
            print(f"✗ Test {test.__name__} failed with exception: {e}")
    
    print("\n" + "=" * 35)
    print(f"Tests passed: {passed}/{len(tests)}")
    
    if passed == len(tests):
        print("\nAll validations passed!")
        print("✓ Simulation mode has been successfully removed")
        print("✓ Real functionality still works correctly")
        print("✓ GQL formatting follows standards")
        print("✓ Both synchronous and asynchronous APIs work")
        return True
    else:
        print("Some validations failed!")
        return False

def main():
    """Main function"""
    try:
        result = asyncio.run(run_all_tests())
        return 0 if result else 1
    except Exception as e:
        print(f"Error running tests: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())