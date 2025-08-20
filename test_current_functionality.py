#!/usr/bin/env python3
"""
Test script to check if all current functionality works properly
"""

import sys
import os

# Add the project root to the path so we can import minigu
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from minigu.python.minigu import MiniGU, AsyncMiniGU, HAS_RUST_BINDINGS, connect, async_connect
import asyncio

def test_synchronous_functionality():
    """Test synchronous functionality"""
    print("Testing synchronous functionality...")
    
    try:
        # Test connection
        db = connect()
        print("✓ Connection successful")
        
        # Test basic query
        result = db.execute("RETURN 'test' AS result")
        if result.data and result.data[0][0] == 'test':
            print("✓ Basic query execution works")
        else:
            print("✗ Basic query execution failed")
            return False
            
        # Test data formatting
        test_data = [{"name": "Alice", "age": 30, "label": "Person"}]
        formatted = db._format_insert_data(test_data)
        if "INSERT :Person" in formatted:
            print("✓ Data formatting works")
        else:
            print("✗ Data formatting failed")
            return False
            
        db.close()
        print("✓ Connection closed successfully")
        return True
        
    except Exception as e:
        print(f"✗ Error in synchronous functionality: {e}")
        return False

async def test_asynchronous_functionality():
    """Test asynchronous functionality"""
    print("\nTesting asynchronous functionality...")
    
    try:
        # Test connection
        db = await async_connect()
        print("✓ Async connection successful")
        
        # Test basic query
        result = await db.execute("RETURN 'async_test' AS result")
        if result.data and result.data[0][0] == 'async_test':
            print("✓ Async basic query execution works")
        else:
            print("✗ Async basic query execution failed")
            return False
            
        # Test data formatting
        test_data = [{"name": "Bob", "age": 25, "label": "Person"}]
        formatted = db._format_insert_data(test_data)
        if "INSERT :Person" in formatted:
            print("✓ Async data formatting works")
        else:
            print("✗ Async data formatting failed")
            return False
            
        await db.close()
        print("✓ Async connection closed successfully")
        return True
        
    except Exception as e:
        print(f"✗ Error in asynchronous functionality: {e}")
        return False

def test_api_entry_points():
    """Test API entry points"""
    print("\nTesting API entry points...")
    
    try:
        # Test synchronous connect
        db = MiniGU()
        print("✓ MiniGU class instantiation works")
        db.close()
        
        # Test connect functions
        db = connect()
        print("✓ connect() function works")
        db.close()
        
        print("✓ All API entry points work")
        return True
        
    except Exception as e:
        print(f"✗ Error in API entry points: {e}")
        return False

async def test_async_api_entry_points():
    """Test async API entry points"""
    print("\nTesting async API entry points...")
    
    try:
        # Test asynchronous connect
        db = AsyncMiniGU()
        print("✓ AsyncMiniGU class instantiation works")
        await db.close()
        
        # Test async_connect functions
        db = await async_connect()
        print("✓ async_connect() function works")
        await db.close()
        
        print("✓ All async API entry points work")
        return True
        
    except Exception as e:
        print(f"✗ Error in async API entry points: {e}")
        return False

def test_rust_bindings_status():
    """Test Rust bindings status"""
    print("\nTesting Rust bindings status...")
    
    print(f"Rust bindings available: {HAS_RUST_BINDINGS}")
    
    if HAS_RUST_BINDINGS:
        print("✓ Rust bindings are available")
        return True
    else:
        print("✗ Rust bindings are not available")
        return False

async def run_all_tests():
    """Run all tests"""
    print("miniGU Current Functionality Test")
    print("=" * 40)
    
    tests = [
        test_rust_bindings_status,
        test_api_entry_points,
        test_synchronous_functionality,
        test_asynchronous_functionality,
        test_async_api_entry_points
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
    
    print("\n" + "=" * 40)
    print(f"Tests passed: {passed}/{len(tests)}")
    
    if passed == len(tests):
        print("All functionality tests completed successfully!")
        return True
    else:
        print("Some functionality tests failed!")
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