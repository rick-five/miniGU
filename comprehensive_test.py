#!/usr/bin/env python3
"""
Comprehensive test script to check all fixed functionality
"""

import sys
import os

# Add the project root to the path so we can import minigu
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from minigu.python.minigu import MiniGU, AsyncMiniGU, HAS_RUST_BINDINGS, async_connect
import asyncio

def test_python_api_formatting():
    """Test Python API GQL formatting"""
    print("1. Testing Python API GQL formatting...")
    
    db = MiniGU()
    
    # Test data
    test_data = [
        {"name": "Alice", "age": 30, "label": "Person"},
        {"name": "Bob", "age": 25, "label": "Person"},
        {"name": "TechCorp", "founded": 2010, "label": "Company"}
    ]
    
    try:
        formatted = db._format_insert_data(test_data)
        print(f"   ✓ Formatted GQL: {formatted}")
        
        # Check that it follows GQL standards
        if "INSERT :" in formatted and "Person" in formatted and "Company" in formatted:
            print("   ✓ Correct GQL syntax (uses :Label format)")
        else:
            print("   ✗ Incorrect GQL syntax")
            return False
            
        return True
    except Exception as e:
        print(f"   ✗ Error in formatting: {e}")
        return False
    finally:
        db.close()

def test_rust_bindings_basic_queries():
    """Test basic queries with Rust bindings"""
    print("2. Testing Rust bindings basic queries...")
    
    if not HAS_RUST_BINDINGS:
        print("   ℹ Rust bindings not available, skipping")
        return True
    
    try:
        db = MiniGU()
        
        # Test simple working queries
        result = db.execute("RETURN 'test' AS result")
        if result.data and result.data[0][0] == 'test':
            print("   ✓ String return query works")
        else:
            print("   ✗ String return query failed")
            return False
            
        result = db.execute('RETURN "test" AS result')
        if result.data and result.data[0][0] == 'test':
            print("   ✓ Double-quoted string return query works")
        else:
            print("   ✗ Double-quoted string return query failed")
            return False
            
        db.close()
        return True
    except Exception as e:
        print(f"   ✗ Error in basic queries: {e}")
        return False

def test_data_loading_simulation():
    """Test data loading in simulation mode"""
    print("3. Testing data loading (simulation mode)...")
    
    # Temporarily disable Rust bindings to test simulation mode
    original_flag = None
    if HAS_RUST_BINDINGS:
        original_flag = sys.modules['minigu.python.minigu'].HAS_RUST_BINDINGS
        sys.modules['minigu.python.minigu'].HAS_RUST_BINDINGS = False
    
    try:
        db = MiniGU()
        
        # Sample data
        sample_data = [
            {"name": "Alice", "age": 30, "label": "Person"},
            {"name": "Bob", "age": 25, "label": "Person"}
        ]
        
        # Test loading data (should work in simulation mode)
        db.load(sample_data)
        print("   ✓ Data loading in simulation mode works")
        
        # Test inserting data as GQL (should work in simulation mode)
        db.insert("INSERT :Person { name: 'Charlie', age: 35 }")
        print("   ✓ GQL insert in simulation mode works")
        
        db.close()
        return True
    except Exception as e:
        print(f"   ✗ Error in simulation mode testing: {e}")
        return False
    finally:
        # Restore original flag
        if original_flag is not None:
            sys.modules['minigu.python.minigu'].HAS_RUST_BINDINGS = original_flag

async def test_async_functionality():
    """Test async functionality"""
    print("4. Testing async functionality...")
    
    try:
        # Fix: Use await with async_connect function
        db = await async_connect()  # Use the async_connect function
        
        # Test formatting
        test_data = [{"name": "AsyncUser", "age": 25, "label": "Person"}]
        formatted = db._format_insert_data(test_data)
        print(f"   ✓ Async formatting works: {formatted[:50]}...")
        
        await db.close()
        return True
    except Exception as e:
        print(f"   ✗ Error in async functionality: {e}")
        return False

def test_api_entry_points():
    """Test main API entry points"""
    print("5. Testing API entry points...")
    
    try:
        # Test synchronous connect
        db = MiniGU()
        print("   ✓ Synchronous connect works")
        db.close()
        
        # Test that the API is importable
        from minigu.python.minigu import connect, async_connect
        print("   ✓ API entry points are importable")
        
        return True
    except Exception as e:
        print(f"   ✗ Error in API entry points: {e}")
        return False

async def run_all_tests():
    """Run all tests"""
    print("miniGU Comprehensive Functionality Test")
    print("=" * 50)
    
    tests = [
        test_python_api_formatting,
        test_rust_bindings_basic_queries,
        test_data_loading_simulation,
        test_async_functionality,
        test_api_entry_points
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
            print(f"   ✗ Test {test.__name__} failed with exception: {e}")
    
    print("\n" + "=" * 50)
    print(f"Tests passed: {passed}/{len(tests)}")
    
    if passed == len(tests):
        print("All tests completed successfully!")
        return True
    else:
        print("Some tests failed!")
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