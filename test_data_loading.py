#!/usr/bin/env python3
"""
Test script to verify fixed data loading functionality
"""

import sys
import os

# Add the project root to the path so we can import minigu
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from minigu.python.minigu import MiniGU, HAS_RUST_BINDINGS

def test_data_loading():
    """Test data loading functionality"""
    print("Testing data loading functionality...")
    
    # Create database instance
    db = MiniGU()
    print(f"Rust bindings available: {HAS_RUST_BINDINGS}")
    
    # Sample data to load
    sample_data = [
        {"name": "Alice", "age": 30, "label": "Person"},
        {"name": "Bob", "age": 25, "label": "Person"},
        {"name": "TechCorp", "founded": 2010, "label": "Company"}
    ]
    
    try:
        # First create a graph
        print("Creating graph...")
        db.create_graph("test_graph")
        print("✓ Graph created")
        
        # Try to load data
        print("Loading sample data...")
        db.load(sample_data)
        print("✓ Data loading completed successfully")
        
        # Try to save the database
        print("Saving database to test_output.mgu...")
        db.save("test_output.mgu")
        print("✓ Database saved successfully")
        
    except Exception as e:
        print(f"✗ Error during data loading: {e}")
        return False
    finally:
        db.close()
    
    return True

def test_insert_functionality():
    """Test insert functionality with GQL statements"""
    print("\nTesting insert functionality...")
    
    db = MiniGU()
    
    try:
        # First create a graph
        print("Creating graph...")
        db.create_graph("test_graph")
        print("✓ Graph created")
        
        # Try inserting using GQL statements
        print("Inserting data using GQL statements...")
        db.insert("INSERT :Person { name: 'Charlie', age: 35 }")
        db.insert("INSERT :Company { name: 'InnovateCo', founded: 2015 }")
        print("✓ GQL insert statements executed successfully")
        
    except Exception as e:
        print(f"✗ Error during GQL insert: {e}")
        return False
    finally:
        db.close()
        
    return True

def test_format_insert_data():
    """Test the _format_insert_data method directly"""
    print("\nTesting _format_insert_data method...")
    
    db = MiniGU()
    
    # Test data
    test_data = [
        {"name": "David", "age": 28, "label": "Person"},
        {"title": "Manager", "salary": 5000.50, "label": "Position"}
    ]
    
    try:
        # First create a graph
        print("Creating graph...")
        db.create_graph("test_graph")
        print("✓ Graph created")
        
        formatted = db._format_insert_data(test_data)
        print(f"Formatted GQL statements: {formatted}")
        
        # Try to execute the formatted statements if we have Rust bindings
        if HAS_RUST_BINDINGS and db._rust_instance:
            print("Attempting to execute formatted statements...")
            db._rust_instance.insert_data(formatted)
            print("✓ Formatted statements executed successfully")
        else:
            print("Skipping execution (no Rust bindings or using simulated implementation)")
            
    except Exception as e:
        print(f"✗ Error during format/execution test: {e}")
        return False
    finally:
        db.close()
        
    return True

def main():
    """Main test function"""
    print("miniGU Data Loading and GQL Insert Test")
    print("=" * 50)
    
    tests = [
        test_format_insert_data,
        test_data_loading,
        test_insert_functionality
    ]
    
    passed = 0
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"✗ Test {test.__name__} failed with exception: {e}")
    
    print("\n" + "=" * 50)
    print(f"Tests passed: {passed}/{len(tests)}")
    
    if passed == len(tests):
        print("All tests completed successfully!")
        return 0
    else:
        print("Some tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())