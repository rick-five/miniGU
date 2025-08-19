#!/usr/bin/env python3

"""
Test miniGU's load and save functionality
"""

try:
    # Try to import Rust bindings
    from minigu_python import PyMiniGU
    print("Successfully imported PyMiniGU")
    
    # Create database instance
    db = PyMiniGU()
    print("Created PyMiniGU instance")
    
    # Test load_data functionality
    test_data = [
        {"name": "Alice", "age": 30, "label": "Person"},
        {"name": "Bob", "age": 25, "label": "Person"},
        {"name": "TechCorp", "founded": 2010, "label": "Company"}
    ]
    
    print("Testing load_data...")
    db.load_data(test_data)
    print("load_data test completed")
    
    # Test save_to_file functionality
    print("Testing save_to_file...")
    db.save_to_file("test_database.mgu")
    print("save_to_file test completed")
    
    # Test load_from_file functionality
    print("Testing load_from_file...")
    db.load_from_file("test_database.mgu")
    print("load_from_file test completed")
    
    print("All tests passed!")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()