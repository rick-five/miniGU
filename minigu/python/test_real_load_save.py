#!/usr/bin/env python3

"""
Test miniGU's real load and save functionality
"""

import tempfile
import os

try:
    # Try to import Rust bindings
    from minigu import connect, MiniGU
    print("Successfully imported miniGU")
    
    # Test save functionality
    print("\n=== Test save functionality ===")
    with connect() as db:
        # Save to temporary file
        with tempfile.NamedTemporaryFile(suffix='.mgu', delete=False) as tmp:
            tmp_path = tmp.name
        
        try:
            print(f"Saving database to file: {tmp_path}")
            db.save(tmp_path)
            print("Save successful!")
            
            # Check if file exists and is not empty
            if os.path.exists(tmp_path):
                size = os.path.getsize(tmp_path)
                print(f"File size: {size} bytes")
                if size > 0:
                    print("File is not empty, save functionality working properly")
                else:
                    print("Warning: File is empty")
            else:
                print("Error: File not created")
        finally:
            # Clean up temporary file
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    # Test load functionality
    print("\n=== Test load functionality ===")
    with connect() as db:
        # Create a test file
        with tempfile.NamedTemporaryFile(suffix='.mgu', delete=False) as tmp:
            tmp_path = tmp.name
            
        try:
            # First save a file
            db.save(tmp_path)
            
            # Then try to load it
            print(f"Loading data from file: {tmp_path}")
            db.load(tmp_path)
            print("Load successful!")
            
        except Exception as e:
            print(f"Error occurred during loading: {e}")
        finally:
            # Clean up temporary file
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    # Test loading from data functionality
    print("\n=== Test loading from data functionality ===")
    with connect() as db:
        # Prepare test data
        test_data = [
            {"name": "Alice", "age": 30, "label": "Person"},
            {"name": "Bob", "age": 25, "label": "Person"},
            {"name": "TechCorp", "founded": 2010, "label": "Company"}
        ]
        
        print("Loading data from Python objects:")
        db.load(test_data)
        print("Data loaded successfully!")
        
        # Save loaded data
        with tempfile.NamedTemporaryFile(suffix='.mgu', delete=False) as tmp:
            tmp_path = tmp.name
            
        try:
            print(f"Saving loaded data to file: {tmp_path}")
            db.save(tmp_path)
            print("Data saved successfully!")
            
            # Check file size
            size = os.path.getsize(tmp_path)
            print(f"File size: {size} bytes")
            
        finally:
            # Clean up temporary file
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    print("\n=== All tests completed ===")
    
except Exception as e:
    print(f"Error occurred during testing: {e}")
    import traceback
    traceback.print_exc()