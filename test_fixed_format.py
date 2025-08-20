#!/usr/bin/env python3
"""
Test script to verify fixed _format_insert_data method
"""

import sys
import os

# Add the project root to the path so we can import minigu
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from minigu.python.minigu import MiniGU

# Create a test instance to access the method
test_db = MiniGU()

# Test data
sample_data = [
    {"name": "Alice", "age": 30, "label": "Person"},
    {"name": "Bob", "age": 25, "label": "Person"}
]

print("Testing fixed _format_insert_data method:")
print("Input data:", sample_data)

# Test the _format_insert_data method
try:
    gql_statement = test_db._format_insert_data(sample_data)
    print("Generated GQL statement:", gql_statement)
    
except Exception as e:
    print(f"Error in _format_insert_data: {e}")

# Test with different data types
print("\nTesting with different data types:")
mixed_data = [
    {"name": "Charlie", "age": 35, "score": 95.5, "label": "Person"},
    {"name": "TechCorp", "founded": 2010, "label": "Company"}
]

try:
    gql_statement = test_db._format_insert_data(mixed_data)
    print("Generated GQL statement:", gql_statement)
    
except Exception as e:
    print(f"Error in _format_insert_data with mixed data: {e}")

# Test with single item
print("\nTesting with single item:")
single_item = [{"name": "David", "age": 28, "label": "Person"}]

try:
    gql_statement = test_db._format_insert_data(single_item)
    print("Generated GQL statement:", gql_statement)
    
except Exception as e:
    print(f"Error in _format_insert_data with single item: {e}")

# Test direct execution if possible
print("\nTesting direct execution:")
try:
    db = MiniGU()
    if db._rust_instance:
        print("Testing with Rust backend")
        # Try a simple valid GQL statement
        try:
            result = db.execute("RETURN 'test' AS result")
            print("Simple query works:", result.data)
        except Exception as e:
            print("Simple query failed:", e)
    else:
        print("Using simulated backend")
    db.close()
except Exception as e:
    print(f"Connection test failed: {e}")

print("\nTest completed.")