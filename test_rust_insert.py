#!/usr/bin/env python3
"""
Test script to directly test Rust bindings insert_data method with different GQL statements
"""

from minigu.python.minigu_python import PyMiniGU

# Create and initialize the Rust instance
db = PyMiniGU()
db.init()

print("Testing Rust bindings insert_data method with various GQL statements:")

# Test cases
test_cases = [
    # Original generated format
    "INSERT VERTEX (Person {name: 'Alice', age: '30'}), (Person {name: 'Bob', age: '25'})",
    
    # Try with semicolon
    "INSERT VERTEX (Person {name: 'Alice', age: '30'}), (Person {name: 'Bob', age: '25'});",
    
    # Try single vertex
    "INSERT VERTEX (Person {name: 'Charlie', age: '35'})",
    
    # Try with VALUES keyword (common in SQL-like syntax)
    "INSERT VERTEX VALUES (Person {name: 'David', age: '28'})",
    
    # Try without parentheses
    "INSERT VERTEX Person {name: 'Eve', age: '32'}",
    
    # Try with explicit label syntax
    "INSERT VERTEX Person:name {name: 'Frank', age: '40'}",
    
    # Try with different property types
    "INSERT VERTEX (Company {name: 'TechCorp', founded: '2010'})",
]

for i, statement in enumerate(test_cases, 1):
    print(f"\n{i}. Testing: {statement}")
    try:
        db.insert_data(statement)
        print("   ✓ Success")
    except Exception as e:
        print(f"   ✗ Failed: {e}")

# Try creating a graph first and then inserting
print("\n--- Testing with explicit graph creation ---")
try:
    db.create_graph("test_graph", None)
    print("✓ Graph created")
    
    # Try inserting into the created graph
    test_statement = "INSERT VERTEX (Person {name: 'Grace', age: '29'})"
    print(f"Testing: {test_statement}")
    db.insert_data(test_statement)
    print("✓ Insert successful")
except Exception as e:
    print(f"✗ Failed: {e}")

# Close the database
db.close()
print("\nTest completed.")