#!/usr/bin/env python3
"""
Minimal diagnostic test for miniGU Python API.
This script verifies that the minigu_python module can be imported and instantiated correctly.
"""

import sys
import os

# Add the python module to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "minigu", "python"))

def main():
    print("Python executable:", sys.executable)
    print("Python path:", sys.path)
    
    try:
        import minigu_python
        print("SUCCESS: minigu_python imported successfully.")
    except ImportError as e:
        print(f"FAILURE: Failed to import minigu_python: {e}")
        return 1
    
    # Try to instantiate PyMiniGU to ensure class registration works
    try:
        db = minigu_python.PyMiniGU()
        print("SUCCESS: PyMiniGU instantiated.")
    except Exception as e:
        print(f"FAILURE: Instantiation failed with error: {e}")
        return 1
    
    print("All tests passed!")
    return 0

if __name__ == "__main__":
    sys.exit(main())