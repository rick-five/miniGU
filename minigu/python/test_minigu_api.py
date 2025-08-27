#!/usr/bin/env python3
"""
Test cases for miniGU Python API.

This file contains tests for:
1. Basic connection functionality
2. Graph creation and management
"""

import unittest
import sys
import os

# Add the python module to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from minigu.python import minigu

class TestMiniGUAPI(unittest.TestCase):
    """Test cases for the miniGU Python API."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.db = minigu.MiniGU()
    
    def tearDown(self):
        """Tear down test fixtures after each test method."""
        if self.db and self.db.is_connected:
            self.db.close()
    
    def test_connection(self):
        """Test basic database connection."""
        self.assertIsInstance(self.db, minigu.MiniGU)
        self.assertTrue(self.db.is_connected)
    
    def test_create_graph(self):
        """Test graph creation."""
        self.db.create_graph("test_graph")
        # If no exception is raised, the test passes
    
    def test_create_graph_with_schema(self):
        """Test graph creation with schema."""
        schema = {
            "Person": {"name": "STRING", "age": "INTEGER"},
            "Company": {"name": "STRING", "founded": "INTEGER"}
        }
        self.db.create_graph("test_graph_with_schema", schema)
        # If no exception is raised, the test passes

    def test_implemented_functionality(self):
        """
        Test implemented functionality in miniGU.
        This replicates the functionality from test_match_return.py
        """
        # Test create graph functionality
        try:
            self.db.create_graph("test_graph_functionality")
        except Exception as e:
            self.fail(f"CREATE GRAPH command failed with error: {e}")
        
        # Test another create graph with schema
        try:
            schema = {
                "Person": {"name": "STRING", "age": "INTEGER"},
                "Company": {"name": "STRING", "founded": "INTEGER"}
            }
            self.db.create_graph("test_graph_with_schema_functionality", schema)
        except Exception as e:
            self.fail(f"CREATE GRAPH with schema command failed with error: {e}")


if __name__ == "__main__":
    unittest.main()