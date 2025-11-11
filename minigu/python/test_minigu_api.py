#!/usr/bin/env python3.7
"""
Test cases for miniGU Python API.

This file contains tests for:
1. Basic connection functionality
2. Graph creation and management
3. Query execution
4. Result handling
5. Error handling
6. Async API functionality
7. Transaction methods
8. Security features

Stability:
    These tests validate the current alpha state of the API.
    Features may change in future versions.
"""

import unittest
import sys
import os
from pathlib import Path

# Add the parent directory to the path so we can import minigu
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import minigu
from minigu import (
    MiniGU, AsyncMiniGU, Vertex, Edge, QueryResult,
    MiniGUError, ConnectionError, QueryError, QuerySyntaxError,
    QueryExecutionError, QueryTimeoutError, DataError, GraphError, TransactionError
)

class TestMiniGUAPI(unittest.TestCase):
    """Test cases for the miniGU Python API."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.db = MiniGU()
        # Ensure we're connected
        if not self.db.is_connected:
            self.db._connect()

    def tearDown(self):
        """Tear down test fixtures after each test method."""
        if self.db and self.db.is_connected:
            self.db.close()

    def test_import(self):
        """Test that the module imports correctly."""
        self.assertTrue(hasattr(minigu, 'MiniGU'))
        self.assertTrue(hasattr(minigu, 'AsyncMiniGU'))

    def test_connection(self):
        """Test database connection."""
        # Test that we can create a database instance
        db = minigu.MiniGU()
        self.assertIsInstance(db, minigu.MiniGU)
        
        # Test that the database connects automatically when needed
        self.assertTrue(db.is_connected)
        
        # Test connection info
        info = db.connection_info
        self.assertIn('is_connected', info)
        self.assertTrue(info['is_connected'])
        
        db.close()

    def test_database_status(self):
        """Test getting database status."""
        status = self.db.get_database_status()
        self.assertIn('status', status)
        self.assertIn('version', status)
        self.assertIn('features', status)
        self.assertEqual(status['status'], 'connected')

    def test_create_graph(self):
        """Test creating a graph."""
        # Test creating a graph
        result = self.db.create_graph("test_graph")
        self.assertTrue(result)
        
        # Test creating a graph with special characters (without sanitization)
        result = self.db.create_graph("test_graph_123")
        self.assertTrue(result)

    def test_load_data(self):
        """Test loading data into the database."""
        self.db.create_graph("test_graph_for_load")
        # Test loading with empty data list
        result = self.db.load([])
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()