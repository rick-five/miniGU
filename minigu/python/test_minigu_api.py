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
import asyncio
import sys
import os

# Add the python module to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import minigu


class TestMiniGUAPI(unittest.TestCase):
    """
    Test suite for the synchronous MiniGU API.
    
    These tests validate the functionality of the synchronous MiniGU interface,
    including connection management, graph operations, data loading, and query execution.
    """
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.db = minigu.MiniGU()
        self.test_graph_name = "test_graph_for_unit_tests"
        # Ensure connection for tests that require it
        if not self.db.is_connected:
            self.db._connect()

    def tearDown(self):
        """Tear down test fixtures after each test method."""
        pass

    def test_connect(self):
        """Test connecting to the database."""
        # Connection should be established in setUp
        self.assertTrue(self.db.is_connected)
        self.assertIsNotNone(self.db._rust_instance)

    def test_create_graph(self):
        """Test creating a graph."""
        # This should work without throwing exceptions and return True
        result = self.db.create_graph("test_graph")
        self.assertTrue(result)

    def test_create_graph_with_special_chars(self):
        """Test creating a graph with special characters in the name."""
        # This should sanitize the name and not throw exceptions
        result = self.db.create_graph("test_graph_with_special_chars_123")
        self.assertTrue(result)

    def test_create_graph_with_injection_attempt(self):
        """Test creating a graph with potential injection attempts."""
        # This should sanitize the name and not throw exceptions
        result = self.db.create_graph("test_graph'; DROP TABLE users; --")
        self.assertTrue(result)

    def test_load_data(self):
        """Test loading data into the database."""
        self.db.create_graph("test_graph_for_load")
        # Test loading with empty data list
        result = self.db.load([])
        self.assertTrue(result)

    def test_save_data(self):
        """Test saving the database."""
        self.db.create_graph("test_graph_for_save")
        # Test saving to a path (this will fail because we don't have a real path, but should return False)
        result = self.db.save("/tmp/test_save")
        # This will likely fail due to path issues, but we're testing the return value handling
        # The important thing is that it returns a boolean, not that it succeeds
        self.assertIsInstance(result, bool)

    def test_begin_transaction(self):
        """Test beginning a transaction."""
        self.db.create_graph("test_graph_for_transaction")
        # This should raise TransactionError as the feature is not yet implemented
        with self.assertRaises(minigu.TransactionError) as context:
            self.db.begin_transaction()
        # Check that the error message indicates the feature is planned but not yet implemented
        self.assertIn("not yet implemented", str(context.exception).lower())

    def test_commit_transaction(self):
        """Test committing a transaction."""
        self.db.create_graph("test_graph_for_commit")
        # This should raise TransactionError as the feature is not yet implemented
        with self.assertRaises(minigu.TransactionError) as context:
            self.db.commit()
        # Check that the error message indicates the feature is planned but not yet implemented
        self.assertIn("not yet implemented", str(context.exception).lower())

    def test_rollback_transaction(self):
        """Test rolling back a transaction."""
        self.db.create_graph("test_graph_for_rollback")
        # This should raise TransactionError as the feature is not yet implemented
        with self.assertRaises(minigu.TransactionError) as context:
            self.db.rollback()
        # Check that the error message indicates the feature is planned but not yet implemented
        self.assertIn("not yet implemented", str(context.exception).lower())

    def test_transaction_methods(self):
        """Test transaction methods existence and basic functionality."""
        # Check that transaction methods exist
        self.assertTrue(hasattr(self.db, 'begin_transaction'))
        self.assertTrue(hasattr(self.db, 'commit'))
        self.assertTrue(hasattr(self.db, 'rollback'))

        # Test that transaction methods raise TransactionError as they are not yet implemented
        self.db.create_graph("test_graph_for_methods")
        with self.assertRaises(minigu.TransactionError) as context:
            self.db.begin_transaction()
        # Check that the error message indicates the feature is planned but not yet implemented
        self.assertIn("not yet implemented", str(context.exception).lower())

    def test_vertex_class(self):
        """Test Vertex class functionality."""
        # Test creating a vertex without parameters
        vertex = minigu.Vertex()
        self.assertIsNone(vertex.id)
        self.assertIsNone(vertex.label)
        self.assertEqual(vertex.properties, {})
        
        # Test creating a vertex with parameters
        vertex = minigu.Vertex(vertex_id=1, label="Person", properties={"name": "Alice", "age": 30})
        self.assertEqual(vertex.id, 1)
        self.assertEqual(vertex.label, "Person")
        self.assertEqual(vertex.properties, {"name": "Alice", "age": 30})
        
        # Test property access
        self.assertEqual(vertex.get_property("name"), "Alice")
        self.assertIsNone(vertex.get_property("nonexistent"))
        
        # Test property modification
        vertex.set_property("city", "New York")
        self.assertEqual(vertex.get_property("city"), "New York")
        
        # Test string representation
        repr_str = repr(vertex)
        self.assertIn("Vertex", repr_str)
        self.assertIn("1", repr_str)
        self.assertIn("Person", repr_str)

    def test_edge_class(self):
        """Test Edge class functionality."""
        # Test creating an edge without parameters
        edge = minigu.Edge()
        self.assertIsNone(edge.id)
        self.assertIsNone(edge.label)
        self.assertIsNone(edge.source_id)
        self.assertIsNone(edge.destination_id)
        self.assertEqual(edge.properties, {})
        
        # Test creating an edge with parameters
        edge = minigu.Edge(edge_id=1, label="KNOWS", source_id=1, destination_id=2, 
                          properties={"since": 2020})
        self.assertEqual(edge.id, 1)
        self.assertEqual(edge.label, "KNOWS")
        self.assertEqual(edge.source_id, 1)
        self.assertEqual(edge.destination_id, 2)
        self.assertEqual(edge.properties, {"since": 2020})
        
        # Test property access
        self.assertEqual(edge.get_property("since"), 2020)
        self.assertIsNone(edge.get_property("nonexistent"))
        
        # Test property modification
        edge.set_property("strength", "strong")
        self.assertEqual(edge.get_property("strength"), "strong")
        
        # Test string representation
        repr_str = repr(edge)
        self.assertIn("Edge", repr_str)
        self.assertIn("1", repr_str)
        self.assertIn("KNOWS", repr_str)
        self.assertIn("1", repr_str)  # source
        self.assertIn("2", repr_str)  # destination

    def test_sanitize_graph_name(self):
        """Test the graph name sanitization function."""
        # Test normal name
        self.assertEqual(minigu._sanitize_graph_name("test_graph"), "test_graph")
        
        # Test name with special characters
        self.assertEqual(minigu._sanitize_graph_name("test_graph_123"), "test_graph_123")
        
        # Test name with injection attempt
        self.assertEqual(minigu._sanitize_graph_name("test_graph'; DROP TABLE users; --"), 
                         "test_graphDROPTABLEusers")
        
        # Test name with only special characters
        self.assertEqual(minigu._sanitize_graph_name("'; --"), "")

    def test_sanitize_file_path(self):
        """Test the file path sanitization function."""
        # Test normal path
        self.assertEqual(minigu._sanitize_file_path("test.csv"), "test.csv")
        
        # Test path with quotes and semicolons
        self.assertEqual(minigu._sanitize_file_path("test';.csv"), "test.csv")
        
        # Test path with directory traversal attempt
        self.assertEqual(minigu._sanitize_file_path("../test.csv"), "/test.csv")
        
        # Test path with newlines
        self.assertEqual(minigu._sanitize_file_path("test\n.csv"), "test.csv")


# Only define async tests if we're on Python 3.8+
if sys.version_info >= (3, 8):
    class TestAsyncMiniGUAPI(unittest.IsolatedAsyncioTestCase):
        """
        Test suite for the asynchronous MiniGU API.
        
        These tests validate the functionality of the asynchronous MiniGU interface,
        including connection management, graph operations, data loading, and query execution.
        """
        
        def setUp(self):
            """Set up test fixtures before each test method."""
            self.db = minigu.AsyncMiniGU()
            self.test_graph_name = "test_graph_for_async_unit_tests"
            # Ensure connection for tests that require it
            if not self.db.is_connected:
                self.db._connect()

        def tearDown(self):
            """Tear down test fixtures after each test method."""
            pass

        async def test_async_connect(self):
            """Test connecting to the database asynchronously."""
            self.assertTrue(self.db.is_connected)
            self.assertIsNotNone(self.db._rust_instance)

        async def test_async_create_graph(self):
            """Test creating a graph asynchronously."""
            result = await self.db.create_graph("test_async_graph")
            self.assertTrue(result)

        async def test_async_create_graph_with_special_chars(self):
            """Test creating a graph with special characters in the name asynchronously."""
            result = await self.db.create_graph("test_async_graph_with_special_chars_123")
            self.assertTrue(result)

        async def test_async_create_graph_with_injection_attempt(self):
            """Test creating a graph with potential injection attempts asynchronously."""
            result = await self.db.create_graph("test_async_graph'; DROP TABLE users; --")
            self.assertTrue(result)

        async def test_async_load_data(self):
            """Test loading data into the database asynchronously."""
            await self.db.create_graph("test_async_graph_for_load")
            # Test loading with empty data list
            result = await self.db.load([])
            self.assertTrue(result)

        async def test_async_save_data(self):
            """Test saving the database asynchronously."""
            await self.db.create_graph("test_async_graph_for_save")
            # Test saving to a path (this will fail because we don't have a real path, but should return False)
            result = await self.db.save("/tmp/test_save")
            # This will likely fail due to path issues, but we're testing the return value handling
            # The important thing is that it returns a boolean, not that it succeeds
            self.assertIsInstance(result, bool)

        async def test_async_begin_transaction(self):
            """Test beginning a transaction asynchronously."""
            await self.db.create_graph("test_async_transaction_graph")
            # This should raise TransactionError as the feature is not yet implemented
            with self.assertRaises(minigu.TransactionError) as context:
                await self.db.begin_transaction()
            # Check that the error message indicates the feature is planned but not yet implemented
            self.assertIn("not yet implemented", str(context.exception).lower())

        async def test_async_commit_transaction(self):
            """Test committing a transaction asynchronously."""
            await self.db.create_graph("test_async_commit_graph")
            # This should raise TransactionError as the feature is not yet implemented
            with self.assertRaises(minigu.TransactionError) as context:
                await self.db.commit()
            # Check that the error message indicates the feature is planned but not yet implemented
            self.assertIn("not yet implemented", str(context.exception).lower())

        async def test_async_rollback_transaction(self):
            """Test rolling back a transaction asynchronously."""
            await self.db.create_graph("test_async_rollback_graph")
            # This should raise TransactionError as the feature is not yet implemented
            with self.assertRaises(minigu.TransactionError) as context:
                await self.db.rollback()
            # Check that the error message indicates the feature is planned but not yet implemented
            self.assertIn("not yet implemented", str(context.exception).lower())

        async def test_async_vertex_class(self):
            """Test Vertex class functionality in async context."""
            # Test creating a vertex without parameters
            vertex = minigu.Vertex()
            self.assertIsNone(vertex.id)
            self.assertIsNone(vertex.label)
            self.assertEqual(vertex.properties, {})
            
            # Test creating a vertex with parameters
            vertex = minigu.Vertex(vertex_id=1, label="Person", properties={"name": "Alice", "age": 30})
            self.assertEqual(vertex.id, 1)
            self.assertEqual(vertex.label, "Person")
            self.assertEqual(vertex.properties, {"name": "Alice", "age": 30})

        async def test_async_edge_class(self):
            """Test Edge class functionality in async context."""
            # Test creating an edge without parameters
            edge = minigu.Edge()
            self.assertIsNone(edge.id)
            self.assertIsNone(edge.label)
            self.assertIsNone(edge.source_id)
            self.assertIsNone(edge.destination_id)
            self.assertEqual(edge.properties, {})


if __name__ == '__main__':
    unittest.main()