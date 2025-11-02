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
        # This should work without throwing exceptions
        self.db.create_graph("test_graph")
        # If we reach here, the test passes

    def test_create_graph_with_special_chars(self):
        """Test creating a graph with special characters in the name."""
        # This should sanitize the name and not throw exceptions
        self.db.create_graph("test_graph_with_special_chars_123")
        # If we reach here, the test passes

    def test_create_graph_with_injection_attempt(self):
        """Test creating a graph with potential injection attempts."""
        # This should sanitize the name and not throw exceptions
        self.db.create_graph("test_graph'; DROP TABLE users; --")
        # If we reach here, the test passes

    def test_begin_transaction(self):
        """Test beginning a transaction."""
        self.db.create_graph("test_graph_for_transaction")
        # This should raise TransactionError as the feature is not yet implemented
        with self.assertRaises(minigu.TransactionError):
            self.db.begin_transaction()

    def test_commit_transaction(self):
        """Test committing a transaction."""
        self.db.create_graph("test_graph_for_commit")
        # This should raise TransactionError as the feature is not yet implemented
        with self.assertRaises(minigu.TransactionError):
            self.db.commit()

    def test_rollback_transaction(self):
        """Test rolling back a transaction."""
        self.db.create_graph("test_graph_for_rollback")
        # This should raise TransactionError as the feature is not yet implemented
        with self.assertRaises(minigu.TransactionError):
            self.db.rollback()

    def test_transaction_methods(self):
        """Test transaction methods existence and basic functionality."""
        # Check that transaction methods exist
        self.assertTrue(hasattr(self.db, 'begin_transaction'))
        self.assertTrue(hasattr(self.db, 'commit'))
        self.assertTrue(hasattr(self.db, 'rollback'))

        # Test that transaction methods raise TransactionError as they are not yet implemented
        self.db.create_graph("test_graph_for_methods")
        with self.assertRaises(minigu.TransactionError):
            self.db.begin_transaction()

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
            await self.db.create_graph("test_async_graph")
            # If no exception is raised, the test passes

        async def test_async_create_graph_with_special_chars(self):
            """Test creating a graph with special characters in the name asynchronously."""
            await self.db.create_graph("test_async_graph_with_special_chars_123")
            # If no exception is raised, the test passes

        async def test_async_create_graph_with_injection_attempt(self):
            """Test creating a graph with potential injection attempts asynchronously."""
            await self.db.create_graph("test_async_graph'; DROP TABLE users; --")
            # If no exception is raised, the test passes

        async def test_async_begin_transaction(self):
            """Test beginning a transaction asynchronously."""
            await self.db.create_graph("test_async_transaction_graph")
            # This should raise TransactionError as the feature is not yet implemented
            with self.assertRaises(minigu.TransactionError):
                await self.db.begin_transaction()

        async def test_async_commit_transaction(self):
            """Test committing a transaction asynchronously."""
            await self.db.create_graph("test_async_commit_graph")
            # This should raise TransactionError as the feature is not yet implemented
            with self.assertRaises(minigu.TransactionError):
                await self.db.commit()

        async def test_async_rollback_transaction(self):
            """Test rolling back a transaction asynchronously."""
            await self.db.create_graph("test_async_rollback_graph")
            # This should raise TransactionError as the feature is not yet implemented
            with self.assertRaises(minigu.TransactionError):
                await self.db.rollback()


if __name__ == '__main__':
    unittest.main()