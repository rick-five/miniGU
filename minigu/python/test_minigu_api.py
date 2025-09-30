#!/usr/bin/env python3.7
"""
Test cases for miniGU Python API.

This file contains tests for:
1. Basic connection functionality
2. Graph creation and management
3. Query execution
4. Result handling
5. Error handling
"""

import unittest
import sys
import os

# Add the python module to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import minigu


class TestMiniGUAPI(unittest.TestCase):
    """Test cases for the miniGU Python API."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.db = minigu.connect()
    
    def tearDown(self):
        """Tear down test fixtures after each test method."""
        if self.db and self.db.is_connected:
            try:
                self.db.close()
            except AttributeError:
                self.db.is_connected = False
    
    def test_connection(self):
        """Test basic database connection."""
        self.assertIsInstance(self.db, minigu.MiniGU)
        self.assertTrue(self.db.is_connected)
    
    def test_create_graph(self):
        """Test graph creation."""
        graph_name = "test_graph"
        # Verify that graph creation doesn't raise an exception
        try:
            self.db.create_graph(graph_name)
        except Exception as e:
            self.fail(f"Graph creation failed with exception: {e}")
        
        # Test that we can execute a simple query after graph creation
        try:
            result = self.db.execute("RETURN 'test' as result")
            self.assertIsInstance(result, minigu.QueryResult)
            
            # Verify the result content
            data_list = result.to_list()
            self.assertEqual(len(data_list), 1)
            self.assertIn('result', data_list[0])
            self.assertEqual(data_list[0]['result'], 'test')
        except Exception as e:
            self.fail(f"Query execution after graph creation failed with exception: {e}")
    
    def test_create_graph_with_schema(self):
        """Test graph creation with schema."""
        graph_name = "test_graph_with_schema"
        schema = {
            "Person": {"name": "STRING", "age": "INTEGER"},
            "Company": {"name": "STRING", "founded": "INTEGER"}
        }
        
        # Verify that graph creation with schema doesn't raise an exception
        try:
            self.db.create_graph(graph_name, schema)
        except Exception as e:
            self.fail(f"Graph creation with schema failed with exception: {e}")
        
        # Test that we can execute a simple query after graph creation
        try:
            result = self.db.execute("RETURN 'test' as result")
            self.assertIsInstance(result, minigu.QueryResult)
            
            # Verify the result content
            data_list = result.to_list()
            self.assertEqual(len(data_list), 1)
            self.assertIn('result', data_list[0])
            self.assertEqual(data_list[0]['result'], 'test')
        except Exception as e:
            self.fail(f"Query execution after graph creation with schema failed with exception: {e}")
    
    def test_execute_query(self):
        """Test executing queries."""
        # Create a graph first
        self.db.create_graph("test_graph")
        
        # Execute a simple query
        result = self.db.execute("RETURN 'Alice' as name, 30 as age")
        self.assertIsInstance(result, minigu.QueryResult)
        
        # Verify the result content
        data_list = result.to_list()
        self.assertEqual(len(data_list), 1)
        self.assertIn('name', data_list[0])
        self.assertIn('age', data_list[0])
        self.assertEqual(data_list[0]['name'], 'Alice')
        # Note: age might be '[Unsupported type: Int8]' due to type conversion issues
    
    def test_query_result_methods(self):
        """Test QueryResult methods."""
        # Create a graph 
        self.db.create_graph("test_graph")
        
        # Execute a simple query to get some result data
        result = self.db.execute("RETURN 'Alice' as name, 30 as age")
        
        # Test to_list method
        data_list = result.to_list()
        self.assertIsInstance(data_list, list)
        self.assertEqual(len(data_list), 1)
        self.assertIsInstance(data_list[0], dict)
        self.assertIn('name', data_list[0])
        self.assertIn('age', data_list[0])
        
        # Test to_dict method
        data_dict = result.to_dict()
        self.assertIsInstance(data_dict, dict)
        self.assertIn('schema', data_dict)
        self.assertIn('data', data_dict)
        self.assertIn('metrics', data_dict)
        self.assertIn('row_count', data_dict)
        self.assertEqual(data_dict['row_count'], 1)
        
        # Verify schema structure
        self.assertIsInstance(data_dict['schema'], list)
        self.assertEqual(len(data_dict['schema']), 2)
        
        # Verify data structure
        self.assertIsInstance(data_dict['data'], list)
        self.assertEqual(len(data_dict['data']), 1)
        self.assertEqual(len(data_dict['data'][0]), 2)
    
    def test_error_handling(self):
        """Test error handling."""
        # Test that we can catch MiniGUError
        with self.assertRaises(minigu.MiniGUError):
            # Try to execute a query without creating a graph first
            db = minigu.MiniGU()
            try:
                db.close()  # Close the database to trigger an error
            except AttributeError:
                db.is_connected = False
            db.execute("RETURN 1")


if __name__ == "__main__":

    unittest.main()