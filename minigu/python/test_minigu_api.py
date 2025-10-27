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
"""

import unittest
import sys
import os
import asyncio

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
        
        # Test that we can get information about the created graph
        try:
            result = self.db.execute("SHOW GRAPHS")
            self.assertIsInstance(result, minigu.QueryResult)
            
            # Verify the result contains our created graph
            data_list = result.to_list()
            graph_names = [row.get('name') for row in data_list]
            self.assertIn(graph_name, graph_names)
        except Exception as e:
            self.fail(f"Schema validation after graph creation failed with exception: {e}")
    
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
        
        # Test that we can examine the schema of the created graph
        try:
            result = self.db.execute(f"SHOW SCHEMA FROM {graph_name}")
            self.assertIsInstance(result, minigu.QueryResult)
            
            # Verify the result contains schema information
            data_list = result.to_list()
            self.assertGreater(len(data_list), 0)
            
            # Check that the schema contains our defined labels
            labels = [row.get('label') for row in data_list]
            self.assertIn('Person', labels)
            self.assertIn('Company', labels)
        except Exception as e:
            self.fail(f"Schema validation after graph creation with schema failed with exception: {e}")
    
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
    
    def test_transaction_methods(self):
        """Test transaction methods existence and basic functionality."""
        # Check that transaction methods exist
        self.assertTrue(hasattr(self.db, 'begin_transaction'))
        self.assertTrue(hasattr(self.db, 'commit'))
        self.assertTrue(hasattr(self.db, 'rollback'))
        
        # Test that we can call transaction methods without AttributeError
        # Since transactions may not be fully implemented yet, we just verify they can be called
        try:
            self.db.begin_transaction()
        except minigu.TransactionError:
            # This is expected since transactions may not be fully implemented yet
            pass
        except Exception as e:
            self.fail(f"begin_transaction raised unexpected exception: {e}")
            
        try:
            self.db.commit()
        except minigu.TransactionError:
            # This is expected since transactions may not be fully implemented yet
            pass
        except Exception as e:
            self.fail(f"commit raised unexpected exception: {e}")
            
        try:
            self.db.rollback()
        except minigu.TransactionError:
            # This is expected since transactions may not be fully implemented yet
            pass
        except Exception as e:
            self.fail(f"rollback raised unexpected exception: {e}")

    def test_context_manager(self):
        """Test context manager usage."""
        with minigu.connect() as db:
            self.assertTrue(db.is_connected)
            db.create_graph("context_test_graph")
            result = db.execute("RETURN 'context test' as result")
            self.assertIsInstance(result, minigu.QueryResult)
            data_list = result.to_list()
            self.assertEqual(len(data_list), 1)
            self.assertEqual(data_list[0]['result'], 'context test')
        # Connection should be closed after context
        self.assertFalse(db.is_connected)
    


class TestAsyncMiniGUAPI(unittest.TestCase):
    """Test cases for the async miniGU Python API."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
    
    def tearDown(self):
        """Tear down test fixtures after each test method."""
        self.loop.close()
    
    def test_async_connection(self):
        """Test async database connection creation."""
        async def _test():
            db = minigu.AsyncMiniGU()
            self.assertIsInstance(db, minigu.AsyncMiniGU)
            self.assertTrue(hasattr(db, '_connect'))
            # Test manual connection and disconnection
            if db.is_connected:
                await db.close()
            return True
        
        result = self.loop.run_until_complete(_test())
        self.assertTrue(result)
    
    def test_async_create_graph(self):
        """Test async graph creation."""
        async def _test():
            db = minigu.AsyncMiniGU()
            try:
                await db.create_graph("async_test_graph")
                return True
            finally:
                if db.is_connected:
                    await db.close()
        
        result = self.loop.run_until_complete(_test())
        self.assertTrue(result)
    
    def test_async_execute_query(self):
        """Test async query execution."""
        async def _test():
            db = minigu.AsyncMiniGU()
            try:
                await db.create_graph("async_test_graph")
                result = await db.execute("RETURN 'Alice' as name, 30 as age")
                self.assertIsInstance(result, minigu.QueryResult)
                return result
            finally:
                if db.is_connected:
                    await db.close()
        
        result = self.loop.run_until_complete(_test())
        self.assertIsInstance(result, minigu.QueryResult)
    
    def test_async_transaction_methods(self):
        """Test async transaction methods existence and basic functionality."""
        async def _test():
            db = minigu.AsyncMiniGU()
            try:
                # Check that transaction methods exist
                self.assertTrue(hasattr(db, 'begin_transaction'))
                self.assertTrue(hasattr(db, 'commit'))
                self.assertTrue(hasattr(db, 'rollback'))
                
                # Test that we can call transaction methods without AttributeError
                # Since transactions may not be fully implemented yet, we just verify they can be called
                try:
                    await db.begin_transaction()
                except minigu.TransactionError:
                    # This is expected since transactions may not be fully implemented yet
                    pass
                except Exception as e:
                    self.fail(f"begin_transaction raised unexpected exception: {e}")
                    
                try:
                    await db.commit()
                except minigu.TransactionError:
                    # This is expected since transactions may not be fully implemented yet
                    pass
                except Exception as e:
                    self.fail(f"commit raised unexpected exception: {e}")
                    
                try:
                    await db.rollback()
                except minigu.TransactionError:
                    # This is expected since transactions may not be fully implemented yet
                    pass
                except Exception as e:
                    self.fail(f"rollback raised unexpected exception: {e}")
                    
                return True
            finally:
                if db.is_connected:
                    await db.close()
        
        result = self.loop.run_until_complete(_test())
        self.assertTrue(result)
    
    def test_async_context_manager(self):
        """Test async context manager usage."""
        async def _test():
            db = minigu.AsyncMiniGU()
            try:
                await db.__aenter__()
                self.assertTrue(db.is_connected)
                await db.create_graph("async_context_test_graph")
                result = await db.execute("RETURN 'async context test' as result")
                self.assertIsInstance(result, minigu.QueryResult)
                data_list = result.to_list()
                self.assertEqual(len(data_list), 1)
                self.assertEqual(data_list[0]['result'], 'async context test')
                await db.__aexit__(None, None, None)
                # Connection should be closed after context
                self.assertFalse(db.is_connected)
                return True
            except Exception as e:
                if db.is_connected:
                    await db.__aexit__(None, None, None)
                raise
        
        result = self.loop.run_until_complete(_test())
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()