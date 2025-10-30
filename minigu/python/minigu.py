"""
miniGU Python API

This module provides Python bindings for the miniGU graph database.
"""

import sys
from typing import Optional, List, Dict, Any, Union
from pathlib import Path
import json
import asyncio

# Import from package __init__.py with fallback for direct execution
try:
    from . import HAS_RUST_BINDINGS, PyMiniGU
except ImportError:
    # Fallback when running directly
    try:
        from minigu_python import PyMiniGU
        HAS_RUST_BINDINGS = True
    except (ImportError, ModuleNotFoundError):
        HAS_RUST_BINDINGS = False
        PyMiniGU = None


class Vertex:
    """Graph vertex representation"""
    
    def __init__(self, label: str, properties: Optional[Dict[str, Any]] = None):
        """
        Initialize a vertex
        
        Args:
            label: Vertex label
            properties: Vertex properties as key-value pairs
        """
        self.label = label
        self.properties = properties or {}
    
    def __repr__(self) -> str:
        return f"Vertex(label='{self.label}', properties={self.properties})"


class Edge:
    """Graph edge representation"""
    
    def __init__(self, label: str, src: Union[Vertex, int], dst: Union[Vertex, int], 
                 properties: Optional[Dict[str, Any]] = None):
        """
        Initialize an edge
        
        Args:
            label: Edge label
            src: Source vertex or vertex ID
            dst: Destination vertex or vertex ID
            properties: Edge properties as key-value pairs
        """
        self.label = label
        self.src = src
        self.dst = dst
        self.properties = properties or {}
    
    def __repr__(self) -> str:
        return f"Edge(label='{self.label}', src={self.src}, dst={self.dst}, properties={self.properties})"


class Path:
    """Graph path representation"""
    
    def __init__(self, nodes: List[Vertex], edges: List[Edge]):
        """
        Initialize a path
        
        Args:
            nodes: List of vertices in the path
            edges: List of edges in the path
        """
        self.nodes = nodes
        self.edges = edges
    
    def __repr__(self) -> str:
        return f"Path(nodes={len(self.nodes)}, edges={len(self.edges)})"


class MiniGUError(Exception):
    """Base exception class for miniGU database"""
    pass


class ConnectionError(MiniGUError):
    """Database connection error"""
    pass


class QueryError(MiniGUError):
    """Base query execution error"""
    pass


class QuerySyntaxError(QueryError):
    """Query syntax error"""
    pass


class QueryExecutionError(QueryError):
    """Query execution error"""
    pass


class QueryTimeoutError(QueryError):
    """Query timeout error"""
    pass


class DataError(MiniGUError):
    """Data loading/saving error"""
    pass


class GraphError(MiniGUError):
    """Graph creation/manipulation error"""
    pass


class TransactionError(MiniGUError):
    """Transaction error"""
    pass


class QueryResult:
    """
    Query result class
    """
    
    def __init__(self, schema: Optional[List[Dict[str, Any]]] = None, 
                 data: Optional[List[List[Any]]] = None,
                 metrics: Optional[Dict[str, float]] = None):
        self.schema = schema or []
        self.data = data or []
        self.metrics = metrics or {}
        self.row_count = len(self.data)
    
    def to_list(self) -> List[Dict[str, Any]]:
        """
        Convert the result to a list of dictionaries format
        
        Returns:
            List of dictionaries, with each row as a dictionary
        """
        if not self.schema or not self.data:
            return []
        
        column_names = [col["name"] for col in self.schema]
        return [dict(zip(column_names, row)) for row in self.data]
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the result to dictionary format
        
        Returns:
            Dictionary containing schema, data, and metrics
        """
        return {
            "schema": self.schema,
            "data": self.data,
            "metrics": self.metrics,
            "row_count": self.row_count
        }
    
    def __repr__(self) -> str:
        return f"QueryResult(rows={self.row_count}, columns={len(self.schema)})"


class AsyncMiniGU:
    """
    Asynchronous Python wrapper for miniGU graph database.
    
    Provides an asynchronous Pythonic interface to the miniGU graph database with support for
    graph creation, data loading, querying, and transaction management.
    """
    
    def __init__(self):
        """Initialize AsyncMiniGU instance."""
        self._rust_instance = None
        self.is_connected = False
        self._loop = asyncio.get_event_loop()
        self._connect()
    
    def _connect(self) -> None:
        """Establish connection to the database."""
        try:
            if HAS_RUST_BINDINGS:
                self._rust_instance = PyMiniGU()
                self._rust_instance.init()
                self.is_connected = True
                print("Session initialized successfully")
                print("Database connected")
            else:
                raise RuntimeError("Rust bindings not available")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {str(e)}")
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def close(self) -> None:
        """Close the database connection."""
        if self._rust_instance:
            self._rust_instance.close()
        self.is_connected = False
    
    async def execute(self, query: str) -> QueryResult:
        """
        Execute GQL query asynchronously.

        Args:
            query: GQL query statement

        Returns:
            Query result

        Raises:
            MiniGUError: Raised when database is not connected
            QuerySyntaxError: Raised when query has syntax errors
            QueryExecutionError: Raised when query execution fails
            QueryTimeoutError: Raised when query times out
        """
        
        if not self.is_connected:
            raise MiniGUError("Database not connected")
        
        if HAS_RUST_BINDINGS and self._rust_instance:
            try:
                result = self._rust_instance.execute(query)
                
                return QueryResult(
                    schema=result.get("schema", []),
                    data=result.get("data", []),
                    metrics=result.get("metrics", {})
                )
            except Exception as e:
                # Throw more precise exceptions based on specific error types
                error_str = str(e).lower()
                if "syntax" in error_str or "unexpected" in error_str:
                    raise QuerySyntaxError(f"Query syntax error: {str(e)}")
                elif "timeout" in error_str:
                    raise QueryTimeoutError(f"Query timeout: {str(e)}")
                elif "plan error" in error_str:
                    raise QueryExecutionError(f"Query planning failed: {str(e)}")
                else:
                    raise QueryExecutionError(f"Query execution failed: {str(e)}")
        else:
            raise RuntimeError("Rust bindings required for database operations")
    
    async def create_graph(self, name: str, schema: Optional[Dict] = None) -> None:
        """
        Create a graph database asynchronously.
        
        Args:
            name: Graph name
            schema: Graph schema definition (optional)
            
        Raises:
            MiniGUError: Raised when database is not connected
            GraphError: Raised when graph creation fails
        """
        
        if not self.is_connected:
            raise MiniGUError("Database not connected")
        
        if HAS_RUST_BINDINGS and self._rust_instance:
            try:
                # Use GQL syntax to create graph
                query = f"CALL create_test_graph('{name}')"
                await self.execute(query)
                print(f"Graph '{name}' created successfully")
            except Exception as e:
                raise GraphError(f"Graph creation failed: {str(e)}")
        else:
            raise RuntimeError("Rust bindings required for database operations")
    
    async def begin_transaction(self) -> None:
        """
        Begin a transaction asynchronously.

        Raises:
            MiniGUError: Raised when database is not connected
            TransactionError: Raised when transaction operations fail
        """
        if not self.is_connected:
            raise MiniGUError("Database not connected")
        
        if HAS_RUST_BINDINGS and self._rust_instance:
            try:
                # Use GQL syntax to begin transaction
                query = "BEGIN"
                await self.execute(query)
            except Exception as e:
                raise TransactionError(f"Failed to begin transaction: {str(e)}")
        else:
            raise RuntimeError("Rust bindings required for database operations")
    
    async def commit(self) -> None:
        """
        Commit the current transaction asynchronously.
        
        Raises:
            MiniGUError: Raised when database is not connected
            TransactionError: Raised when commit fails
        """
        if not self.is_connected:
            raise MiniGUError("Database not connected")
        
        if HAS_RUST_BINDINGS and self._rust_instance:
            try:
                self._rust_instance.commit()
            except Exception as e:
                raise TransactionError(f"Failed to commit transaction: {str(e)}")
        else:
            raise RuntimeError("Rust bindings required for database operations")
    
    async def rollback(self) -> None:
        """
        Rollback the current transaction asynchronously.
        
        Raises:
            MiniGUError: Raised when database is not connected
            TransactionError: Raised when rollback fails
        """
        if not self.is_connected:
            raise MiniGUError("Database not connected")
        
        if HAS_RUST_BINDINGS and self._rust_instance:
            try:
                self._rust_instance.rollback()
            except Exception as e:
                raise TransactionError(f"Failed to rollback transaction: {str(e)}")
        else:
            raise RuntimeError("Rust bindings required for database operations")


class MiniGU:
    """
    Python wrapper for miniGU graph database.
    
    Provides a Pythonic interface to the miniGU graph database with support for
    graph creation, data loading, querying, and transaction management.
    """
    
    def __init__(self):
        """Initialize MiniGU instance."""
        self._rust_instance = None
        self.is_connected = False
        self._connect()
    
    def _connect(self) -> None:
        """Establish connection to the database."""
        try:
            if HAS_RUST_BINDINGS:
                self._rust_instance = PyMiniGU()
                self._rust_instance.init()
                self.is_connected = True
                print("Session initialized successfully")
                print("Database connected")
            else:
                raise RuntimeError("Rust bindings not available")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database: {str(e)}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def close(self) -> None:
        """Close the database connection."""
        if self._rust_instance:
            self._rust_instance.close()
        self.is_connected = False
    
    def execute(self, query: str) -> QueryResult:
        """
        Execute GQL query.
        
        Args:
            query: GQL query statement
            
        Returns:
            Query result
            
        Raises:
            MiniGUError: Raised when database is not connected
            QuerySyntaxError: Raised when query has syntax errors
            QueryExecutionError: Raised when query execution fails
            QueryTimeoutError: Raised when query times out
        """
        if not self.is_connected:
            raise MiniGUError("Database not connected")
        
        if HAS_RUST_BINDINGS and self._rust_instance:
            # Execute query using Rust backend
            try:
                result_dict = self._rust_instance.execute(query)
                schema = result_dict.get("schema", [])
                data = result_dict.get("data", [])
                metrics = result_dict.get("metrics", {})
                return QueryResult(schema, data, metrics)
            except Exception as e:
                # Throw more precise exceptions based on specific error types
                error_str = str(e).lower()
                if "syntax" in error_str or "unexpected" in error_str:
                    raise QuerySyntaxError(f"Query syntax error: {str(e)}")
                elif "timeout" in error_str:
                    raise QueryTimeoutError(f"Query timeout: {str(e)}")
                elif "plan error" in error_str:
                    raise QueryExecutionError(f"Query planning failed: {str(e)}")
                else:
                    raise QueryExecutionError(f"Query execution failed: {str(e)}")
        else:
            raise RuntimeError("Rust bindings required for database operations")
    
    def create_graph(self, name: str, schema: Optional[Dict] = None) -> None:
        """
        Create a graph database
        
        Args:
            name: Graph name
            schema: Graph schema definition (optional)
            
        Raises:
            MiniGUError: Raised when database is not connected
            GraphError: Raised when graph creation fails
        """
        if not self.is_connected:
            raise MiniGUError("Database not connected")
        
        if HAS_RUST_BINDINGS and self._rust_instance:
            try:
                # Use GQL syntax to create graph
                query = f"CALL create_test_graph('{name}')"
                self.execute(query)
                print(f"Graph '{name}' created successfully")
            except Exception as e:
                raise GraphError(f"Graph creation failed: {str(e)}")
        else:
            raise RuntimeError("Rust bindings required for database operations")
    
    def begin_transaction(self) -> None:
        """
        Begin a transaction.
        
        Raises:
            MiniGUError: Raised when database is not connected
        """
        if not self.is_connected:
            raise MiniGUError("Database not connected")
        
        if HAS_RUST_BINDINGS and self._rust_instance:
            try:
                # Use GQL syntax to begin transaction
                query = "BEGIN"
                self.execute(query)
            except Exception as e:
                raise TransactionError(f"Failed to begin transaction: {str(e)}")
        else:
            raise RuntimeError("Rust bindings required for database operations")
    
    def commit(self) -> None:
        """
        Commit the current transaction.
        
        Raises:
            MiniGUError: Raised when database is not connected
        """
        if not self.is_connected:
            raise MiniGUError("Database not connected")
        
        if HAS_RUST_BINDINGS and self._rust_instance:
            try:
                self._rust_instance.commit()
            except Exception as e:
                raise TransactionError(f"Failed to commit transaction: {str(e)}")
        else:
            raise RuntimeError("Rust bindings required for database operations")
    
    def rollback(self) -> None:
        """
        Rollback the current transaction.
        
        Raises:
            MiniGUError: Raised when database is not connected
        """
        if not self.is_connected:
            raise MiniGUError("Database not connected")
        
        if HAS_RUST_BINDINGS and self._rust_instance:
            try:
                self._rust_instance.rollback()
            except Exception as e:
                raise TransactionError(f"Failed to rollback transaction: {str(e)}")
        else:
            raise RuntimeError("Rust bindings required for database operations")
    
    def load(self, data: Union[List[Dict], str, Path]) -> None:
        """
        Load data into the database
        
        Args:
            data: Data to load, can be a list of dictionaries or file path
            
        Raises:
            MiniGUError: Raised when database is not connected
            DataError: Raised when data loading fails
        """
        if not self.is_connected:
            raise MiniGUError("Database not connected")
        
        if HAS_RUST_BINDINGS and self._rust_instance:
            try:
                if isinstance(data, (str, Path)):
                    self._rust_instance.load_from_file(str(data))
                else:
                    self._rust_instance.load_data(data)
                print(f"Data loaded successfully")
            except Exception as e:
                raise DataError(f"Data loading failed: {str(e)}")
        else:
            raise RuntimeError("Rust bindings required for database operations")

    def save(self, path: str) -> None:
        """
        Save the database to the specified path
        
        Args:
            path: Save path
            
        Raises:
            MiniGUError: Raised when database is not connected
            DataError: Raised when save fails
        """
        if not self.is_connected:
            raise MiniGUError("Database not connected")
        
        if HAS_RUST_BINDINGS and self._rust_instance:
            try:
                self._rust_instance.save_to_file(path)
                print(f"Database saved to {path}")
            except Exception as e:
                raise DataError(f"Database save failed: {str(e)}")
        else:
            raise RuntimeError("Rust bindings required for database operations")


def connect() -> MiniGU:
    """
    Create a connection to the miniGU database.
    
    Returns:
        MiniGU database connection object
    """
    return MiniGU()


async def async_connect(db_path: Optional[str] = None,
                        thread_count: int = 1,
                        cache_size: int = 1000,
                        enable_logging: bool = False) -> AsyncMiniGU:
    """
    Create an asynchronous connection to the miniGU database.
    
    Args:
        db_path: Database file path, if None creates an in-memory database
        thread_count: Number of threads for parallel execution
        cache_size: Size of the query result cache
        enable_logging: Whether to enable query execution logging
        
    Returns:
        AsyncMiniGU database connection object
    """
    return AsyncMiniGU(db_path, thread_count, cache_size, enable_logging)