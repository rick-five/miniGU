"""
miniGU Python API Package

This package provides Python bindings for the miniGU graph database.
It offers both synchronous and asynchronous interfaces for interacting with
the graph database, including graph creation, data loading, querying, and
transaction management.

Stability:
    This API is currently in alpha state. Features may change in future versions.
    
Features:
    - Synchronous and asynchronous interfaces
    - Graph creation and management
    - Data loading from various sources
    - GQL query execution
    - Transaction support (planned)
    - Security features including input sanitization

Example:
    >>> from minigu import MiniGU
    >>> db = MiniGU()
    >>> db.create_graph("my_graph")
    >>> result = db.execute("MATCH (n) RETURN n LIMIT 10")
"""

# Main module initialization file
try:
    import minigu_python
    HAS_RUST_BINDINGS = True
    PyMiniGU = minigu_python.PyMiniGU
except ImportError as e:
    # Fallback if the Rust extension is not available
    HAS_RUST_BINDINGS = False
    PyMiniGU = None
    # Print the actual error for debugging purposes
    print(f"Warning: Failed to import Rust extension: {e}")

# Import the main classes and functions to expose them publicly
from .minigu import (
    MiniGU,
    AsyncMiniGU,
    connect,
    async_connect,
    MiniGUError,
    ConnectionError,
    QueryError,
    QuerySyntaxError,
    QueryExecutionError,
    QueryTimeoutError,
    DataError,
    GraphError,
    TransactionError,
    QueryResult
)

# Export everything
__all__ = [
    'MiniGU',
    'AsyncMiniGU',
    'connect',
    'async_connect',
    'MiniGUError',
    'ConnectionError',
    'QueryError',
    'QuerySyntaxError',
    'QueryExecutionError',
    'QueryTimeoutError',
    'DataError',
    'GraphError',
    'TransactionError',
    'QueryResult',
    'PyMiniGU',
    'HAS_RUST_BINDINGS'
]

__version__ = "0.1.0"
__author__ = "miniGU Team"