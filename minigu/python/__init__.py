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