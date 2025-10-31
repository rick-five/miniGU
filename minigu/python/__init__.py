# Main module initialization file
# Defer the import of the Rust extension to avoid segfaults during module initialization
HAS_RUST_BINDINGS = False
PyMiniGU = None

try:
    import minigu_python
    HAS_RUST_BINDINGS = True
    PyMiniGU = minigu_python.PyMiniGU
except (ImportError, Exception):
    # Fallback if the Rust extension is not available or fails to load
    HAS_RUST_BINDINGS = False
    PyMiniGU = None

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