# Main module initialization file
try:
    from .minigu_python import PyMiniGU
    HAS_RUST_BINDINGS = True
except ImportError:
    # Fallback if the Rust extension is not available
    PyMiniGU = None
    HAS_RUST_BINDINGS = False

# Expose public API
from .minigu import (
    Vertex,
    Edge,
    Path,
    MiniGUError,
    ConnectionError,
    QueryError,
    QuerySyntaxError,
    QueryExecutionError,
    QueryTimeoutError,
    DataError,
    GraphError,
    TransactionError,
    QueryResult,
    AsyncMiniGU,
    MiniGU,
    connect,
    async_connect,
)

__all__ = [
    'PyMiniGU',
    'HAS_RUST_BINDINGS',
    'Vertex',
    'Edge',
    'Path',
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
    'AsyncMiniGU',
    'MiniGU',
    'connect',
    'async_connect',
]