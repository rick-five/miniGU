# Main module initialization file
# Defer the import of the Rust extension to avoid segfaults during module initialization
HAS_RUST_BINDINGS = False
PyMiniGU = None

try:
    from .minigu_python import HAS_RUST_BINDINGS, PyMiniGU
except (ImportError, Exception):
    # Fallback if the Rust extension is not available or fails to load
    try:
        import minigu_python
        HAS_RUST_BINDINGS = True
        PyMiniGU = minigu_python.PyMiniGU
    except (ImportError, Exception):
        HAS_RUST_BINDINGS = False
        PyMiniGU = None

# Import the main classes and functions to expose them publicly
from .minigu import (
    MiniGU,
    AsyncMiniGU,
    connect,
    async_connect,
    MiniGUError,
    QuerySyntaxError,
    TimeoutError,
    TransactionError,
    NotImplementedError,
    is_syntax_error,
    is_timeout_error,
    is_transaction_error,
    is_not_implemented_error,
)

# Export everything
__all__ = [
    "MiniGU",
    "AsyncMiniGU",
    "connect",
    "async_connect",
    "MiniGUError",
    "QuerySyntaxError",
    "TimeoutError",
    "TransactionError",
    "NotImplementedError",
    "is_syntax_error",
    "is_timeout_error",
    "is_transaction_error",
    "is_not_implemented_error",
    "HAS_RUST_BINDINGS",
    "PyMiniGU",
]