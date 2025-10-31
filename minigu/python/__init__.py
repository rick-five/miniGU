# Main module initialization file
# Defer the import of the Rust extension to avoid segfaults during module initialization

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
)

# Try to import from the safe wrapper module
HAS_RUST_BINDINGS = False
PyMiniGU = None
is_syntax_error = None
is_timeout_error = None
is_transaction_error = None
is_not_implemented_error = None

try:
    from . import minigu_python
    HAS_RUST_BINDINGS = minigu_python.HAS_RUST_BINDINGS
    PyMiniGU = minigu_python.PyMiniGU
    
    # Try to import utility functions
    try:
        is_syntax_error = minigu_python.is_syntax_error
        is_timeout_error = minigu_python.is_timeout_error
        is_transaction_error = minigu_python.is_transaction_error
        is_not_implemented_error = minigu_python.is_not_implemented_error
    except AttributeError:
        pass
        
except (ImportError, AttributeError):
    # Fallback if the Rust extension is not available or fails to load
    try:
        import minigu_python
        HAS_RUST_BINDINGS = True
        PyMiniGU = minigu_python.PyMiniGU
        
        # Try to import utility functions
        try:
            is_syntax_error = minigu_python.is_syntax_error
            is_timeout_error = minigu_python.is_timeout_error
            is_transaction_error = minigu_python.is_transaction_error
            is_not_implemented_error = minigu_python.is_not_implemented_error
        except AttributeError:
            pass
    except (ImportError, Exception):
        HAS_RUST_BINDINGS = False
        PyMiniGU = None

# Re-export the HAS_RUST_BINDINGS flag and PyMiniGU class
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