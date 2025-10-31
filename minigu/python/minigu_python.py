"""
Safe wrapper for the minigu_python Rust extension.

This module provides a safe way to import and use the minigu_python Rust extension,
handling potential import errors and segfaults gracefully.
"""

# Attempt to import the Rust extension
try:
    import minigu_python
    HAS_RUST_BINDINGS = True
    PyMiniGU = minigu_python.PyMiniGU
    
    # Re-export utility functions if they exist
    if hasattr(minigu_python, 'is_syntax_error'):
        is_syntax_error = minigu_python.is_syntax_error
    if hasattr(minigu_python, 'is_timeout_error'):
        is_timeout_error = minigu_python.is_timeout_error
    if hasattr(minigu_python, 'is_transaction_error'):
        is_transaction_error = minigu_python.is_transaction_error
    if hasattr(minigu_python, 'is_not_implemented_error'):
        is_not_implemented_error = minigu_python.is_not_implemented_error
        
    # Make sure we export everything
    __all__ = ['HAS_RUST_BINDINGS', 'PyMiniGU']
    if 'is_syntax_error' in globals():
        __all__.append('is_syntax_error')
    if 'is_timeout_error' in globals():
        __all__.append('is_timeout_error')
    if 'is_transaction_error' in globals():
        __all__.append('is_transaction_error')
    if 'is_not_implemented_error' in globals():
        __all__.append('is_not_implemented_error')
        
except (ImportError, Exception) as e:
    # If import fails, set safe defaults
    HAS_RUST_BINDINGS = False
    PyMiniGU = None
    is_syntax_error = None
    is_timeout_error = None
    is_transaction_error = None
    is_not_implemented_error = None
    
    __all__ = ['HAS_RUST_BINDINGS', 'PyMiniGU', 'is_syntax_error', 
               'is_timeout_error', 'is_transaction_error', 'is_not_implemented_error']
    
    # Log the error for debugging
    import logging
    logging.getLogger(__name__).debug(f"Failed to import minigu_python: {e}")