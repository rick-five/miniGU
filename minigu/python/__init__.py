# Main module initialization file
try:
    from .minigu_python import PyMiniGU
    HAS_RUST_BINDINGS = True
    __all__ = ['PyMiniGU', 'HAS_RUST_BINDINGS']
except ImportError:
    # Fallback if the Rust extension is not available
    HAS_RUST_BINDINGS = False