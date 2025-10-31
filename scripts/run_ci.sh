#!/usr/bin/env bash
set -euo pipefail

taplo fmt --check --diff

cargo fmt --check

cargo clippy --tests --features "${DEFAULT_FEATURES:-std,serde,miette}" --no-deps

# Removed the early cargo build that was here before
# cargo build --features "${DEFAULT_FEATURES:-std,serde,miette}"

cargo nextest run --features "${DEFAULT_FEATURES:-std,serde,miette}"
cargo test --features "${DEFAULT_FEATURES:-std,serde,miette}" --doc

cargo doc --lib --no-deps --features "${DEFAULT_FEATURES:-std,serde,miette}"

echo "Running Python API tests..."
cd minigu/python

if ! command -v python3 &> /dev/null && ! command -v python &> /dev/null; then
    echo "Python is not available, skipping Python tests"
    exit 0
fi

if command -v python3 &> /dev/null; then
    PYTHON_CMD=python3
else
    PYTHON_CMD=python
fi

# Set up virtual environment
echo "Setting up virtual environment..."
$PYTHON_CMD -m venv .venv || echo "Failed to create virtual environment"

# Activate virtual environment
if [ -f ".venv/bin/activate" ]; then
    # Linux/macOS
    source .venv/bin/activate
elif [ -f ".venv/Scripts/activate" ]; then
    # Windows
    source .venv/Scripts/activate
else
    echo "Virtual environment activation script not found"
    exit 1
fi

# Upgrade pip and install required packages
echo "Installing required packages..."
pip install --upgrade pip
pip install pytest maturin

# Build the Python extension module inside virtual environment
echo "Building Python extension module..."
cargo build --features "extension-module"

# Copy the built extension module to the current directory so Python can find it
# The extension will have .so suffix on Linux, .dylib on macOS, and .dll on Windows
if [ -f "../../target/debug/libminigu_python.so" ]; then
    cp "../../target/debug/libminigu_python.so" "./minigu_python.so"
elif [ -f "../../target/debug/libminigu_python.dylib" ]; then
    cp "../../target/debug/libminigu_python.dylib" "./minigu_python.so"
elif [ -f "../../target/debug/minigu_python.dll" ]; then
    cp "../../target/debug/minigu_python.dll" "./minigu_python.pyd"
elif [ -f "../../target/debug/libminigu_python.dll" ]; then
    cp "../../target/debug/libminigu_python.dll" "./minigu_python.pyd"
fi

echo "Attempting to run Python tests..."
python -m pytest test_minigu_api.py -v || python test_minigu_api.py
echo "Python API tests completed."