#!/usr/bin/env bash
set -euo pipefail

# Basic checks
cargo fmt --check
cargo clippy --tests --features "${DEFAULT_FEATURES:-std,serde,miette}" --no-deps

# Run tests
cargo nextest run --features "${DEFAULT_FEATURES:-std,serde,miette}"
cargo test --features "${DEFAULT_FEATURES:-std,serde,miette}" --doc

# Build documentation
cargo doc --lib --no-deps --features "${DEFAULT_FEATURES:-std,serde,miette}"

echo "Running Python API tests..."
cd minigu/python

# Set up virtual environment
echo "Setting up virtual environment..."
python -m venv .venv

# Activate virtual environment
if [ -f ".venv/bin/activate" ]; then
    # Linux/macOS
    source .venv/bin/activate
elif [ -f ".venv/Scripts/activate" ]; then
    # Windows
    source .venv/Scripts/activate
fi

# Upgrade pip and install required packages
echo "Installing required packages..."
pip install --upgrade pip
pip install maturin pytest

# Build the Python extension module using maturin (方式一)
echo "Building Python extension module with maturin..."
python -m maturin develop --release || echo "Maturin build failed, but continuing..."

echo "Attempting to run Python tests..."
python -m pytest test_minigu_api.py -v
echo "Python API tests completed."