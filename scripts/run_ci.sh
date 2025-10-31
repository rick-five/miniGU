#!/usr/bin/env bash
set -euo pipefail

taplo fmt --check --diff

cargo fmt --check

cargo clippy --tests --features "${DEFAULT_FEATURES:-std,serde,miette}" --no-deps

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

# Check architecture consistency
echo "Checking system and Python architecture consistency..."
echo "Runner Architecture:"
uname -m
echo "Python Interpreter Arch:"
python -c "import platform; print(platform.machine())"
echo "Python executable path:"
which python
echo "Python version:"
python --version

# Upgrade pip and install required packages
echo "Installing required packages..."
pip install --upgrade pip
pip install maturin pytest

# Build the Python extension module using maturin (方式一)
echo "Building Python extension module with maturin..."
python -m maturin develop --release

# Diagnostic step: Create a minimal test script and run it
echo "Running minimal import test..."
cat > minimal_test.py <<EOF
import sys
print("Python executable:", sys.executable)
print("Python path:", sys.path)

try:
    import minigu_python
    print("SUCCESS: minigu_python imported successfully.")
    # Try to instantiate PyMiniGU to ensure class registration also works
    try:
        db = minigu_python.PyMiniGU()
        print("SUCCESS: PyMiniGU instantiated.")
    except Exception as e:
        print(f"FAILURE: Instantiation failed with error: {e}")
        import traceback
        traceback.print_exc()
except Exception as e:
    print(f"FAILURE: Import failed with error: {e}")
    import traceback
    traceback.print_exc()
EOF

# Use VENV Python to run the script
echo "Executing minimal test..."
python minimal_test.py

echo "Attempting to run Python tests..."
python -m pytest test_minigu_api.py -v
echo "Python API tests completed."