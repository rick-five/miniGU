#!/usr/bin/env bash
set -euo pipefail

# TOML 格式检查
taplo fmt --check --diff

# 代码格式检查
cargo fmt --check

# Clippy 静态检查
cargo clippy --tests --features "${DEFAULT_FEATURES:-std,serde,miette}" --no-deps

# 构建
cargo build --features "${DEFAULT_FEATURES:-std,serde,miette}"

# 测试
cargo nextest run --features "${DEFAULT_FEATURES:-std,serde,miette}"
cargo test --features "${DEFAULT_FEATURES:-std,serde,miette}" --doc

# 文档构建
cargo doc --lib --no-deps --features "${DEFAULT_FEATURES:-std,serde,miette}"

# Python API 测试
echo "Running Python API tests..."
cd minigu/python
# 检查Python是否可用
if ! command -v python &> /dev/null && ! command -v python3 &> /dev/null; then
    echo "Python is not available, skipping Python tests"
    exit 0
fi
# 确保使用python3命令
if command -v python3 &> /dev/null; then
    PYTHON_CMD=python3
else
    PYTHON_CMD=python
fi
# 安装maturin
pip install maturin
# 使用maturin构建wheel并安装
maturin build
# 查找构建的wheel文件并安装
WHEEL_FILE=$(find target/wheels -name "*.whl" | head -n 1)
if [ -n "$WHEEL_FILE" ]; then
    pip install "$WHEEL_FILE"
else
    echo "No wheel file found, trying maturin develop as fallback"
    maturin develop
fi
# 运行Python测试
$PYTHON_CMD test_minigu_api.py
echo "Python API tests completed."