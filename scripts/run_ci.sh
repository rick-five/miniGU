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
if ! command -v python3 &> /dev/null && ! command -v python &> /dev/null; then
    echo "Python is not available, skipping Python tests"
    exit 0
fi

# 确定使用的Python命令
if command -v python3 &> /dev/null; then
    PYTHON_CMD=python3
else
    PYTHON_CMD=python
fi

# 检查pip是否可用
if ! command -v pip3 &> /dev/null && ! command -v pip &> /dev/null; then
    echo "pip is not available, trying to install pip..."
    # 尝试安装pip
    if command -v apt-get &> /dev/null; then
        apt-get update && apt-get install -y python3-pip
    elif command -v yum &> /dev/null; then
        yum install -y python3-pip
    else
        echo "Cannot install pip, skipping Python tests"
        exit 0
    fi
fi

# 确定使用的pip命令
if command -v pip3 &> /dev/null; then
    PIP_CMD=pip3
else
    PIP_CMD=pip
fi

# 安装maturin
$PIP_CMD install maturin

# 尝试使用maturin build方式
set +e  # 关闭错误退出，以便我们可以处理错误
maturin build
BUILD_RESULT=$?
set -e  # 重新启用错误退出

if [ $BUILD_RESULT -eq 0 ]; then
    # 查找并安装wheel文件
    WHEEL_FILE=$(find target/wheels -name "*.whl" 2>/dev/null | head -n 1)
    if [ -n "$WHEEL_FILE" ] && [ -f "$WHEEL_FILE" ]; then
        $PIP_CMD install "$WHEEL_FILE"
        echo "Successfully installed wheel file"
    else
        echo "No wheel file found, trying maturin develop"
        maturin develop
    fi
else
    echo "maturin build failed, trying maturin develop"
    # 尝试直接使用maturin develop
    maturin develop
fi

# 运行Python测试
$PYTHON_CMD test_minigu_api.py
echo "Python API tests completed."