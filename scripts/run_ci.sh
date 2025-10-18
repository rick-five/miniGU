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

# 检查系统类型，如果是Linux则确保安装了venv模块
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    if ! $PYTHON_CMD -c "import venv" &> /dev/null; then
        echo "Python venv module not available, trying to install python3-venv"
        if command -v apt-get &> /dev/null; then
            sudo apt-get update
            sudo apt-get install -y python3-venv
        else
            echo "Cannot install python3-venv, skipping Python tests"
            exit 0
        fi
    fi
fi

# 创建虚拟环境并激活
$PYTHON_CMD -m venv .venv

# 检查虚拟环境激活脚本并激活
if [ -f ".venv/bin/activate" ]; then
    # Linux/macOS
    source .venv/bin/activate
elif [ -f ".venv/Scripts/activate" ]; then
    # Windows
    source .venv/Scripts/activate
else
    echo "Cannot find virtual environment activation script, skipping Python tests"
    exit 0
fi

# 升级pip并安装maturin
python -m pip install --upgrade pip
pip install maturin

# 构建Python扩展
maturin develop

# 运行Python测试
python test_minigu_api.py
echo "Python API tests completed."