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
# 创建Python虚拟环境
python -m venv .venv
# 激活虚拟环境 (兼容Linux/macOS和Windows)
if [ -f ".venv/bin/activate" ]; then
    source .venv/bin/activate
elif [ -f ".venv/Scripts/activate" ]; then
    source .venv/Scripts/activate
fi
# 升级pip并安装maturin
pip install --upgrade pip
pip install maturin
# 使用maturin构建Python模块
maturin develop
# 运行Python测试
python test_minigu_api.py
echo "Python API tests completed."