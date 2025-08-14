# miniGU 介绍

[![Star](https://shields.io/github/stars/tugraph-family/miniGU?logo=startrek&label=Star&color=yellow)](https://github.com/TuGraph-family/miniGU/stargazers)
[![UT&&IT](https://github.com/TuGraph-family/miniGU/actions/workflows/ci.yml/badge.svg)](https://github.com/TuGraph-family/miniGU/actions/workflows/ci.yml)

MiniGU 是 [TuGraph](https://tugraph.tech) 团队基联合多所高校共建专为零基础的同学设计的图数据库、图计算技术入门学习项目。 

MiniGU 是一个基于 Rust 语言实现的图数据库，旨在帮助学习者快速掌握图数据库和图计算的基本概念和技术。它提供了一个简单易用的交互式 shell 环境，支持基本的图数据操作和查询。

注意：MiniGU正在快速迭代中

# 文档

详细文档TBA

## 快速上手

Start the interactive shell:
```bash
cargo run -- shell    # start in debug mode
cargo run -r -- shell # start in release mode
```

## 构建说明

### Windows
在 Windows 上构建项目应该可以直接工作。

### macOS
在 macOS 上构建 Python 绑定时，可能需要确保正确安装了 Python 开发环境：
```bash
brew install python3
```

如果遇到链接错误，可以尝试设置环境变量：
```bash
export PYO3_PYTHON=python3
```

### Linux
在 Linux 上构建项目应该可以直接工作，但确保安装了必要的开发工具。

### ARM64 支持
项目支持在 ARM64 架构上构建和运行。要进行交叉编译，请确保安装了相应的工具链：

对于 Linux ARM64：
```bash
# Ubuntu/Debian
sudo apt-get install gcc-aarch64-linux-gnu

# CentOS/RHEL
sudo yum install gcc-aarch64-linux-gnu
```

然后配置 Cargo 使用正确的链接器，在项目根目录创建 `.cargo/config.toml` 文件：
```toml
[target.aarch64-unknown-linux-gnu]
linker = "aarch64-linux-gnu-gcc"
```

构建命令：
```bash
rustup target add aarch64-unknown-linux-gnu
cargo build --target aarch64-unknown-linux-gnu
```

对于 macOS ARM64（本地构建）：
确保在 macOS ARM64 设备上进行构建（如 Apple Silicon Mac）：

1. 安装必要的开发工具：
```bash
# 安装 Homebrew（如果尚未安装）
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# 安装 Python 开发环境
brew install python3
```

2. 构建命令：
```bash
# 添加目标平台支持
rustup target add aarch64-apple-darwin

# 构建项目
cargo build --target aarch64-apple-darwin --features std,serde,miette
```

注意：由于需要特定的 macOS SDK 和工具链，从其他平台（如 Windows 或 Linux）进行 macOS 交叉编译可能需要额外的配置和工具。

对于 Windows ARM64，需要安装 Visual Studio 或 Build Tools 并确保包含 C++ 工具。

## 系统架构

TBA

# Contributing

TuGraph 社区热情欢迎每一位对图计算、数据库技术、Rust语言热爱的开发者，无论是doc修改和补充、bug fix还是new feature。

MiniGU 开放了一些[新功能的开发](https://github.com/tugraph-family/miniGU/issues?q=is%3Aopen+is%3Aissue+label%3A%22help+wanted%22)，欢迎有兴趣的同学一起共建。

如果你对MiniGU不熟悉也没关系，可以直接联系我们，将会有社区导师指导你上手！更多详情，请参考 [社区贡献](CONTRIBUTING.md)。

# Contributors

感谢对这个项目做过贡献的个人开发者，名单如下：

<a href="https://github.com/TuGraph-family/miniGU/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=TuGraph-family/miniGU" />
</a>

## 联系我们

官网: [tugraph.tech](https://tugraph.tech)

通过钉钉群、微信群、微信公众号、邮箱和电话联系我们:
![contacts](./docs/images/contact.jpeg)



