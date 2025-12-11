"""
Panda Web 包安装配置

本文件定义了 Panda Web 包的安装配置，包括包名、版本、依赖等信息。
使用 setuptools 进行包管理。

核心概念
--------

- **包配置**：定义包的元数据（名称、版本、描述等）
- **依赖管理**：定义包的依赖关系
- **入口点**：定义命令行入口点

为什么需要这个文件？
-------------------

在 Python 包管理中，需要定义包的安装配置：
- 需要定义包的元数据
- 需要管理包的依赖
- 需要定义包的入口点

这个文件提供了这些配置。

注意事项
--------

- 使用 setuptools 进行包管理
- 定义了命令行入口点
- 需要 Python 3.8 或更高版本
"""

from setuptools import setup, find_packages

setup(
    name="panda_web",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "fastapi>=0.68.0",
        "uvicorn>=0.15.0",
        "python-multipart>=0.0.5",
        "aiofiles>=0.7.0",
        "panda_common"
    ],
    entry_points={
        "console_scripts": [
            "panda_web=panda_web.main:main",
        ],
    },
    author="PandaAI Team",
    author_email="team@pandaai.com",
    description="Web interface service for PandaAI platform",
    keywords="web,vue,fastapi",
    python_requires=">=3.8",
) 