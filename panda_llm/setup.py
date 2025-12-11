"""
Panda LLM 包安装配置

本文件定义了 Panda LLM 包的安装配置，包括包名、版本、依赖等信息。
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
- 包含测试依赖（可选）
- 定义了命令行入口点
"""

from setuptools import setup, find_packages
import os

# 确保当前目录中有一个panda_llm目录
if not os.path.exists('panda_llm'):
    os.makedirs('panda_llm')
    with open('panda_llm/__init__.py', 'a'):
        pass

setup(
    name='panda_llm',
    version='0.1.0',
    description='Panda AI LLM integration service',
    author='Panda AI',
    packages=find_packages(),
    include_package_data=True,
    package_data={
        'panda_llm': [
            'services/**/*',
            'models/**/*',
            'routes/**/*',
            '*.yaml',
        ],
    },
    install_requires=[
        'fastapi>=0.104.1',
        'uvicorn>=0.24.0',
        'pydantic>=2.4.2',
        'aiohttp>=3.9.1',
        'pyyaml>=6.0.1',
        'python-dotenv>=1.0.0',
        'openai>=1.0.0',
        'panda_common',
    ],
    extras_require={
        'test': [
            'pytest',
            'pytest-asyncio',
            'httpx',
        ],
    },
    entry_points={
        'console_scripts': [
            'panda_llm = panda_llm.__main__:main',
        ],
    },
) 