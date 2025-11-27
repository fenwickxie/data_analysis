#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
data_analysis 包安装配置
"""

from setuptools import setup, find_packages
from pathlib import Path

# 读取README作为长描述
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text(encoding="utf-8") if readme_file.exists() else ""

# 读取依赖
requirements_file = Path(__file__).parent / "requirements.txt"
requirements = []
if requirements_file.exists():
    with open(requirements_file, "r", encoding="utf-8") as f:
        requirements = [line.strip() for line in f if line.strip() and not line.startswith("#")]

setup(
    name="data_analysis",
    version="2.0.0",
    author="xie.fangyu",
    author_email="fenwickxie@outlook.com",  # 请替换为实际邮箱
    description="充电站数据分析解析模块 - 从Kafka获取数据，解析并分发给各业务模块",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/fenwickxie/data_analysis",  # 请替换为实际仓库地址
    packages=find_packages(exclude=["tests", "tests.*", "docs", "scripts"]),
    include_package_data=True,
    install_requires=requirements,
    python_requires=">=3.10",
    license="MIT",  # 使用 license 参数代替 classifier
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
    ],
    keywords="kafka data-analysis charging-station data-processing",
    project_urls={
        "Documentation": "https://github.com/fenwickxie/data_analysis/blob/main/README.md",
        "Source": "https://github.com/fenwickxie/data_analysis",
        "Bug Reports": "https://github.com/fenwickxie/data_analysis/issues",
    },
    # 入口点（命令行工具，可选）
    # entry_points={
    #     'console_scripts': [
    #         'data-analysis=d_a.main:main',
    #     ],
    # },
)