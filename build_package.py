#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
构建和打包 data_analysis 包的辅助脚本
"""

import os
import sys
import shutil
import subprocess
from pathlib import Path

def clean_build_artifacts():
    """清理旧的构建产物"""
    print("🧹 清理旧的构建产物...")
    
    dirs_to_remove = ['build', 'dist', '*.egg-info', '__pycache__', '.pytest_cache']
    
    for pattern in dirs_to_remove:
        if '*' in pattern:
            # 使用通配符匹配
            import glob
            for path in glob.glob(pattern):
                if os.path.isdir(path):
                    shutil.rmtree(path)
                    print(f"  ✓ 删除 {path}")
        else:
            path = Path(pattern)
            if path.exists():
                shutil.rmtree(path)
                print(f"  ✓ 删除 {path}")
    
    # 递归删除所有 __pycache__
    for root, dirs, files in os.walk('.'):
        if '__pycache__' in dirs:
            pycache_path = os.path.join(root, '__pycache__')
            shutil.rmtree(pycache_path)
            print(f"  ✓ 删除 {pycache_path}")

def check_dependencies():
    """检查必要的构建依赖"""
    print("\n📦 检查构建依赖...")
    
    required_packages = ['build', 'wheel', 'setuptools']
    missing = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"  ✓ {package} 已安装")
        except ImportError:
            missing.append(package)
            print(f"  ✗ {package} 未安装")
    
    if missing:
        print(f"\n❌ 缺少以下依赖: {', '.join(missing)}")
        print(f"请运行: pip install {' '.join(missing)}")
        return False
    
    return True

def build_package():
    """构建包"""
    print("\n🔨 开始构建包...")
    
    try:
        # 使用 python -m build
        result = subprocess.run(
            [sys.executable, '-m', 'build'],
            check=True,
            capture_output=True,
            text=True
        )
        print(result.stdout)
        print("  ✓ 构建成功!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"  ✗ 构建失败!")
        print(e.stderr)
        return False

def list_artifacts():
    """列出生成的文件"""
    print("\n📋 生成的文件:")
    
    dist_dir = Path('dist')
    if dist_dir.exists():
        for file in sorted(dist_dir.iterdir()):
            size = file.stat().st_size / 1024  # KB
            print(f"  ✓ {file.name} ({size:.1f} KB)")
    else:
        print("  ✗ dist 目录不存在")

def main():
    """主函数"""
    print("=" * 60)
    print("  data_analysis 包构建工具")
    print("=" * 60)
    
    # 确保在项目根目录
    if not Path('setup.py').exists() and not Path('pyproject.toml').exists():
        print("❌ 错误: 请在项目根目录下运行此脚本")
        sys.exit(1)
    
    # 清理
    clean_build_artifacts()
    
    # 检查依赖
    if not check_dependencies():
        sys.exit(1)
    
    # 构建
    if not build_package():
        sys.exit(1)
    
    # 列出产物
    list_artifacts()
    
    print("\n" + "=" * 60)
    print("✅ 构建完成!")
    print("=" * 60)
    print("\n📦 使用方法:")
    print("  pip install dist/data_analysis-xxx-py3-none-any.whl")
    print("\n💡 提示:")
    print("  - 在模型项目中安装此 wheel 文件")
    print("  - 复制 config.yaml.example 和 main.py.example 到模型项目")
    print("  - 参考 QUICKSTART.md 快速开始")
    print()

if __name__ == '__main__':
    main()
