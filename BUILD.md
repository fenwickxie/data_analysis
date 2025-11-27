# 打包说明

## 快速打包

### 方法一：使用构建脚本（推荐）

```powershell
# 一键构建
python build_package.py
```

这个脚本会：
1. 清理旧的构建产物
2. 检查必要的依赖
3. 构建 wheel 包
4. 显示生成的文件

### 方法二：使用 build 命令

```powershell
# 1. 安装构建工具（如果还没安装）
pip install build wheel setuptools

# 2. 清理旧文件
Remove-Item -Recurse -Force dist, build, *.egg-info -ErrorAction SilentlyContinue

# 3. 构建
python -m build
```

### 方法三：使用传统 setup.py（不推荐）

```powershell
python setup.py sdist bdist_wheel
```

## 生成的文件

构建完成后，在 `dist/` 目录下会生成：

```
dist/
├── data_analysis-2.0.0-py3-none-any.whl  ← 用于安装的 wheel 文件
└── data_analysis-2.0.0.tar.gz            ← 源码包
```

## 验证打包

### 检查 wheel 内容

```powershell
# 查看 wheel 包含的文件
python -m zipfile -l dist/data_analysis-2.0.0-py3-none-any.whl
```

### 测试安装

```powershell
# 在虚拟环境中测试安装
python -m venv test_env
test_env\Scripts\activate
pip install dist/data_analysis-2.0.0-py3-none-any.whl
python -c "from d_a.config import KAFKA_CONFIG; print('✓ 安装成功')"
deactivate
```

## 常见问题

### ❌ 错误：no commands supplied

**问题**：直接运行 `python setup.py` 报错

**解决**：不要直接运行 `setup.py`，使用以下方法之一：
```powershell
# 推荐
python -m build

# 或
python build_package.py
```

### ❌ 错误：ModuleNotFoundError: No module named 'build'

**问题**：缺少构建工具

**解决**：
```powershell
pip install build wheel setuptools
```

### ❌ 警告：SetuptoolsDeprecationWarning: License classifiers are deprecated

**解决**：已修复，使用 `license` 参数代替 classifier

### ⚠️ 打包不包含某些文件

**问题**：打包后缺少文件（如 config.yaml.example）

**解决**：检查并更新 `MANIFEST.in` 文件
```
include config.yaml.example
include main.py.example
```

## 发布流程（可选）

### 发布到 PyPI

```powershell
# 1. 安装 twine
pip install twine

# 2. 检查包
twine check dist/*

# 3. 上传到 TestPyPI（测试）
twine upload --repository testpypi dist/*

# 4. 从 TestPyPI 安装测试
pip install --index-url https://test.pypi.org/simple/ data_analysis

# 5. 确认无误后上传到正式 PyPI
twine upload dist/*
```

### 版本管理

更新版本号时需要修改：
- `setup.py` 中的 `version="2.0.0"`
- `pyproject.toml` 中的 `version = "2.0.0"`

建议使用语义化版本：
- `2.0.0` → `2.0.1` (修复bug)
- `2.0.0` → `2.1.0` (新功能)
- `2.0.0` → `3.0.0` (破坏性变更)

## 文件说明

### 打包相关文件

- **`setup.py`** - 包配置（向后兼容）
- **`pyproject.toml`** - 现代打包配置（推荐）
- **`MANIFEST.in`** - 指定额外包含的文件
- **`requirements.txt`** - 依赖列表
- **`build_package.py`** - 构建辅助脚本

### 分发相关文件

- **`config.yaml.example`** - 配置文件模板
- **`main.py.example`** - 入口文件模板
- **`QUICKSTART.md`** - 快速开始指南
- **`README.md`** - 项目说明

## 最佳实践

1. **清理旧文件**：每次构建前清理 `dist/`, `build/`, `*.egg-info`
2. **测试安装**：在虚拟环境中测试安装和导入
3. **版本管理**：遵循语义化版本规范
4. **文档同步**：更新版本时同步更新文档
5. **Git 标签**：发布时打 git tag：`git tag v2.0.0`

## 参考资料

- [Python Packaging User Guide](https://packaging.python.org/)
- [setuptools 文档](https://setuptools.pypa.io/)
- [PEP 517 - Build Backend](https://peps.python.org/pep-0517/)
- [PEP 518 - pyproject.toml](https://peps.python.org/pep-0518/)
