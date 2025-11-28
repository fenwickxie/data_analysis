# 数据分析测试套件

此目录包含 `d_a` 项目的所有自动化测试。测试模块的组织结构旨在反映主应用程序的结构。

## 测试架构

重构后的测试套件组织如下：

- `test_config_and_topics.py`: 验证应用程序的配置，包括 Topic 定义、模块依赖和解析器映射。
- `test_dispatcher.py`: 包含 `DataDispatcher` 的单元测试，验证数据路由、窗口化和依赖聚合逻辑。
- `test_parsers.py`: 包含所有独立 Topic 和模块输出解析器的单元测试，确保它们能正确处理和转换原始数据。
- `test_service.py`: 为 `DataAnalysisService` 和 `AsyncDataAnalysisService` 提供集成测试，覆盖主服务生命周期、健康检查和批次聚合。
- `test_kafka_integration.py`: 专注于 Kafka 相关的集成测试，通过模拟 Kafka 客户端来验证服务是否能正确消费、处理和生产消息。
- `test_data_generation.py`: 测试用于测试的模拟数据生成工具，确保可以创建真实且多样的测试数据。
- `run_tests.py`: 主测试运行脚本，提供一个命令行界面来执行不同的测试套件（例如，连接测试、快速检查、完整测试运行）。
- `fixtures/`: 包含测试夹具和工具，例如模拟数据生成器 (`test_utils.py`) 及其基类 (`data_generator_base.py`)。

## 环境准备

### 安装测试依赖

```bash
# 安装开发和测试依赖
pip install -r requirements-dev.txt
```

或单独安装测试工具：

```bash
pip install pytest>=9.0.1
pip install pytest-asyncio>=1.3.0
pip install pytest-html>=4.1.1
pip install pytest-cov>=6.0.0
```

## 如何运行测试

测试使用 `pytest` 框架运行。您可以从项目根目录运行所有测试：

```bash
pytest tests/
```

或者，您可以使用提供的测试运行器来执行更具体的测试场景：

### 运行所有测试
```bash
python tests/run_tests.py test --all
```

### 运行特定模块测试
您可以测试特定模块（例如 `service`、`dispatcher`）：
```bash
python tests/run_tests.py test --module service
python tests/run_tests.py test --module dispatcher
```

### 检查 Kafka 连接性
此测试验证是否可以建立到 Kafka 代理的连接。
```bash
python tests/run_tests.py connectivity
```

## 生成测试报告

### 生成 HTML 和 XML 报告

```bash
# 运行测试并生成 HTML 和 JUnit XML 报告
python tests/run_tests.py test --all --report
```

生成的报告位于 `test_reports/` 目录：
- `test_report.html` - 美观的 HTML 格式测试报告
- `test_report.xml` - JUnit XML 格式（用于 CI/CD 集成）

### 生成代码覆盖率报告

```bash
# 运行测试并生成覆盖率报告
python tests/run_tests.py test --all --coverage
```

覆盖率报告位于 `test_reports/htmlcov/index.html`

### 生成所有报告（推荐）

```bash
# 同时生成测试报告和覆盖率报告
python tests/run_tests.py test --all --report --coverage
```

### 使用 pytest 直接生成报告

```bash
# HTML 报告
pytest tests/ --html=test_report.html --self-contained-html -v

# JUnit XML 报告
pytest tests/ --junitxml=test_report.xml -v

# 覆盖率报告
pytest tests/ --cov=d_a --cov-report=html --cov-report=term -v

# 组合使用
pytest tests/ \
  --html=test_report.html \
  --self-contained-html \
  --junitxml=test_report.xml \
  --cov=d_a \
  --cov-report=html \
  -v
```

## 测试报告说明

### HTML 测试报告

HTML 报告包含以下内容：
- 测试执行摘要（通过/失败数量、耗时）
- 详细的测试结果列表
- 失败测试的堆栈跟踪信息
- 测试环境信息

### 覆盖率报告

覆盖率报告显示：
- 整体代码覆盖率百分比
- 每个模块的覆盖率详情
- 未覆盖的代码行高亮显示
- 分支覆盖率信息

目标覆盖率：
- 核心模块（dispatcher, service）：≥ 90%
- 解析器模块（parsers）：≥ 85%
- Kafka 客户端：≥ 80%

## 持续集成

在 CI/CD 环境中，可以使用以下配置：

```yaml
# 示例 GitHub Actions 配置
- name: Run tests with coverage
  run: |
    python tests/run_tests.py test --all --report --coverage
    
- name: Upload test reports
  uses: actions/upload-artifact@v3
  with:
    name: test-reports
    path: test_reports/
```

## 测试最佳实践

1. **运行测试前**：确保 Kafka 配置正确（如使用 mock，无需真实 Kafka）
2. **本地开发**：使用 `--report` 和 `--coverage` 定期检查测试质量
3. **CI/CD**：始终生成报告并归档，便于问题追踪
4. **覆盖率**：新功能必须包含相应测试，保持覆盖率不下降

## 故障排查

### 异步测试失败

如果遇到 "RuntimeError: Runner.run() cannot be called from a running event loop" 错误：
- 确保使用 `python tests/run_tests.py` 而不是在已有事件循环中运行
- 检查 `pyproject.toml` 中 `asyncio_mode = "auto"`

### 测试超时

- 增加 pytest 超时设置：`pytest tests/ --timeout=300`
- 或在 `pyproject.toml` 中配置

### 报告生成失败

- 确保已安装 `pytest-html` 和 `pytest-cov`
- 检查文件写入权限（`test_reports/` 目录）
