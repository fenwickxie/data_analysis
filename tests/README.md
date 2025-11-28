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

## 如何运行测试

测试使用 `pytest` 框架运行。您可以从项目根目录运行所有测试：

```bash
pytest tests/
```

或者，您可以使用提供的测试运行器来执行更具体的测试场景：

### 运行所有测试
```bash
python tests/run_tests.py --all
```

### 运行快速冒烟测试
这将运行一小部分测试，以快速验证基本功能。
```bash
python tests/run_tests.py --quick
```

### 测试特定模块
您可以测试特定模块的数据消耗（例如 `load_prediction`）：
```bash
python tests/run_tests.py --module load_prediction
```

### 检查 Kafka 连接性
此测试验证是否可以建立到 Kafka 代理的连接。
```bash
python tests/run_tests.py --connect
```
