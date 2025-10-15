# 数据解析模块 data_analysis

## 项目简介

data_analysis是一个专为充电站数据分析设计的模块，旨在从Kafka获取各topic数据，解析、加工成各业务模块所需格式，并提供统一的数据分发接口。该模块支持多场站并发处理、窗口数据补全/插值、跨模块依赖处理，并易于扩展和云平台部署。

## 背景与目标用户

本模块主要面向充电站运营平台的数据处理需求，支持多种业务场景，包括负荷预测、运营优化、电价策略、热管理、客户挖掘等。目标用户包括：
- 充电站运营平台开发团队
- 数据分析工程师
- 机器学习模型开发人员
- 系统架构师

## 安装说明

### 环境要求
- Python 3.7+
- Kafka集群（用于数据输入/输出）

### 安装步骤

1. 克隆仓库：
   ```bash
   git clone [仓库地址]
   cd data_analysis
   ```

2. 安装依赖：
   ```bash
   pip install -r requirements.txt
   ```

3. 配置Kafka连接（在`data_analysis/config.py`中修改`KAFKA_CONFIG`）：
   ```python
   KAFKA_CONFIG = {
       'bootstrap_servers': ['your_kafka_server:9092'],
       'group_id': 'your_group_id',
       'auto_offset_reset': 'latest',
       'enable_auto_commit': True,
   }
   ```

4. 运行示例：
   ```bash
   # 同步示例
   python data_analysis/main.py
   
   # 异步示例
   python data_analysis/async_main.py
   ```

## 目录结构

- data_analysis/
  - __init__.py
  - config.py           # 配置文件，Kafka、topic、模块依赖等
  - kafka_client.py     # Kafka消费封装
  - kafka_producer.py   # Kafka生产封装
  - parser_base.py      # 解析器基类
  - topic_parsers/      # 各topic原始数据解析器
      - __init__.py
      - station_param.py
      - station_realtime_data.py
      - environment_calendar.py
      - device_meter.py
      - device_gun.py
      - car_order.py
      - car_price.py
      - device_error.py
      - device_host.py
      - device_storage.py
  - parsers/            # 各业务模块数据整合器
      - __init__.py
      - electricity_price_parser.py
      - load_prediction_parser.py
      - pv_prediction_parser.py
      - thermal_management_parser.py
      - station_guidance_parser.py
      - evaluation_model_parser.py
      - SOH_model_parser.py
      - operation_optimization_parser.py
      - customer_mining_parser.py
  - dispatcher.py       # 数据分发与依赖处理
  - main.py             # 启动入口

## 主要功能
- Kafka消费与topic订阅，topic与模块多对多映射
- topic原始数据解析与整合
- 跨模块数据依赖处理
- 对外统一数据分发接口，支持上传到Kafka
- 易于扩展和云平台部署

## 配置说明

### 核心配置文件：`data_analysis/config.py`

1. **Kafka配置**：
   ```python
   KAFKA_CONFIG = {
       'bootstrap_servers': ['localhost:9092'],  # Kafka服务器地址
       'group_id': 'data_analysis_group',         # 消费者组ID
       'auto_offset_reset': 'latest',             # 消费策略：latest/earliest
       'enable_auto_commit': True,               # 是否自动提交offset
   }
   ```

2. **Topic配置**：
   - `TOPIC_DETAIL`：每个topic的详细配置，包括字段、推送频率、依赖模块和数据窗口大小
   - `TOPIC_TO_MODULES`：topic到业务模块的多对多映射
   - `MODULE_TO_TOPICS`：业务模块到topic的多对多映射
   - `MODULE_DEPENDENCIES`：模块间的依赖关系

3. **数据窗口配置**：
   - 每个topic的`window_size`定义了数据缓存窗口大小
   - 窗口大小根据数据频率和业务需求设定，例如高频数据(15秒)可能需要更大的窗口

### 服务配置

1. **数据过期时间**：
   ```python
   # 在初始化服务时设置
   service = DataAnalysisService(data_expire_seconds=600)  # 数据10分钟后过期
   ```

2. **输出Topic配置**：
   ```python
   # 输出Topic前缀
   service = DataAnalysisService(output_topic_prefix="MODULE-OUTPUT-")
   ```

3. **窗口补全策略**：
   - 'zero'：补零（默认）
   - 'linear'：线性插值
   - 'forward'：前向填充
   - 'missing'：标记为None

## 故障排除指南

### 常见问题及解决方案

1. **Kafka连接失败**：
   - 检查`KAFKA_CONFIG`中的`bootstrap_servers`是否正确
   - 确认Kafka服务是否正常运行
   - 检查网络连接和防火墙设置

2. **数据解析错误**：
   - 检查topic数据格式是否符合预期
   - 确认字段名称是否与配置一致
   - 查看日志中的具体错误信息

3. **内存占用过高**：
   - 调整`data_expire_seconds`减少数据缓存时间
   - 优化窗口大小，避免不必要的历史数据存储
   - 增加系统可用内存或调整JVM参数（如使用Kafka Java客户端时）

4. **数据分发延迟**：
   - 检查Kafka生产者配置
   - 考虑增加消费者并行度
   - 优化回调函数处理逻辑

5. **模块依赖问题**：
   - 检查`MODULE_DEPENDENCIES`配置是否正确
   - 确认依赖模块是否正常输出数据
   - 查看依赖解析相关的日志

### 日志查看

日志文件默认保存在`data_analysis.log`中，包含以下关键信息：
- Kafka连接状态
- 数据处理过程
- 错误和异常信息
- 性能指标

可通过修改`logging.basicConfig`配置调整日志级别和输出方式。

## 性能优化建议

1. **多场站并发处理**：
   - 同步服务：调整`ThreadPoolExecutor`的`max_workers`参数
   - 异步服务：利用asyncio的并发能力，适当增加事件循环并发数

2. **内存优化**：
   - 合理设置数据窗口大小，避免不必要的历史数据存储
   - 定期清理过期数据，调用`clean_expired()`方法
   - 使用高效的数据结构，如`deque`实现滑动窗口

3. **Kafka优化**：
   - 根据数据量调整`batch_size`和`linger_ms`参数
   - 使用压缩减少网络传输开销
   - 合理分区，提高并行处理能力

4. **批处理优化**：
   - 对于高频数据，考虑适当降低处理频率
   - 实现批处理回调，减少单条数据处理开销

5. **资源监控**：
   - 监控内存使用情况，及时调整缓存策略
   - 跟踪Kafka消费/生产速率，平衡处理能力
   - 监控CPU使用率，优化计算密集型任务

## topic与模块映射

| topic                        | 字段（部分） | 推送频率 | 需求模块 |
|------------------------------|--------------|----------|----------|
| SCHEDULE-STATION-PARAM       | station_id, station_temp, ... | 新建/变更 | load_prediction, operation_optimization, electricity_price, SOH_model |
| SCHEDULE-STATION-REALTIME-DATA | station_id, gun_id, ... | 1分钟1次 | load_prediction, operation_optimization, electricity_price, SOH_model, thermal_management, evaluation_model |
| SCHEDULE-ENVIRONMENT-CALENDAR | workday_code, holiday_code | 1年1次 | load_prediction, operation_optimization, electricity_price, SOH_model |
| SCHEDULE-DEVICE-METER        | meter_id, current_power, ... | 5分钟1次 | operation_optimization, electricity_price |
| SCHEDULE-DEVICE-GUN          | host_id, gun_id, gun_status | 15秒1次 | operation_optimization |
| SCHEDULE-CAR-ORDER           | station_id, order_id, ... | 1秒1次 | load_prediction, operation_optimization, station_guidance, electricity_price, evaluation_model |
| SCHEDULE-CAR-PRICE           | station_id, period_no, ... | 1月1次 | operation_optimization, electricity_price, evaluation_model, thermal_management |
| SCHEDULE-DEVICE-ERROR        | station_id, host_error, ... | 触发推送 | operation_optimization, SOH_model |
| SCHEDULE-DEVICE-HOST         | host_id, acdc_status, ... | 1秒/15秒 | evaluation_model, thermal_management |
| SCHEDULE-DEVICE-STORAGE      | host_id, storage_id, ... | 15秒1次 | evaluation_model, thermal_management, electricity_price, operation_optimization |


## 数据流转说明

1. data_analysis模块从Kafka消费所有topic，按topic类型用topic_parsers解析原始数据。
2. dispatcher按场站缓存所有topic数据，自动整合为各业务模块所需结构（见parsers/）。
3. 各业务模块输入数据可上传到Kafka，供下游模型直接消费。
4. 支持多场站并发、内存自动清理、易于扩展。


## 嵌入式用法示例

### 1. 同步用法（适合传统多线程/阻塞模型）

```python
from data_analysis import DataAnalysisService

def my_module_callback(station_id, outputs):
  # outputs为该场站所有业务模块输入数据
  # 例如：outputs['load_prediction']、outputs['electricity_price']等
  do_something(outputs['load_prediction'])

service = DataAnalysisService()
service.start(callback=my_module_callback)
# ...业务主循环...
service.stop()
```

如需独立运行，直接执行main.py即可。

### 2. 异步用法（推荐，适合高并发/云原生场景）

```python
import asyncio
from data_analysis.async_service import AsyncDataAnalysisService

async def my_model_predict(module_input):
  # 这里写模型推理逻辑
  await asyncio.sleep(0.01)  # 模拟异步推理
  return {"result": 123}

async def my_callback(station_id, module_input):
  # module_input为本模型所需结构化输入（含窗口补全/插值、依赖聚合）
  result = await my_model_predict(module_input)
  return result  # 返回值会自动上传到Kafka

async def main():
  service = AsyncDataAnalysisService(module_name="load_prediction")
  await service.start(callback=my_callback)
  try:
    for _ in range(10):  # 模拟主循环10次
      print(service.get_station_status())
      await asyncio.sleep(1)
  finally:
    await service.stop()

if __name__ == "__main__":
  asyncio.run(main())
```

如需独立运行，直接执行 async_main.py。

---

## 同步/异步API兼容性与迁移说明

### 1. API兼容性
- 同步API（DataAnalysisService）适合传统阻塞/多线程场景，接口为阻塞式回调。
- 异步API（AsyncDataAnalysisService）基于asyncio，支持高并发、异步回调、异步Kafka、异步上传。
- 两者均支持多场站、窗口补全/插值、依赖聚合、自动上传Kafka、健康监控、配置热更新。

### 2. 迁移与混用建议
- 推荐新项目/云原生/高并发场景优先使用异步API。
- 迁移方式：
  - 回调函数由同步def改为async def，内部可await模型推理/IO。
  - 服务启动/停止由同步start/stop改为await service.start/stop。
  - Kafka、窗口补全、依赖聚合等用法保持一致。
- 同步/异步不可直接混用（如同一场站/同一Kafka group），如需混用请确保topic、group_id、端口等配置隔离。

### 3. 注意事项
- 异步API需在asyncio事件循环中运行。
- 异步回调支持await任意异步操作，适合模型推理、远程调用等高并发场景。
- 同步API适合遗留系统或对异步无需求场景。
- 日志、健康监控、配置热更新等功能同步/异步均支持。

如需更多用法示例、测试用例，请参考 data_analysis/async_main.py、tests/test_async_service.py。

## 贡献指南

我们欢迎任何形式的贡献，包括但不限于：
- 报告bug
- 提出功能建议
- 改进文档
- 提交代码

### 开发环境设置

1. 克隆仓库并创建开发分支：
   ```bash
   git clone [仓库地址]
   cd data_analysis
   git checkout -b feature/your-feature-name
   ```

2. 安装开发依赖：
   ```bash
   pip install -r requirements.txt
   pip install pytest black flake8  # 测试和代码格式化工具
   ```

3. 运行测试：
   ```bash
   pytest tests/
   ```

### 代码规范

- 遵循PEP 8 Python代码风格规范
- 使用black进行代码格式化
- 使用flake8检查代码质量
- 为所有公共API添加完整的文档字符串
- 保持函数和类的简洁性，避免过长的函数

### 提交PR流程

1. 确保所有测试通过
2. 更新相关文档
3. 提交清晰、描述性的提交信息
4. 创建Pull Request，详细描述变更内容和原因

### 提交信息规范

提交信息应遵循以下格式：
```
<类型>(<范围>): <描述>

[可选的详细描述]

[可选的关闭issue引用]
```

类型包括：
- feat: 新功能
- fix: 修复bug
- docs: 文档更新
- style: 代码格式调整
- refactor: 代码重构
- test: 测试相关
- chore: 其他不涉及src或test的修改

## 版本历史

### v1.0.0 (2025-10-15)
- 初始版本发布
- 实现基本的数据解析和分发功能
- 支持同步和异步两种API
- 实现多场站并发处理
- 支持数据窗口补全/插值
- 支持模块间依赖处理
- 支持配置热更新

### 计划中的功能
- v1.1.0: 添加更多数据窗口补全策略
- v1.2.0: 增强监控和日志功能
- v1.3.0: 支持分布式部署
- v2.0.0: 实现插件化架构
