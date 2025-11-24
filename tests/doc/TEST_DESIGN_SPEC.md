# Data Analysis 项目测试软件说明书

**版本**: 3.0  
**日期**: 2025年11月24日  
**项目**: 充电桩数据分析平台测试套件  

---

## 文档说明

本文档是测试套件的**技术文档**。

## 目录

1. [概述](#1-概述)
2. [重构与精简历程](#2-重构与精简历程)
3. [设计模式应用](#3-设计模式应用)
4. [测试架构](#4-测试架构)
5. [核心组件详解](#5-核心组件详解)
6. [API接口详解](#6-api接口详解)
7. [使用指南](#7-使用指南)
8. [扩展指南](#8-扩展指南)
9. [最佳实践](#9-最佳实践)
10. [附录](#10-附录)

---

## 1. 概述

### 1.1 项目背景

本测试套件为充电桩数据分析平台提供完整的测试解决方案，涵盖单元测试、集成测试、端到端测试等多个层面。经过**两轮重构优化**，测试代码质量和组织性得到显著提升。

### 1.2 测试目标

- **提高测试覆盖率**: 确保所有核心功能模块得到充分测试
- **提升测试效率**: 通过自动化测试减少人工干预，提高测试速度
- **增强测试可靠性**: 通过单元测试和集成测试确保代码质量
- **优化测试流程**: 通过模块化设计和代码复用减少重复工作

### 1.3 测试范围

- **Kafka消息队列**: 数据生产、消费、topic管理
- **数据分发器(Dispatcher)**: 窗口管理、依赖处理、数据补全
- **分析服务**: 同步/异步服务、回调机制、场站管理
- **数据解析**: Topic解析、模块解析、格式转换
- **集成流程**: 端到端数据流、异常处理、性能监控

### 1.4 技术栈

- **测试框架**: pytest, asyncio
- **消息队列**: Kafka (kafka-python, aiokafka)
- **数据格式**: JSON, 时序窗口数据
- **并发模型**: 线程池, 协程
- **设计模式**: 工厂模式、策略模式、建造者模式、模板方法模式

### 1.5 最终文件清单

#### 核心测试 (10个)

| 文件名 | 行数 | 测试数 | 说明 |
|--------|------|--------|------|
| `test_dispatcher.py` | 230 | 12 | **合并3个文件** (窗口/补零/依赖) |
| `test_service.py` | 340 | 12 | **合并3个文件** (异步/同步/上传) |
| `test_fixtures.py` | 200 | 18 | 基础设施单元测试 |
| `test_mock_producer.py` | 280 | - | 模拟生产者 |
| `test_kafka_consume.py` | 220 | - | 消费测试 |
| `test_mock_data_generator.py` | - | 10 | 数据生成器测试 |
| `test_extreme_and_integration.py` | - | 15 | 极端情况测试 |
| `test_config_based_parser.py` | - | 8 | 配置解析器测试 |
| `test_extract_station_data.py` | - | 6 | 场站数据提取 |
| `test_offset_commit.py` | - | 5 | Offset提交测试 |

#### 功能测试 (3个)
- `test_time_series_concatenation.py` - 时序数据拼接测试
- `test_topic_subscription.py` - Topic订阅测试  
- `run_tests.py` - 测试运行器 (140行)

#### 基础设施 (3个)
- `fixtures/__init__.py` (30行)
- `fixtures/data_generator_base.py` (450行, 4种设计模式)
- `fixtures/test_utils.py` (300行, 模板方法模式)

#### 工具和示例 (3个)
- `examples/test_batch_upload.py` - 批次上传示例
- `tools/diagnose_fetch.py` - 拉取诊断工具
- `tools/verify_group_id_fix.py` - Group ID验证工具

**总计**: 19个文件, ~2050行测试代码, ~86个测试用例


## 3. 测试架构

### 3.1 整体架构图

```
┌─────────────────────────────────────────────────────────────┐
│                    测试套件整体架构                            │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  单元测试层   │  │  集成测试层   │  │  工具测试层   │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
│         │                 │                 │               │
│  ┌──────▼──────────────────▼──────────────▼─────────┐      │
│  │            测试数据生成器 & 模拟器                  │      │
│  │  - MockDataGenerator (窗口数据生成)               │      │
│  │  - MockProducer (Kafka生产模拟)                   │      │
│  │  - TopicConsumeTester (消费测试)                  │      │
│  └────────────────────────────────────────────────────┘      │
│                                                               │
│  ┌────────────────────────────────────────────────────┐      │
│  │              被测系统 (SUT)                          │      │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐         │      │
│  │  │Dispatcher│  │  Service  │  │  Parser   │         │      │
│  │  └──────────┘  └──────────┘  └──────────┘         │      │
│  └────────────────────────────────────────────────────┘      │
│                                                               │
│  ┌────────────────────────────────────────────────────┐      │
│  │              外部依赖                                │      │
│  │  ┌──────────┐  ┌──────────┐                        │      │
│  │  │  Kafka   │  │  Config   │                        │      │
│  │  └──────────┘  └──────────┘                        │      │
│  └────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

### 3.2 测试分层

#### 3.2.1 单元测试层

测试单个组件的功能正确性：

- `test_dispatcher.py`: Dispatcher窗口管理和数据缓存
- `test_dispatcher_padding.py`: 数据补全策略（零填充、线性插值等）
- `test_dependency.py`: 模块间依赖关系处理
- `test_mock_data_generator.py`: 数据生成器功能验证

#### 3.2.2 集成测试层

测试多个组件的协同工作：

- `test_async_service.py`: 异步服务完整流程
- `test_kafka_upload.py`: Kafka上传集成
- `test_integration_extra.py`: 服务健康监控、配置热更新
- `test_extreme_and_integration.py`: 边界条件和异常处理

#### 3.2.3 工具测试层

提供测试辅助和环境验证：

- `test_kafka_consume.py`: Kafka消费测试工具
- `test_mock_producer.py`: 模拟数据生产工具
- `run_tests.py`: 统一测试入口

### 3.3 数据流

```
生产者 → Kafka Topic → 消费者 → Dispatcher → Parser → 业务回调 → 结果处理
   ↑                                                              ↓
   └──────────────── 模拟生产者（测试） ─────────────────────────────┘
                           ↑
                      数据生成器
```

---

## 4. 测试工具详解

### 4.1 统一测试入口 (run_tests.py)

**功能**: 提供便捷的命令行接口运行各类测试

**命令格式**:
```bash
python tests/run_tests.py <test_type> [options]
```

**支持的测试类型**:

| 命令 | 说明 | 适用场景 |
|------|------|----------|
| `connectivity` | Kafka连接测试 | 环境验证 |
| `quick` | 快速消费测试（前5个topic） | 快速验证 |
| `consume` | 完整消费测试 | 全面验证 |
| `module` | 模块特定topic测试 | 开发调试 |
| `produce` | 模拟生产者 | 数据生成 |

**参数说明**:

```bash
--module MODULE         # 指定测试模块名（默认: load_prediction）
--duration DURATION     # 运行时长（秒，默认: 60）
--timeout TIMEOUT       # Topic超时时间（秒，默认: 20）
```

**使用示例**:

```bash
# 快速验证Kafka连接
python tests/run_tests.py connectivity

# 测试load_prediction模块
python tests/run_tests.py module --module load_prediction --timeout 30

# 运行生产者5分钟
python tests/run_tests.py produce --duration 300
```

### 4.2 Kafka消费测试工具 (test_kafka_consume.py)

**核心类**: `TopicConsumeTester`

**主要功能**:

1. **连接性测试**
   ```python
   await test_kafka_connectivity()
   ```
   - 验证Kafka服务可达
   - 检查配置正确性
   - 返回连接状态

2. **Topic消费测试**
   ```python
   tester = TopicConsumeTester()
   await tester.test_single_topic_async("TOPIC_NAME", timeout_seconds=30)
   ```
   - 支持异步/同步两种模式
   - 自动采集样本数据
   - 统计消息计数

3. **批量测试**
   ```python
   topics = ["TOPIC1", "TOPIC2", "TOPIC3"]
   results = await tester.test_all_topics_async(topics, timeout_per_topic=20)
   ```
   - 串行测试所有topic
   - 生成详细报告
   - 错误日志记录

4. **服务集成测试**
   ```python
   result = await tester.test_service_integration(
       module_name="load_prediction",
       duration_seconds=30
   )
   ```
   - 验证完整数据流
   - 监控回调执行
   - 场站统计分析

**输出报告**:

```
================================================================================
TOPIC CONSUMPTION TEST SUMMARY
================================================================================

Total topics tested: 10
Topics with data: 8
Topics without data: 2
Success rate: 80.0%
Total messages received: 156

✓ Topics successfully consumed:
  - SCHEDULE-STATION-PARAM: 5 messages
  - SCHEDULE-STATION-REALTIME-DATA: 12 messages
  ...

⚠ Topics with no data:
  - SCHEDULE-CAR-PRICE
  - SCHEDULE-ENVIRONMENT-CALENDAR

📋 Sample messages:
  Topic: SCHEDULE-STATION-PARAM
    Keys: ['station_id', 'station_temp', 'lat', ...]
    Sample: {'station_id': 'station_001', ...}
```

### 4.3 模拟数据生产工具 (test_mock_producer.py)

**核心类**:
- `MockDataGenerator`: 数据生成器
- `MockProducer`: Kafka生产者

**支持的Topic及数据格式**:

| Topic | 数据格式 | 窗口大小 | 更新频率 |
|-------|----------|----------|----------|
| SCHEDULE-STATION-PARAM | 单值 | 1 | 配置变更时 |
| SCHEDULE-STATION-REALTIME-DATA | 窗口数组 | 可配置 | 1小时1次 |
| SCHEDULE-DEVICE-METER | 窗口数组 | 可配置 | 5分钟1次 |
| SCHEDULE-DEVICE-GUN | 窗口数组 | 可配置 | 15秒1次 |
| SCHEDULE-CAR-ORDER | 窗口数组 | 可配置 | 1秒1次 |
| SCHEDULE-CAR-PRICE | 多时段 | 1 | 1月1次 |
| SCHEDULE-DEVICE-ERROR | 窗口数组 | 10 | 触发时 |
| SCHEDULE-DEVICE-HOST | 窗口数组 | 可配置 | 动态 |
| SCHEDULE-DEVICE-STORAGE | 窗口数组 | 可配置 | 15秒1次 |
| SCHEDULE-ENVIRONMENT-CALENDAR | 单值 | 1 | 1年1次 |

**窗口数据示例**:

```python
{
    "station_id": "station_001",
    "history_curve_station_avg": [100.5, 102.3, 98.7, ...],  # 时序数组
    "history_curve_station_max": [200.1, 205.4, 195.8, ...],  # 时序数组
    "timestamps": [1699000000, 1699000060, 1699000120, ...]   # 时间戳数组
}
```

**使用方法**:

```python
import asyncio
from tests.test_mock_producer import MockProducer

async def produce_test_data():
    producer = MockProducer()
    await producer.start()
    
    # 运行5分钟，每10秒发送一批数据
    await producer.run_continuous(
        duration_seconds=300,
        interval_seconds=10
    )
    
    await producer.stop()

asyncio.run(produce_test_data())
```

**自定义数据生成**:

```python
from tests.test_mock_producer import MockDataGenerator

generator = MockDataGenerator()

# 生成场站实时数据（100个时间点）
data = generator.generate_station_realtime_data(
    station_id="station_001",
    window_size=100
)

# 生成订单数据（50个时间点）
order_data = generator.generate_car_order(
    station_id="station_001",
    window_size=50
)
```

---

## 5. API接口详解

本章节提供测试工具和函数的详细API文档，包括参数说明、返回值、使用示例和注意事项。

### API快速索引

| 类/函数 | 所属模块 | 功能描述 |
|---------|----------|----------|
| **TopicConsumeTester** | test_kafka_consume.py | Kafka topic消费测试工具 |
| └─ test_single_topic_async() | | 异步测试单个topic |
| └─ test_single_topic_sync() | | 同步测试单个topic |
| └─ test_all_topics_async() | | 异步批量测试topic |
| └─ test_all_topics_sync() | | 同步批量测试topic |
| └─ test_service_integration() | | 完整服务集成测试 |
| └─ print_summary() | | 打印测试结果报告 |
| **MockDataGenerator** | test_mock_producer.py | 测试数据生成器 |
| └─ generate_station_param() | | 生成场站参数数据 |
| └─ generate_station_realtime_data() | | 生成场站实时窗口数据 |
| └─ generate_environment_calendar() | | 生成环境日历数据 |
| └─ generate_device_meter() | | 生成电表窗口数据 |
| └─ generate_device_gun() | | 生成充电枪窗口数据 |
| └─ generate_car_order() | | 生成订单窗口数据 |
| └─ generate_car_price() | | 生成电价时段数据 |
| └─ generate_device_error() | | 生成设备错误窗口数据 |
| └─ generate_device_host() | | 生成主机窗口数据 |
| └─ generate_device_storage() | | 生成储能窗口数据 |
| **MockProducer** | test_mock_producer.py | Kafka模拟生产者 |
| └─ start() | | 启动生产者连接 |
| └─ stop() | | 停止生产者 |
| └─ produce_topic_data() | | 发送单条topic数据 |
| └─ run_continuous() | | 持续运行并发送数据 |
| **测试入口函数** | run_tests.py | 命令行测试工具 |
| └─ main() | | 主函数，解析参数并执行测试 |
| **独立测试函数** | test_kafka_consume.py | 全局测试函数 |
| └─ test_kafka_connectivity() | | 测试Kafka连接性 |
| └─ quick_test() | | 快速测试前N个topic |
| └─ test_module_topics() | | 测试指定模块的topic |
| └─ main() | | 完整消费测试主函数 |

---

### 5.1 TopicConsumeTester 类 (test_kafka_consume.py)

Kafka topic消费测试工具类，用于验证topic的可用性和数据格式。

#### 5.1.1 类初始化

```python
TopicConsumeTester(kafka_config: Optional[Dict] = None)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| kafka_config | Optional[Dict] | None | Kafka配置字典，为None时使用KAFKA_CONFIG |

**实例属性**:

| 属性名 | 类型 | 说明 |
|--------|------|------|
| consumed_topics | Set[str] | 已消费的topic集合 |
| topic_message_counts | Dict[str, int] | 每个topic的消息计数 |
| topic_sample_data | Dict[str, List] | 每个topic的样本数据列表 |
| errors | List[str] | 错误日志列表 |

**示例**:
```python
from tests.test_kafka_consume import TopicConsumeTester

# 使用默认配置
tester = TopicConsumeTester()

# 使用自定义配置
custom_config = {
    'bootstrap_servers': 'localhost:9092',
    'group_id': 'test_group'
}
tester = TopicConsumeTester(kafka_config=custom_config)
```

#### 5.1.2 test_single_topic_async

异步测试单个topic的消费功能。

```python
async def test_single_topic_async(
    self,
    topic: str,
    timeout_seconds: int = 30
) -> bool
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| topic | str | - | 要测试的topic名称 |
| timeout_seconds | int | 30 | 超时时间（秒） |

**返回值**: `bool` - 是否成功消费到数据

**功能说明**:
- 创建AsyncKafkaConsumerClient实例
- 订阅指定topic
- 在超时时间内尝试消费消息
- 记录消息计数和样本数据（最多5条）
- 自动关闭消费者连接

**异常处理**:
- 连接失败时记录到errors列表
- 超时后返回False

**示例**:
```python
tester = TopicConsumeTester()
success = await tester.test_single_topic_async(
    "SCHEDULE-STATION-PARAM",
    timeout_seconds=20
)
if success:
    print(f"消息数: {tester.topic_message_counts['SCHEDULE-STATION-PARAM']}")
    print(f"样本: {tester.topic_sample_data['SCHEDULE-STATION-PARAM'][0]}")
```

#### 5.1.3 test_single_topic_sync

同步测试单个topic的消费功能（线程安全包装）。

```python
def test_single_topic_sync(
    self,
    topic: str,
    timeout_seconds: int = 30
) -> bool
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| topic | str | - | 要测试的topic名称 |
| timeout_seconds | int | 30 | 超时时间（秒） |

**返回值**: `bool` - 是否成功消费到数据

**功能说明**:
- 内部创建新的事件循环
- 调用test_single_topic_async执行异步逻辑
- 返回异步函数的结果

**适用场景**: 在同步代码中调用topic测试

**示例**:
```python
tester = TopicConsumeTester()
# 在普通函数中使用
success = tester.test_single_topic_sync("SCHEDULE-DEVICE-METER")
```

#### 5.1.4 test_all_topics_async

异步批量测试多个topic。

```python
async def test_all_topics_async(
    self,
    topics: List[str],
    timeout_per_topic: int = 30
) -> Dict[str, bool]
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| topics | List[str] | - | topic名称列表 |
| timeout_per_topic | int | 30 | 每个topic的超时时间（秒） |

**返回值**: `Dict[str, bool]` - {topic名称: 是否成功} 字典

**功能说明**:
- 串行测试所有topic（避免资源竞争）
- 记录每个topic的成功/失败状态
- 累积所有消息计数和样本数据

**示例**:
```python
topics = [
    "SCHEDULE-STATION-PARAM",
    "SCHEDULE-DEVICE-METER",
    "SCHEDULE-CAR-ORDER"
]
results = await tester.test_all_topics_async(topics, timeout_per_topic=20)

for topic, success in results.items():
    status = "✓" if success else "✗"
    print(f"{status} {topic}")
```

#### 5.1.5 test_all_topics_sync

同步批量测试多个topic。

```python
def test_all_topics_sync(
    self,
    topics: List[str],
    timeout_per_topic: int = 30
) -> Dict[str, bool]
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| topics | List[str] | - | topic名称列表 |
| timeout_per_topic | int | 30 | 每个topic的超时时间（秒） |

**返回值**: `Dict[str, bool]` - {topic名称: 是否成功} 字典

**示例**:
```python
tester = TopicConsumeTester()
results = tester.test_all_topics_sync([
    "SCHEDULE-STATION-PARAM",
    "SCHEDULE-DEVICE-METER"
])
```

#### 5.1.6 test_service_integration

测试完整的服务集成流程。

```python
async def test_service_integration(
    self,
    module_name: str,
    duration_seconds: int = 30
) -> Dict[str, Any]
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| module_name | str | - | 模块名称（如'load_prediction'） |
| duration_seconds | int | 30 | 测试运行时长（秒） |

**返回值**: `Dict[str, Any]` - 测试结果统计

```python
{
    'duration': 30,
    'callback_count': 15,
    'station_count': 3,
    'topics_monitored': 5,
    'stations': ['station_001', 'station_002', 'station_003']
}
```

**功能说明**:
1. 根据module_name获取依赖的topic列表
2. 创建DataDispatcher和Service实例
3. 注册回调函数监控服务输出
4. 启动服务并运行指定时长
5. 统计回调次数、场站数量等信息

**适用场景**: 端到端集成测试，验证完整数据流

**示例**:
```python
result = await tester.test_service_integration(
    module_name="load_prediction",
    duration_seconds=60
)
print(f"回调次数: {result['callback_count']}")
print(f"测试场站: {result['stations']}")
```

#### 5.1.7 print_summary

打印测试结果汇总报告。

```python
def print_summary(self) -> None
```

**功能说明**:
- 统计总topic数、成功/失败数量
- 计算成功率
- 显示每个topic的消息计数
- 展示样本数据的键名和第一条数据
- 列出所有错误日志

**输出格式**:
```
================================================================================
TOPIC CONSUMPTION TEST SUMMARY
================================================================================

Total topics tested: 10
Topics with data: 8
Topics without data: 2
Success rate: 80.0%
Total messages received: 156

✓ Topics successfully consumed:
  - SCHEDULE-STATION-PARAM: 5 messages
  - SCHEDULE-DEVICE-METER: 23 messages
  ...

⚠ Topics with no data:
  - SCHEDULE-CAR-PRICE
  - SCHEDULE-ENVIRONMENT-CALENDAR

📋 Sample messages:
  Topic: SCHEDULE-STATION-PARAM
    Keys: ['station_id', 'station_temp', 'lat', 'lng', ...]
    Sample: {'station_id': 'station_001', 'station_temp': 25.3, ...}

⚠ Errors encountered:
  - Failed to connect to SCHEDULE-CAR-PRICE: timeout
```

**示例**:
```python
tester = TopicConsumeTester()
await tester.test_all_topics_async(all_topics)
tester.print_summary()  # 打印完整报告
```

---

### 5.2 MockDataGenerator 类 (test_mock_producer.py)

模拟Kafka消息数据生成器，根据业务规则生成各类topic的测试数据。

#### 5.2.1 类初始化

```python
MockDataGenerator()
```

**实例属性**:

| 属性名 | 类型 | 说明 |
|--------|------|------|
| station_ids | List[str] | 场站ID列表 ['station_001', 'station_002', 'station_003'] |
| host_ids | List[str] | 主机ID列表 ['host_001', 'host_002', 'host_003'] |
| meter_ids | List[str] | 电表ID列表 ['meter_001', 'meter_002'] |
| gun_ids | List[str] | 充电枪ID列表 ['gun_001', 'gun_002', 'gun_003', 'gun_004'] |

**示例**:
```python
from tests.test_mock_producer import MockDataGenerator

generator = MockDataGenerator()
```

#### 5.2.2 generate_station_param

生成场站参数数据（单值，无窗口）。

```python
def generate_station_param(self, station_id: str) -> Dict[str, Any]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| station_id | str | 场站ID |

**返回值**: `Dict[str, Any]` - 场站参数数据

**返回数据结构**:
```python
{
    "station_id": str,           # 场站ID
    "station_temp": float,       # 场站温度 (20.0~35.0)
    "lat": float,                # 纬度 (30.0~40.0)
    "lng": float,                # 经度 (110.0~120.0)
    "gun_count": int,            # 充电枪数量 (4~12)
    "grid_capacity": int,        # 电网容量kW (500~2000)
    "storage_count": int,        # 储能设备数量 (1~4)
    "storage_capacity": int,     # 储能容量kWh (100~500)
    "host_id": str,              # 主机ID
    "timestamp": float           # 时间戳
}
```

**示例**:
```python
data = generator.generate_station_param("station_001")
print(f"充电枪数量: {data['gun_count']}")
print(f"电网容量: {data['grid_capacity']}kW")
```

#### 5.2.3 generate_station_realtime_data

生成场站实时数据窗口（7天历史曲线）。

```python
def generate_station_realtime_data(
    self,
    station_id: str,
    window_size: int = 100
) -> Dict[str, Any]
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| station_id | str | - | 场站ID |
| window_size | int | 100 | 窗口大小（时间点数量） |

**返回值**: `Dict[str, Any]` - 场站实时数据窗口

**返回数据结构**:
```python
{
    "station_id": str,                        # 场站ID
    "gun_id": str,                            # 充电枪ID
    "history_curve_gun_avg": List[float],     # 枪平均功率窗口（长度=window_size）
    "history_curve_gun_max": List[float],     # 枪最大功率窗口
    "history_curve_station_avg": List[float], # 场站平均功率窗口
    "history_curve_station_max": List[float], # 场站最大功率窗口
    "timestamps": List[float]                 # 时间戳窗口（间隔60秒）
}
```

**时间间隔**: 每分钟一个点

**示例**:
```python
data = generator.generate_station_realtime_data("station_001", window_size=50)
print(f"窗口长度: {len(data['history_curve_station_avg'])}")
print(f"平均功率范围: {min(data['history_curve_station_avg'])} ~ {max(data['history_curve_station_avg'])}")
```

#### 5.2.4 generate_environment_calendar

生成环境日历数据（单值）。

```python
def generate_environment_calendar(self) -> Dict[str, Any]
```

**返回值**: `Dict[str, Any]` - 环境日历数据

**返回数据结构**:
```python
{
    "workday_code": int,    # 0工作日, 1周末
    "holiday_code": int,    # 0正常, 1节假日, 2调休
    "date": str,            # 日期 "YYYY-MM-DD"
    "timestamp": float      # 时间戳
}
```

**示例**:
```python
data = generator.generate_environment_calendar()
if data['workday_code'] == 0:
    print("今天是工作日")
if data['holiday_code'] == 1:
    print("今天是节假日")
```

#### 5.2.5 generate_device_meter

生成电表数据窗口（5分钟间隔）。

```python
def generate_device_meter(
    self,
    meter_id: str,
    window_size: int = 100
) -> Dict[str, Any]
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| meter_id | str | - | 电表ID |
| window_size | int | 100 | 窗口大小 |

**返回值**: `Dict[str, Any]` - 电表数据窗口

**返回数据结构**:
```python
{
    "meter_id": str,                    # 电表ID
    "current_power": List[float],       # 当前功率窗口（50.0~300.0）
    "rated_power_limit": List[float],   # 额定功率限制窗口（固定500.0）
    "timestamps": List[float]           # 时间戳窗口（间隔300秒）
}
```

**时间间隔**: 5分钟（300秒）

**示例**:
```python
data = generator.generate_device_meter("meter_001", window_size=40)
avg_power = sum(data['current_power']) / len(data['current_power'])
print(f"平均功率: {avg_power:.2f}kW")
```

#### 5.2.6 generate_device_gun

生成充电枪数据窗口（15秒间隔）。

```python
def generate_device_gun(
    self,
    host_id: str,
    window_size: int = 100
) -> Dict[str, Any]
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| host_id | str | - | 主机ID |
| window_size | int | 100 | 窗口大小 |

**返回值**: `Dict[str, Any]` - 充电枪数据窗口

**返回数据结构**:
```python
{
    "host_id": str,               # 主机ID
    "gun_id": str,                # 充电枪ID
    "gun_status": List[int],      # 状态窗口（0空闲,1充电中,2故障,3预约）
    "timestamps": List[float]     # 时间戳窗口（间隔15秒）
}
```

**时间间隔**: 15秒

**状态码说明**:
- 0: 空闲
- 1: 充电中
- 2: 故障
- 3: 预约

**示例**:
```python
data = generator.generate_device_gun("host_001", window_size=40)
charging_count = data['gun_status'].count(1)
print(f"充电中状态次数: {charging_count}")
```

#### 5.2.7 generate_car_order

生成订单数据窗口（1秒间隔）。

```python
def generate_car_order(
    self,
    station_id: str,
    window_size: int = 100
) -> Dict[str, Any]
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| station_id | str | - | 场站ID |
| window_size | int | 100 | 窗口大小 |

**返回值**: `Dict[str, Any]` - 订单数据窗口

**返回数据结构**:
```python
{
    "station_id": str,                # 场站ID
    "order_id": str,                  # 订单ID
    "charger_id": str,                # 充电机ID
    "gun_id": str,                    # 充电枪ID
    "charger_rated_current": float,   # 额定电流（250.0A）
    "start_time": float,              # 开始时间戳
    "end_time": float,                # 结束时间戳
    "start_SOC": float,               # 起始SOC（20.0%）
    "current_SOC": List[float],       # SOC窗口（逐渐增加20%~100%）
    "demand_voltage": List[float],    # 需求电压窗口（350~450V）
    "demand_current": List[float],    # 需求电流窗口（50~200A）
    "mileage": int,                   # 里程数（1000~100000km）
    "car_model": str,                 # 车型
    "battery_capacity": float,        # 电池容量（60/75/90/100kWh）
    "timestamps": List[float]         # 时间戳窗口（间隔1秒）
}
```

**时间间隔**: 1秒

**SOC变化**: 从20%线性增加到接近100%

**示例**:
```python
data = generator.generate_car_order("station_001", window_size=30)
soc_increase = data['current_SOC'][-1] - data['current_SOC'][0]
print(f"订单ID: {data['order_id']}")
print(f"SOC增加: {soc_increase:.1f}%")
print(f"车型: {data['car_model']}")
```

#### 5.2.8 generate_car_price

生成电价数据（单值，包含多个时段）。

```python
def generate_car_price(self, station_id: str) -> Dict[str, Any]
```

| 参数 | 类型 | 说明 |
|------|------|------|
| station_id | str | 场站ID |

**返回值**: `Dict[str, Any]` - 电价数据

**返回数据结构**:
```python
{
    "station_id": str,
    "periods": List[Dict],  # 时段列表（4个时段，每个6小时）
    "timestamp": float
}

# periods中每个元素:
{
    "period_no": int,       # 时段编号（1~4）
    "start_time": str,      # 开始时间 "HH:MM"
    "end_time": str,        # 结束时间 "HH:MM"
    "period_type": int,     # 1峰, 2平, 3谷
    "grid_price": float,    # 电网电价（0.3~1.2元/kWh）
    "service_fee": float    # 服务费（0.1~0.5元/kWh）
}
```

**时段划分**: 0-6时, 6-12时, 12-18时, 18-24时

**示例**:
```python
data = generator.generate_car_price("station_001")
for period in data['periods']:
    total_price = period['grid_price'] + period['service_fee']
    period_name = {1: '峰', 2: '平', 3: '谷'}[period['period_type']]
    print(f"{period['start_time']}-{period['end_time']} {period_name}时段: {total_price:.2f}元/kWh")
```

#### 5.2.9 generate_device_error

生成设备错误数据窗口。

```python
def generate_device_error(
    self,
    station_id: str,
    window_size: int = 10
) -> Dict[str, Any]
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| station_id | str | - | 场站ID |
| window_size | int | 10 | 窗口大小 |

**返回值**: `Dict[str, Any]` - 设备错误数据窗口

**返回数据结构**:
```python
{
    "station_id": str,
    "host_error": List[int],        # 主机错误（0正常,1故障）
    "ac_error": List[int],          # AC错误
    "dc_error": List[int],          # DC错误
    "terminal_error": List[int],    # 终端错误
    "storage_error": List[int],     # 储能错误
    "timestamps": List[float]       # 时间戳（间隔60~600秒随机）
}
```

**时间间隔**: 60~600秒随机

**示例**:
```python
data = generator.generate_device_error("station_001", window_size=5)
total_errors = sum(data['host_error']) + sum(data['ac_error']) + sum(data['dc_error'])
print(f"总错误次数: {total_errors}")
```

#### 5.2.10 generate_device_host

生成主机数据窗口（1秒或15秒间隔）。

```python
def generate_device_host(
    self,
    host_id: str,
    window_size: int = 100
) -> Dict[str, Any]
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| host_id | str | - | 主机ID |
| window_size | int | 100 | 窗口大小 |

**返回值**: `Dict[str, Any]` - 主机数据窗口

**返回数据结构**:
```python
{
    "host_id": str,
    "acdc_status": List[int],           # ACDC状态（0停机,1运行,2故障）
    "dcdc_input_power": List[float],    # DCDC输入功率（10.0~200.0kW）
    "acdc_input_power": List[float],    # ACDC输入功率（10.0~200.0kW）
    "timestamps": List[float]           # 时间戳（间隔1秒或15秒）
}
```

**时间间隔**: 1秒或15秒（随机选择）

**示例**:
```python
data = generator.generate_device_host("host_001", window_size=40)
avg_dcdc = sum(data['dcdc_input_power']) / len(data['dcdc_input_power'])
print(f"DCDC平均功率: {avg_dcdc:.2f}kW")
```

#### 5.2.11 generate_device_storage

生成储能数据窗口（15秒间隔）。

```python
def generate_device_storage(
    self,
    host_id: str,
    window_size: int = 100
) -> Dict[str, Any]
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| host_id | str | - | 主机ID |
| window_size | int | 100 | 窗口大小 |

**返回值**: `Dict[str, Any]` - 储能数据窗口

**返回数据结构**:
```python
{
    "host_id": str,
    "storage_id": str,                  # 储能设备ID
    "storage_power": List[float],       # 功率（-100~100kW，负值放电，正值充电）
    "storage_current": List[float],     # 电流（-50~50A）
    "storage_temp_max": List[float],    # 最高温度（25~45°C）
    "storage_temp_min": List[float],    # 最低温度（20~35°C）
    "storage_SOC": List[float],         # SOC（20~100%）
    "storage_SOH": List[float],         # SOH健康度（85~100%）
    "timestamps": List[float]           # 时间戳（间隔15秒）
}
```

**时间间隔**: 15秒

**示例**:
```python
data = generator.generate_device_storage("host_001", window_size=40)
avg_soc = sum(data['storage_SOC']) / len(data['storage_SOC'])
avg_soh = sum(data['storage_SOH']) / len(data['storage_SOH'])
print(f"平均SOC: {avg_soc:.1f}%")
print(f"平均SOH: {avg_soh:.1f}%")
```

---

### 5.3 MockProducer 类 (test_mock_producer.py)

模拟Kafka生产者，持续发送测试数据到Kafka。

#### 5.3.1 类初始化

```python
MockProducer(
    kafka_config: Optional[Dict] = None,
    topic_detail: Optional[Dict] = None
)
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| kafka_config | Optional[Dict] | None | Kafka配置，为None时使用KAFKA_CONFIG |
| topic_detail | Optional[Dict] | None | Topic详情配置，为None时使用TOPIC_DETAIL |

**实例属性**:

| 属性名 | 类型 | 说明 |
|--------|------|------|
| kafka_config | Dict | Kafka连接配置 |
| topic_detail | Dict | Topic详细信息 |
| generator | MockDataGenerator | 数据生成器实例 |
| producer | Optional[AsyncKafkaProducerClient] | Kafka生产者客户端 |
| running | bool | 运行状态标志 |

**示例**:
```python
from tests.test_mock_producer import MockProducer

# 使用默认配置
producer = MockProducer()

# 使用自定义配置
custom_config = {'bootstrap_servers': 'localhost:9092'}
producer = MockProducer(kafka_config=custom_config)
```

#### 5.3.2 start

启动生产者连接。

```python
async def start(self) -> None
```

**功能说明**:
- 创建AsyncKafkaProducerClient实例
- 建立与Kafka的连接
- 记录启动日志

**异常**: 连接失败时抛出异常

**示例**:
```python
producer = MockProducer()
await producer.start()
```

#### 5.3.3 stop

停止生产者并关闭连接。

```python
async def stop(self) -> None
```

**功能说明**:
- 设置running标志为False
- 关闭生产者连接
- 记录停止日志

**示例**:
```python
await producer.stop()
```

#### 5.3.4 produce_topic_data

发送单条topic数据。

```python
async def produce_topic_data(
    self,
    topic: str,
    data: Dict
) -> None
```

| 参数 | 类型 | 说明 |
|------|------|------|
| topic | str | 目标topic名称 |
| data | Dict | 要发送的数据字典 |

**功能说明**:
- 检查生产者是否已启动
- 使用producer.send()发送数据
- 记录发送日志（显示topic和数据键名）
- 失败时记录错误日志

**异常**: 生产者未启动时抛出RuntimeError

**示例**:
```python
data = generator.generate_station_param("station_001")
await producer.produce_topic_data("SCHEDULE-STATION-PARAM", data)
```

#### 5.3.5 run_continuous

持续运行指定时长，定期发送各topic数据。

```python
async def run_continuous(
    self,
    duration_seconds: int = 60,
    interval_seconds: int = 5
) -> None
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| duration_seconds | int | 60 | 总运行时长（秒） |
| interval_seconds | int | 5 | 每次发送间隔（秒） |

**功能说明**:
- 设置running标志为True
- 在duration_seconds时长内循环运行
- 每隔interval_seconds发送一批数据
- 为每个场站生成和发送以下topic数据：
  - 场站参数（每10次迭代发送1次）
  - 场站实时数据（每次）
  - 订单数据（每次）
  - 电价数据（每20次迭代发送1次）
  - 设备错误（20%概率触发）
- 为每个主机生成和发送：
  - 主机数据（每次）
  - 充电枪数据（每次）
  - 储能数据（每次）
- 为每个电表生成和发送电表数据
- 环境日历（每30次迭代发送1次）
- 记录迭代次数和完成日志

**数据发送频率控制**:

| Topic | 发送频率 | 说明 |
|-------|----------|------|
| SCHEDULE-STATION-PARAM | 低频（每10次） | 配置类数据 |
| SCHEDULE-STATION-REALTIME-DATA | 每次 | 实时监控数据 |
| SCHEDULE-CAR-ORDER | 每次 | 订单数据 |
| SCHEDULE-CAR-PRICE | 低频（每20次） | 电价策略 |
| SCHEDULE-DEVICE-ERROR | 随机（20%） | 故障数据 |
| SCHEDULE-DEVICE-HOST | 每次 | 主机状态 |
| SCHEDULE-DEVICE-GUN | 每次 | 充电枪状态 |
| SCHEDULE-DEVICE-STORAGE | 每次 | 储能状态 |
| SCHEDULE-DEVICE-METER | 每次 | 电表数据 |
| SCHEDULE-ENVIRONMENT-CALENDAR | 低频（每30次） | 日历数据 |

**窗口大小**:
- 场站实时数据: 50个点
- 订单数据: 30个点
- 设备错误: 5个点
- 主机/枪/储能/电表: 40个点

**示例**:
```python
producer = MockProducer()
await producer.start()

# 运行5分钟，每10秒发送一批数据
await producer.run_continuous(
    duration_seconds=300,
    interval_seconds=10
)

await producer.stop()
```

**使用场景**:
- 长时间集成测试
- 性能压力测试
- 服务稳定性验证
- 数据流监控

**注意事项**:
- 必须先调用start()启动生产者
- 完成后应调用stop()释放资源
- interval_seconds不宜过小（建议≥5秒）
- duration_seconds应根据测试需求设置

---

### 5.4 测试入口函数 (run_tests.py)

命令行测试工具，提供统一的测试入口。

#### 5.4.1 main

主函数，解析命令行参数并执行相应测试。

```python
def main() -> None
```

**命令行参数**:

| 参数 | 类型 | 选项 | 说明 |
|------|------|------|------|
| test_type | 位置参数 | consume, produce, quick, module, connectivity | 测试类型 |
| --module | 可选 | 字符串 | 模块名称（默认: load_prediction） |
| --duration | 可选 | 整数 | 运行时长秒数（默认: 60） |
| --timeout | 可选 | 整数 | 超时时间秒数（默认: 20） |

**测试类型说明**:

1. **connectivity**: Kafka连接性测试
   ```bash
   python tests/run_tests.py connectivity
   ```
   - 调用test_kafka_connectivity()
   - 验证Kafka服务可访问性
   - 返回连接成功/失败状态

2. **quick**: 快速消费测试
   ```bash
   python tests/run_tests.py quick --timeout 30
   ```
   - 调用quick_test(timeout)
   - 测试前5个topic的消费
   - 适用于快速验证

3. **consume**: 完整消费测试
   ```bash
   python tests/run_tests.py consume
   ```
   - 调用test_kafka_consume.main()
   - 测试所有topic的消费
   - 生成详细报告

4. **module**: 模块特定topic测试
   ```bash
   python tests/run_tests.py module --module load_prediction --timeout 30
   ```
   - 调用test_module_topics(module_name)
   - 测试指定模块依赖的所有topic
   - 验证模块集成

5. **produce**: 模拟数据生产
   ```bash
   python tests/run_tests.py produce --duration 300
   ```
   - 调用test_mock_producer.main()
   - 启动模拟生产者持续发送数据
   - 用于测试数据准备

**示例使用场景**:

```bash
# 场景1: 环境初次验证
python tests/run_tests.py connectivity

# 场景2: 开发中快速检查
python tests/run_tests.py quick --timeout 15

# 场景3: 完整功能测试
python tests/run_tests.py consume

# 场景4: 测试负载预测模块
python tests/run_tests.py module --module load_prediction

# 场景5: 准备测试数据（10分钟）
python tests/run_tests.py produce --duration 600
```

---

### 5.5 独立测试函数

位于`test_kafka_consume.py`中的全局测试函数。

#### 4.5.1 test_kafka_connectivity

测试Kafka服务连接性。

```python
async def test_kafka_connectivity() -> bool
```

**返回值**: `bool` - 连接是否成功

**功能说明**:
- 创建AsyncKafkaConsumerClient实例
- 尝试启动消费者连接
- 获取可用topic列表
- 显示连接状态和topic数量
- 关闭连接

**输出示例**:
```
Testing Kafka connectivity...
✓ Successfully connected to Kafka
Available topics: 10
  - SCHEDULE-STATION-PARAM
  - SCHEDULE-DEVICE-METER
  ...
```

**示例**:
```python
success = await test_kafka_connectivity()
if not success:
    print("Kafka服务不可用")
    sys.exit(1)
```

#### 5.5.2 quick_test

快速测试前N个topic。

```python
async def quick_test(timeout: int = 20, num_topics: int = 5) -> None
```

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| timeout | int | 20 | 每个topic超时时间 |
| num_topics | int | 5 | 测试topic数量 |

**功能说明**:
- 从TOPIC_DETAIL中选取前num_topics个topic
- 使用TopicConsumeTester测试这些topic
- 打印汇总报告

**示例**:
```python
# 快速测试前3个topic，每个超时15秒
await quick_test(timeout=15, num_topics=3)
```

#### 5.5.3 test_module_topics

测试特定模块的所有依赖topic。

```python
async def test_module_topics(module_name: str) -> None
```

| 参数 | 类型 | 说明 |
|------|------|------|
| module_name | str | 模块名称（如'load_prediction'） |

**功能说明**:
- 从MODULE_TO_TOPICS获取模块依赖的topic列表
- 使用TopicConsumeTester测试所有topic
- 打印测试结果

**支持的模块**:
- load_prediction: 负载预测
- operation_optimization: 运行优化
- thermal_management: 热管理

**示例**:
```python
await test_module_topics("load_prediction")
```

#### 5.5.4 main (test_kafka_consume.py)

完整的消费测试主函数。

```python
async def main() -> None
```

**功能说明**:
1. 测试Kafka连接性
2. 测试所有配置的topic
3. 打印详细汇总报告

**示例**:
```python
asyncio.run(main())
```

---

## 6. 测试用例说明

### 6.1 单元测试

#### 6.1.1 test_dispatcher.py

**测试目标**: DataDispatcher数据窗口管理

**测试用例**:

1. **test_window_padding**
   - 验证窗口数据补全功能
   - 测试不足窗口长度时的填充策略
   - 断言: `len(result['current_power_window']) == win_size`

2. **test_expired_clean**
   - 验证过期数据清理功能
   - 测试数据过期后自动删除
   - 断言: `station_id not in dispatcher.data_cache`

**关键代码**:
```python
def test_window_padding():
    dispatcher = DataDispatcher(data_expire_seconds=60)
    station_id = 'test_station'
    topic = 'SCHEDULE-DEVICE-METER'
    
    # 插入少量数据
    for i in range(3):
        dispatcher.update_topic_data(
            station_id, topic,
            {'meter_id': 1, 'current_power': i, 'rated_power_limit': 10}
        )
    
    # 获取模块输入，验证窗口补全
    result = dispatcher.get_module_input(station_id, 'operation_optimization')
    assert 'current_power_window' in result
```

#### 6.1.2 test_dispatcher_padding.py

**测试目标**: 数据补全策略

**补全策略说明**:

| 策略 | 说明 | 适用场景 |
|------|------|----------|
| `zero` | 零填充 | 默认策略，简单快速 |
| `linear` | 线性插值 | 连续数值数据 |
| `forward` | 前向填充 | 保持最后有效值 |
| `missing` | None标记 | 明确标识缺失 |

**测试用例**:

1. **test_zero_padding**
   ```python
   # 输入: [1, 2], 目标长度: 5
   # 输出: [0, 0, 0, 1, 2]
   ```

2. **test_linear_padding**
   ```python
   # 输入: [1, 3], 目标长度: 4
   # 输出: [1.0, 1.67, 2.33, 3.0] (线性插值)
   ```

3. **test_forward_padding**
   ```python
   # 输入: [5], 目标长度: 3
   # 输出: [5, 5, 5]
   ```

4. **test_missing_padding**
   ```python
   # 输入: [], 目标长度: 2
   # 输出: [None, None]
   ```

#### 6.1.3 test_dependency.py

**测试目标**: 模块间依赖关系处理

**依赖关系图**:

```
load_prediction
    ↓
operation_optimization
    ↑
thermal_management
```

**测试用例**:

1. **test_dependency_aggregation**
   - 验证依赖模块的输出聚合
   - 测试字段级联传递
   - 断言依赖字段存在于结果中

#### 6.1.4 test_mock_data_generator.py

**测试目标**: 数据生成器功能验证

**测试覆盖**:

- ✅ 所有10个topic的数据生成
- ✅ 窗口大小配置
- ✅ 数据范围验证
- ✅ 时间戳递增验证
- ✅ 多场站数据生成

**关键测试**:

```python
def test_station_realtime_data_generation():
    generator = MockDataGenerator()
    window_size = 50
    data = generator.generate_station_realtime_data(
        "test_station", window_size
    )
    
    # 验证窗口长度
    assert len(data["history_curve_station_avg"]) == window_size
    
    # 验证时间戳递增
    timestamps = data["timestamps"]
    for i in range(1, len(timestamps)):
        assert timestamps[i] > timestamps[i-1]
```

### 6.2 集成测试

#### 6.2.1 test_async_service.py

**测试目标**: 异步服务完整流程

**测试流程**:

```
1. 创建AsyncDataAnalysisService
2. Mock Kafka Consumer/Producer
3. 注入测试消息
4. 启动服务并设置回调
5. 验证回调执行
6. 验证结果上传
7. 停止服务
```

**关键断言**:

```python
assert service.producer.sent  # 验证数据已上传
assert service.get_station_status()['S']['running'] is False  # 验证状态
```

#### 6.2.2 test_kafka_upload.py

**测试目标**: Kafka结果上传功能

**测试步骤**:

1. Mock KafkaProducerClient.send方法
2. 启动DataAnalysisService
3. 模拟数据输入
4. 触发回调返回结果
5. 验证上传的topic和数据格式

#### 6.2.3 test_integration_extra.py

**测试目标**: 扩展集成功能

**测试用例**:

1. **test_health_monitoring**
   - 验证服务健康状态监控
   - 测试场站任务状态查询
   - 断言状态字段完整性

2. **test_reload_config**
   - 验证配置热更新功能
   - 测试运行时配置变更
   - 断言配置生效

3. **test_async_add_remove_station**
   - 验证动态添加/移除场站
   - 测试异步任务管理
   - 断言任务正确启停

#### 6.2.4 test_extreme_and_integration.py

**测试目标**: 边界条件和异常处理

**极端场景测试**:

1. **空数据窗口**
   ```python
   @pytest.mark.parametrize("padding, seq, target", [
       ('zero', [], 3),
       ('linear', [], 2),
   ])
   ```

2. **无效Topic**
   ```python
   dispatcher.update_topic_data('sid', 'INVALID_TOPIC', {'x': 1})
   # 应该捕获异常，不抛出
   ```

3. **回调异常**
   ```python
   async def bad_cb(station_id, module_input):
       raise ValueError("test error")
   # 服务应继续运行，不崩溃
   ```

4. **真实Kafka集成**
   ```python
   # 检测本地Kafka服务
   if not kafka_port_open():
       pytest.skip("Kafka未运行")
   # 进行真实的生产消费测试
   ```

---

## 7. 使用指南

### 7.1 快速开始

#### 7.1.1 环境准备

```bash
# 1. 克隆项目
git clone <repository>
cd data_analysis

# 2. 安装依赖
pip install -r requirements.txt

# 3. 配置Kafka
# 编辑 d_a/config.py，设置bootstrap_servers
```

#### 7.1.2 运行测试

**方式1: 使用便捷脚本（推荐）**

```bash
# 验证环境
python tests/run_tests.py connectivity

# 快速测试
python tests/run_tests.py quick

# 完整测试
python tests/run_tests.py consume
```

**方式2: 使用pytest**

```bash
# 运行所有单元测试
pytest tests/test_dispatcher.py -v
pytest tests/test_dependency.py -v

# 运行特定测试
pytest tests/test_dispatcher.py::test_window_padding -v

# 运行异步测试
pytest tests/test_async_service.py -v
```

**方式3: 直接运行测试文件**

```bash
python tests/test_kafka_consume.py
python tests/test_mock_producer.py
```

### 7.2 开发流程测试

#### 7.2.1 新功能开发

```bash
# 1. 启动模拟生产者（终端1）
python tests/run_tests.py produce --duration 600

# 2. 开发调试（终端2）
python d_a/main.py

# 3. 测试验证（终端3）
python tests/run_tests.py module --module <your_module>
```

#### 7.2.2 Bug修复验证

```bash
# 1. 编写针对性测试用例
# tests/test_bugfix_xxx.py

# 2. 运行测试验证Bug存在
pytest tests/test_bugfix_xxx.py -v

# 3. 修复代码

# 4. 再次运行测试验证修复
pytest tests/test_bugfix_xxx.py -v
```

#### 7.2.3 集成测试

```python
# tests/test_custom_integration.py
import asyncio
from tests.test_mock_producer import MockProducer
from tests.test_kafka_consume import TopicConsumeTester

async def integration_test():
    # 启动生产者
    producer = MockProducer()
    await producer.start()
    producer_task = asyncio.create_task(
        producer.run_continuous(60, 5)
    )
    
    # 等待数据生成
    await asyncio.sleep(10)
    
    # 测试消费
    tester = TopicConsumeTester()
    results = await tester.test_all_topics_async(
        list(TOPIC_DETAIL.keys()),
        timeout_per_topic=15
    )
    
    tester.print_summary()
    await producer_task
    await producer.stop()

asyncio.run(integration_test())
```



## 8. 配置说明

### 8.1 Kafka配置

**配置文件**: `d_a/config.py`

**核心配置项**:

```python
KAFKA_CONFIG = {
    'bootstrap_servers': ['10.8.4.40:35888'],  # Kafka服务器地址
    'consumer': {
        'group_id': 'stack-charge-tcp-command',  # 消费者组ID
        'auto_offset_reset': 'latest',  # 偏移量重置策略
        'enable_auto_commit': False,     # 手动提交偏移量
        'max_poll_records': 3000,        # 单次poll最大记录数
    },
    'producer': {
        # 生产者配置
    }
}
```

**偏移量策略**:

| 策略 | 说明 | 适用场景 |
|------|------|----------|
| `latest` | 从最新消息开始 | 生产环境，只关注新数据 |
| `earliest` | 从最早消息开始 | 测试环境，需要历史数据 |

**测试时修改**:

```python
# 临时修改配置进行测试
test_config = {
    'bootstrap_servers': ['localhost:9092'],
    'consumer': {
        'group_id': 'test-group',
        'auto_offset_reset': 'earliest',  # 读取历史数据
    }
}

tester = TopicConsumeTester(kafka_config=test_config)
```

### 8.2 Topic配置

**配置位置**: `d_a/config.py` -> `TOPIC_DETAIL`

**配置结构**:

```python
TOPIC_DETAIL = {
    'TOPIC_NAME': {
        'fields': ['field1', 'field2', ...],  # 字段列表
        'frequency': '更新频率描述',            # 更新频率
        'modules': ['module1', 'module2'],    # 使用该topic的模块
        'window_size': 100,                   # 窗口大小
    }
}
```

**示例**:

```python
'SCHEDULE-STATION-REALTIME-DATA': {
    'fields': [
        'station_id', 'gun_id',
        'history_curve_gun_avg',
        'history_curve_gun_max',
        'history_curve_station_avg',
        'history_curve_station_max'
    ],
    'frequency': '1小时1次，推送7天',
    'modules': [
        'load_prediction',
        'operation_optimization',
        'electricity_price',
        'SOH_model',
        'thermal_management',
        'evaluation_model'
    ],
    'window_size': 7*24  # 7天×24小时
}
```

### 8.3 模块依赖配置

**配置位置**: `d_a/config.py` -> `MODULE_DEPENDENCIES`

```python
MODULE_DEPENDENCIES = {
    'electricity_price': ['pv_prediction', 'evaluation_model', 'SOH_model'],
    'station_guidance': ['load_prediction', 'evaluation_model'],
    'thermal_management': ['load_prediction', 'operation_optimization'],
    'operation_optimization': ['load_prediction'],
}
```

**依赖关系说明**:

- Key: 目标模块
- Value: 依赖的上游模块列表
- 系统自动聚合上游输出到目标模块输入

### 8.4 测试参数配置

**TopicConsumeTester参数**:

```python
tester = TopicConsumeTester(
    kafka_config=custom_config  # 自定义Kafka配置
)
```

**测试超时配置**:

```python
# 单个topic超时
await tester.test_single_topic_async(
    "TOPIC_NAME",
    timeout_seconds=30  # 30秒超时
)

# 批量测试超时
await tester.test_all_topics_async(
    topics,
    timeout_per_topic=20  # 每个topic 20秒
)
```

**服务集成测试配置**:

```python
result = await tester.test_service_integration(
    module_name="load_prediction",
    duration_seconds=60  # 运行60秒
)
```

**MockProducer配置**:

```python
producer = MockProducer(
    kafka_config=custom_config,  # Kafka配置
    topic_detail=custom_topics   # Topic配置
)

await producer.run_continuous(
    duration_seconds=300,  # 运行时长
    interval_seconds=10    # 发送间隔
)
```

---


## 9. 最佳实践

### 9.1 测试编写规范

#### 9.1.1 命名规范

```python
# 测试文件: test_<module_name>.py
test_dispatcher.py
test_kafka_consume.py

# 测试函数: test_<what_it_tests>
def test_window_padding():
    pass

def test_expired_clean():
    pass

# 测试类: Test<ClassName>
class TestDispatcher:
    def test_update_data(self):
        pass
```

#### 9.1.2 测试结构

```python
def test_feature():
    # 1. Arrange: 准备测试数据和环境
    dispatcher = DataDispatcher()
    station_id = "test_station"
    test_data = {"field": "value"}
    
    # 2. Act: 执行被测试的操作
    dispatcher.update_topic_data(station_id, "TOPIC", test_data)
    result = dispatcher.get_module_input(station_id, "module")
    
    # 3. Assert: 验证结果
    assert result is not None
    assert "field" in result
    
    # 4. Cleanup: 清理资源（如需要）
    dispatcher.clean_expired()
```

#### 9.1.3 使用Fixtures

```python
import pytest

@pytest.fixture
def dispatcher():
    """提供预配置的dispatcher实例"""
    d = DataDispatcher(data_expire_seconds=60)
    yield d
    # 清理代码（如需要）

@pytest.fixture
def test_data():
    """提供测试数据"""
    return {
        "station_id": "test_001",
        "value": 123
    }

def test_with_fixtures(dispatcher, test_data):
    dispatcher.update_topic_data(
        test_data["station_id"],
        "TOPIC",
        test_data
    )
    assert dispatcher.get_topic_window(
        test_data["station_id"],
        "TOPIC"
    )
```

### 9.2 测试覆盖率

#### 9.2.1 运行覆盖率测试

```bash
# 安装coverage工具
pip install pytest-cov

# 运行测试并生成覆盖率报告
pytest tests/ --cov=d_a --cov-report=html --cov-report=term

# 查看HTML报告
# 打开 htmlcov/index.html
```

#### 9.2.2 目标覆盖率

| 组件 | 目标覆盖率 | 当前状态 |
|------|-----------|---------|
| Dispatcher | 90% | ✅ |
| Service | 85% | ✅ |
| Parser | 80% | ✅ |
| Kafka Client | 75% | ⚠️ |

### 9.3 持续集成最佳实践

#### 9.3.1 分层测试策略

```yaml
# 快速测试（每次提交）
- 单元测试
- 静态代码分析
- 代码格式检查

# 完整测试（Pull Request）
- 单元测试
- 集成测试
- 代码覆盖率检查

# 全面测试（发布前）
- 所有测试
- 性能测试
- 压力测试
- 端到端测试
```

#### 9.3.2 测试数据管理

```python
# 使用固定的测试数据集
TEST_STATIONS = ["station_001", "station_002", "station_003"]
TEST_TOPICS = [
    "SCHEDULE-STATION-PARAM",
    "SCHEDULE-STATION-REALTIME-DATA"
]

# 版本化测试数据
# tests/fixtures/v1.0/
#   - station_data.json
#   - realtime_data.json
```

### 9.4 性能测试

#### 9.4.1 基准测试

```python
import time
import statistics

def benchmark_dispatcher():
    dispatcher = DataDispatcher()
    times = []
    
    for _ in range(1000):
        start = time.time()
        dispatcher.update_topic_data(
            "station_001",
            "TOPIC",
            {"value": 1}
        )
        times.append(time.time() - start)
    
    print(f"平均耗时: {statistics.mean(times)*1000:.2f}ms")
    print(f"P95: {statistics.quantiles(times, n=20)[18]*1000:.2f}ms")
    print(f"P99: {statistics.quantiles(times, n=100)[98]*1000:.2f}ms")
```

#### 9.4.2 压力测试

```python
async def stress_test():
    """并发场站压力测试"""
    service = AsyncDataAnalysisService()
    
    # 模拟1000个场站
    stations = [f"station_{i:04d}" for i in range(1000)]
    
    async def callback(station_id, module_input):
        return {"result": "ok"}
    
    await service.start(callback=callback)
    
    # 添加所有场站
    for sid in stations:
        await service.add_station(sid)
    
    # 运行10分钟
    await asyncio.sleep(600)
    
    # 统计
    status = service.get_station_status()
    print(f"活跃场站: {len([s for s in status.values() if s['running']])}")
    
    await service.stop()
```

### 9.5 测试文档

#### 9.5.1 文档字符串

```python
def test_complex_feature():
    """
    测试复杂功能的边界条件。
    
    测试场景:
    1. 正常情况: 完整数据输入
    2. 边界情况: 空数据输入
    3. 异常情况: 非法数据输入
    
    预期结果:
    - 正常情况返回正确结果
    - 边界情况返回默认值
    - 异常情况抛出特定异常
    
    已知限制:
    - 不支持超大窗口(>10000)
    
    相关Issue: #123, #456
    """
    pass
```

#### 9.5.2 测试报告

定期生成测试报告:

```bash
# 生成JUnit格式报告
pytest tests/ --junit-xml=test-results.xml

# 生成HTML报告
pytest tests/ --html=test-report.html --self-contained-html

# 集成到CI系统
# 配置CI自动上传测试报告到Dashboard
```

---

## 附录

### A. 测试文件清单

| 文件 | 类型 | 说明 | 优先级 |
|------|------|------|-------|
| run_tests.py | 工具 | 统一测试入口 | ⭐⭐⭐ |
| test_kafka_consume.py | 工具 | Kafka消费测试 | ⭐⭐⭐ |
| test_mock_producer.py | 工具 | 模拟数据生产 | ⭐⭐⭐ |
| test_dispatcher.py | 单元测试 | Dispatcher功能 | ⭐⭐⭐ |
| test_dependency.py | 单元测试 | 依赖关系 | ⭐⭐ |
| test_dispatcher_padding.py | 单元测试 | 数据补全 | ⭐⭐ |
| test_mock_data_generator.py | 单元测试 | 数据生成器 | ⭐⭐ |
| test_async_service.py | 集成测试 | 异步服务 | ⭐⭐⭐ |
| test_kafka_upload.py | 集成测试 | Kafka上传 | ⭐⭐ |
| test_integration_extra.py | 集成测试 | 扩展功能 | ⭐⭐ |
| test_extreme_and_integration.py | 集成测试 | 边界条件 | ⭐⭐ |

### B. 快速参考

#### B.1 常用命令

```bash
# 连接测试
python tests/run_tests.py connectivity

# 快速测试
python tests/run_tests.py quick

# 模块测试
python tests/run_tests.py module --module load_prediction

# 生产数据
python tests/run_tests.py produce --duration 120

# 单元测试
pytest tests/test_dispatcher.py -v

# 覆盖率测试
pytest tests/ --cov=d_a --cov-report=term
```

#### B.2 配置文件位置

```
d_a/config.py               # 主配置文件
  ├── KAFKA_CONFIG          # Kafka配置
  ├── TOPIC_DETAIL          # Topic配置
  ├── MODULE_DEPENDENCIES   # 依赖关系
  └── MODULE_OUTPUT_TOPICS  # 输出映射
```

#### B.3 重要类和函数

```python
# 测试工具
TopicConsumeTester          # Kafka消费测试
MockDataGenerator           # 数据生成器
MockProducer               # 模拟生产者

# 测试函数
test_kafka_connectivity()   # 连接测试
quick_test()               # 快速测试
test_module_topics()       # 模块测试
test_service_integration() # 集成测试
```

### C. 版本历史

| 版本 | 日期 | 变更内容 |
|------|------|----------|
| 1.0 | 2025-11-06 | 初始版本，包含完整测试套件 |
| 1.1 | 2025-11-10 | 添加覆盖率报告，优化测试数据管理 |
| 1.2 | 2025-11-15 | 增加性能测试，更新文档结构 |
| 1.3 | 2025-11-20 | 优化测试报告，添加基准测试 |
| 1.4 | 2025-11-25 | 完善文档字符，增加测试文档 |

---

## 附录：API函数索引

### 消费测试相关
- `TopicConsumeTester.__init__()` - 初始化消费测试器
- `TopicConsumeTester.test_single_topic_async()` - 异步测试单个topic
- `TopicConsumeTester.test_single_topic_sync()` - 同步测试单个topic  
- `TopicConsumeTester.test_all_topics_async()` - 异步批量测试
- `TopicConsumeTester.test_all_topics_sync()` - 同步批量测试
- `TopicConsumeTester.test_service_integration()` - 服务集成测试
- `TopicConsumeTester.print_summary()` - 打印测试报告
- `test_kafka_connectivity()` - 测试Kafka连接
- `quick_test()` - 快速测试
- `test_module_topics()` - 模块topic测试

### 数据生成相关
- `MockDataGenerator.__init__()` - 初始化数据生成器
- `MockDataGenerator.generate_station_param()` - 场站参数
- `MockDataGenerator.generate_station_realtime_data()` - 场站实时数据
- `MockDataGenerator.generate_environment_calendar()` - 环境日历
- `MockDataGenerator.generate_device_meter()` - 电表数据
- `MockDataGenerator.generate_device_gun()` - 充电枪数据
- `MockDataGenerator.generate_car_order()` - 订单数据
- `MockDataGenerator.generate_car_price()` - 电价数据
- `MockDataGenerator.generate_device_error()` - 设备错误
- `MockDataGenerator.generate_device_host()` - 主机数据
- `MockDataGenerator.generate_device_storage()` - 储能数据

### 数据生产相关
- `MockProducer.__init__()` - 初始化生产者
- `MockProducer.start()` - 启动生产者
- `MockProducer.stop()` - 停止生产者
- `MockProducer.produce_topic_data()` - 发送单条数据
- `MockProducer.run_continuous()` - 持续运行

### 测试入口
- `run_tests.main()` - 命令行入口函数

---

**文档结束**

*本说明书包含完整的测试工具文档和API接口详解*  
*版本: 1.0 | 最后更新: 2025-11-24*
