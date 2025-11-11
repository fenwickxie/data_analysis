# API 文档

> **文档版本**：v1.1  
> **更新日期**：2025-11-07  
> **对应代码版本**：data_analysis v1.1 (branch: feature-one)

本文档详细说明data_analysis模块的公共API、回调函数规范和配置参数。

**修订说明**：
- v1.1 (2025-11-07): 更新以匹配实际代码实现
  - 添加 `result_handler` 参数到服务构造函数
  - 修正方法名：`get_all_outputs` → `get_all_inputs`
  - 移除未实现的 `set_padding_strategy` 方法
  - 更新配置参数说明以匹配 config.py
  - 修正字段命名规范（camelCase）
  - 完善Kafka客户端参数白名单说明

## 目录

1. [公共API参考](#公共api参考)
2. [回调函数规范](#回调函数规范)
3. [配置参数说明](#配置参数说明)

## 公共API参考

### DataAnalysisService 类

同步数据解析服务，适合传统多线程/阻塞模型。

#### 初始化

```python
DataAnalysisService(
    module_name=None,
    topics=None,
    kafka_config=None,
    data_expire_seconds=600,
    output_topic_prefix="MODULE-OUTPUT-",
    result_handler=None
)
```

**参数**:
- `module_name` (str, optional): 业务模块名称，用于标识当前处理模块。默认为None。
- `topics` (list, optional): 需要消费的topic列表，默认为配置文件中所有topic。
- `kafka_config` (dict, optional): Kafka连接配置，默认为config.KAFKA_CONFIG。
- `data_expire_seconds` (int, optional): 数据过期时间(秒)，默认为600。
- `output_topic_prefix` (str, optional): 输出topic前缀，默认为"MODULE-OUTPUT-"。
- `result_handler` (callable, optional): 结果处理回调函数 `(station_id, result) -> None`，在结果上传Kafka后调用。默认为None。

**异常**:
- `KafkaConnectionError`: 当Kafka连接失败时抛出。

#### start(callback=None, background=True)

启动数据解析服务。

**参数**:
- `callback` (function, optional): 回调函数(station_id, module_input)，每个场站数据更新时调用。默认为None。
- `background` (bool, optional): 是否后台线程运行。默认为True。

**返回值**: None

#### stop()

停止数据解析服务。

**参数**: 无

**返回值**: None

#### get_outputs(station_id)

获取指定场站所有模块的输入结构。

**参数**:
- `station_id` (str): 场站ID。

**返回值**: dict，包含所有模块的输入数据。

#### reload_config()

动态热加载config.py配置（topic、窗口、依赖等），并同步dispatcher。

**参数**: 无

**返回值**: None

#### get_station_status()

获取所有场站线程健康状态。

**返回值**: dict {station_id: {'running': bool, 'future': Future}}

#### get_service_status()

获取服务主线程及Kafka连接健康状态。

**返回值**: dict {main_thread_alive, consumer_alive, producer_alive, station_count}

---

### AsyncDataAnalysisService 类

异步数据解析服务，支持多场站并发、窗口补全/插值、依赖聚合、自动上传Kafka。

#### 初始化

```python
AsyncDataAnalysisService(
    module_name=None,
    topics=None,
    kafka_config=None,
    data_expire_seconds=600,
    output_topic_prefix="MODULE-OUTPUT-",
    result_handler=None
)
```

**参数**:
- `module_name` (str, optional): 业务模块名称，用于标识当前处理模块。默认为None。
- `topics` (list, optional): 需要消费的topic列表，默认为配置文件中所有topic。
- `kafka_config` (dict, optional): Kafka连接配置，默认为config.KAFKA_CONFIG。
- `data_expire_seconds` (int, optional): 数据过期时间(秒)，默认为600。
- `output_topic_prefix` (str, optional): 输出topic前缀，默认为"MODULE-OUTPUT-"。
- `result_handler` (callable, optional): 结果处理回调函数 `async (station_id, result) -> None`，在结果上传Kafka后调用。可以是同步或异步函数。默认为None。

**异常**:
- `KafkaConnectionError`: 当Kafka连接失败时抛出。

#### start(callback=None)

启动异步数据解析服务。

**参数**:
- `callback` (function, optional): 回调函数(station_id, module_input)，每个场站数据更新时调用。默认为None。

**返回值**: None

#### stop()

停止异步数据解析服务。

**参数**: 无

**返回值**: None

#### get_all_inputs(station_id)

获取指定场站所有模块的输入结构。

**参数**:
- `station_id` (str): 场站ID。

**返回值**: dict，包含所有模块的输入数据，格式为 `{module_name: module_input}`。

**示例**:
```python
inputs = await service.get_all_inputs("station001")
# 返回: {"load_prediction": {...}, "operation_optimization": {...}, ...}
```

#### add_station(station_id, callback)

动态添加场站异步任务。

**参数**:
- `station_id` (str): 场站ID。
- `callback` (function): 回调函数。

**返回值**: None

#### remove_station(station_id)

动态移除场站异步任务。

**参数**:
- `station_id` (str): 场站ID。

**返回值**: None

#### get_station_status()

获取所有场站异步任务健康状态。

**返回值**: dict {station_id: {'running': bool, 'task': asyncio.Task}}

---

### DataDispatcher 类

数据分发与依赖处理核心。

#### 初始化

```python
DataDispatcher(data_expire_seconds=600)
```

**参数**:
- `data_expire_seconds` (int, optional): 数据过期时间(秒)，默认为600。

#### update_topic_data(station_id, topic, raw_data)

更新指定场站、topic的数据窗口。

**参数**:
- `station_id` (str): 场站ID。
- `topic` (str): topic名称。
- `raw_data` (dict): 原始数据。

**返回值**: None

#### get_topic_window(station_id, topic)

获取窗口数据（只返回data部分，按时间升序）。

**参数**:
- `station_id` (str): 场站ID。
- `topic` (str): topic名称。

**返回值**: list，窗口数据列表。

#### get_module_input(station_id, module)

整合该场站所有topic窗口数据，组装为模块输入，窗口不足自动补零或插值。

**参数**:
- `station_id` (str): 场站ID。
- `module` (str): 模块名称。

**返回值**: dict，模块输入数据。

#### get_all_inputs(station_id)

获取指定场站所有模块的输入结构（内部调用，通常不直接使用）。

**参数**:
- `station_id` (str): 场站ID。

**返回值**: dict，包含所有模块的输入数据，格式为 `{module_name: module_input}`。

**示例**:
```python
inputs = dispatcher.get_all_inputs("station001")
# 返回: {"load_prediction": {...}, "operation_optimization": {...}, ...}
```

#### clean_expired()

清理过期数据（自动定期调用，通常不需要手动调用）。

**参数**: 无

**返回值**: None

#### reload_config(config_mod)

热加载config模块，动态更新topic、窗口、依赖等。

**参数**:
- `config_mod` (module): 配置模块。

**返回值**: None

---

### Kafka 客户端类

以下客户端封装基于 kafka-python/aiokafka，对连接与发送/消费做了重试与序列化处理。

注意：当前实现期望顶层存在 `bootstrap_servers`（扁平格式）。如使用嵌套格式，也请在外层同时提供 `bootstrap_servers` 以保证兼容性。

支持的参数透传（白名单）：
- 消费者（同步 kafka-python）：group_id, auto_offset_reset, enable_auto_commit, max_poll_records, session_timeout_ms, request_timeout_ms, heartbeat_interval_ms, max_poll_interval_ms, security_protocol, sasl_mechanism, sasl_plain_username, sasl_plain_password, ssl_cafile, ssl_certfile, ssl_keyfile
- 生产者（同步 kafka-python）：acks, retries, compression_type, linger_ms, batch_size, max_in_flight_requests_per_connection, buffer_memory, security_protocol, sasl_mechanism, sasl_plain_username, sasl_plain_password, ssl_cafile, ssl_certfile, ssl_keyfile
- 异步（aiokafka）消费者/生产者：security_protocol, sasl_mechanism, sasl_plain_username, sasl_plain_password（常用安全参数）

#### KafkaConsumerClient

```python
KafkaConsumerClient(topics: list[str], config: dict, max_retries: int = 5, retry_interval: int = 5)
```

- 参数：
    - `topics`: 订阅的 topic 列表
    - `config`: Kafka 配置（需包含 `bootstrap_servers`；`group_id`/`auto_offset_reset`/`enable_auto_commit` 可从 `config['consumer']` 或顶层读取）
    - `max_retries`: 连接失败重试次数，默认 5
    - `retry_interval`: 重试间隔秒数，默认 5

- 方法：
    - `poll(timeout_ms: int = 1000) -> dict`：返回 {TopicPartition: [Message]}
    - `close() -> None`

- 异常：
    - 连接/消费异常将以 `KafkaConnectionError` 记录并重试；最终失败抛出异常

#### KafkaProducerClient

```python
KafkaProducerClient(config: dict, max_retries: int = 5, retry_interval: int = 5)
```

- 参数：
    - `config`: Kafka 配置（需包含 `bootstrap_servers`）
    - `max_retries`/`retry_interval`: 同上

- 方法：
    - `send(topic: str, value: dict) -> None`：内部 `flush()`；失败自动重试
    - `close() -> None`

- 异常：
    - 多次重试仍失败将抛出异常

#### AsyncKafkaConsumerClient

```python
AsyncKafkaConsumerClient(topics: list[str], config: dict, loop=None)
```

- 方法：
    - `await start() -> None`
    - `await getone() -> Message`
    - `await stop() -> None`

#### AsyncKafkaProducerClient

```python
AsyncKafkaProducerClient(config: dict, loop=None)
```

- 方法：
    - `await start() -> None`
    - `await send(topic: str, value: dict) -> None`（send_and_wait）
    - `await stop() -> None`

---

### 异常类

#### DataAnalysisError

基础异常类，所有模块异常的基类。

#### KafkaConnectionError

Kafka连接失败时抛出。

#### DispatcherError

数据分发相关异常。

## 回调函数规范

### 同步回调函数

```python
def my_callback(station_id, module_input):
    # module_input为本模型所需结构化输入（含窗口补全/插值、依赖聚合）
    result = my_model_predict(module_input)  # 处理数据
    return result  # 返回值会自动上传到Kafka
```

**参数**:
- `station_id` (str): 场站ID。
- `module_input` (dict): 该场站当前模块所需输入数据，包含所有依赖数据。

**返回值**: 
- 处理结果数据，将自动上传到Kafka。如果返回None，则使用module_input作为上传内容。

### 异步回调函数

```python
async def my_callback(station_id, module_input):
    # module_input为本模型所需结构化输入（含窗口补全/插值、依赖聚合）
    result = await my_model_predict(module_input)  # 异步处理数据
    return result  # 返回值会自动上传到Kafka
```

**参数**:
- `station_id` (str): 场站ID。
- `module_input` (dict): 该场站当前模块所需输入数据，包含所有依赖数据。

**返回值**: 
- 处理结果数据，将自动上传到Kafka。如果返回None，则使用module_input作为上传内容。

### 回调函数注意事项

1. 回调函数应尽量保持轻量，避免耗时操作
2. 如需耗时操作，建议使用异步回调或单独线程处理
3. 确保回调函数能够处理各种异常情况
4. 回调函数中的错误会被系统捕获并记录，不会影响整体服务运行

## 配置参数说明

### Kafka配置 (KAFKA_CONFIG)

#### 嵌套格式（推荐）

```python
KAFKA_CONFIG = {
    'consumer': {
        'bootstrap_servers': ['localhost:9092'],  # Kafka服务器地址列表
        'group_id': 'data_analysis_group',        # 消费者组ID
        'auto_offset_reset': 'latest',            # 消费策略：latest/earliest
        'enable_auto_commit': True,               # 是否自动提交offset
        'max_poll_records': 500,                  # 单次拉取最大记录数
        'session_timeout_ms': 30000,              # 会话超时时间(毫秒)
    },
    'producer': {
        'bootstrap_servers': ['localhost:9092'],  # Kafka服务器地址列表
        'acks': 'all',                            # 消息确认模式：all/1/0
        'retries': 3,                             # 发送失败重试次数
        'max_in_flight_requests_per_connection': 5,  # 单连接未确认请求数
        'compression_type': 'gzip',               # 压缩类型：gzip/snappy/lz4
    }
}
```

#### 扁平格式（向后兼容）

```python
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],  # Kafka服务器地址列表
    'group_id': 'data_analysis_group',        # 消费者组ID
    'auto_offset_reset': 'latest',            # 消费策略：latest/earliest
    'enable_auto_commit': True,               # 是否自动提交offset
}
```

**参数说明**:

**消费者配置 (consumer)**:
- `bootstrap_servers`: Kafka集群地址列表，格式为['host1:port1', 'host2:port2']
- `group_id`: 消费者组ID，用于标识消费者组
- `auto_offset_reset`: 消费策略，'latest'从最新数据开始，'earliest'从最早数据开始
- `enable_auto_commit`: 是否自动提交消费offset
- `max_poll_records`: 单次poll拉取的最大记录数，默认500
- `session_timeout_ms`: 会话超时时间，默认30000毫秒

**生产者配置 (producer)**:
- `bootstrap_servers`: Kafka集群地址列表
- `acks`: 消息确认模式，'all'等待所有副本确认（最安全），'1'等待leader确认，'0'不等待确认
- `retries`: 发送失败后的重试次数，默认3
- `max_in_flight_requests_per_connection`: 单连接未确认请求数，默认5
- `compression_type`: 消息压缩类型，可选'gzip'/'snappy'/'lz4'/'none'

**格式说明**:
- 优先使用嵌套格式，可以为消费者和生产者分别配置参数
- 扁平格式主要用于向后兼容，系统会自动适配
- 嵌套格式下，consumer和producer配置互不影响

### Topic配置 (TOPIC_DETAIL)

```python
TOPIC_DETAIL = {
    'SCHEDULE-STATION-PARAM': {
        'fields': ['stationId', 'stationTemp', 'lat', 'lng', ...],     # 字段列表（使用camelCase）
        'frequency': '新建站或配置更改时',                              # 推送频率
        'modules': ['load_prediction', 'operation_optimization', ...],  # 需求模块
        'window_size': 1                                                # 窗口大小
    },
    'REAL-STATION-DATA': {
        'fields': ['stationId', 'timestamp', 'totalPower', ...],        # 高频数据字段
        'frequency': '15秒',                                            # 推送频率
        'modules': ['load_prediction', 'evaluation_model', ...],        # 需求模块
        'window_size': 7 * 24 * 60 * 4                                  # 7天数据（15秒一次）
    },
    # 其他topic配置...
}
```

**参数说明**:
- `fields`: 该topic包含的字段列表（注意：实际数据使用 camelCase 命名，如 `stationId` 而非 `station_id`）
- `frequency`: 数据推送频率描述
- `modules`: 需要消费此数据的业务模块列表
- `window_size`: 数据窗口大小，单位为数据点数量。计算示例：
  - 高频数据(15秒)的7天窗口：`7 * 24 * 60 * 4 = 40320` 个数据点
  - 低频配置数据：`1` 个数据点即可

### 模块依赖配置 (MODULE_DEPENDENCIES)

```python
MODULE_DEPENDENCIES = {
    'electricity_price': ['pv_prediction', 'evaluation_model', 'SOH_model'],
    'station_guidance': ['load_prediction', 'evaluation_model'],
    'thermal_management': ['load_prediction', 'operation_optimization'],
    'operation_optimization': ['load_prediction'],
}
```

**参数说明**:
- 键为模块名称，值为该模块依赖的其他模块列表
- 依赖模块的数据会自动包含在模块输入中

### 服务配置参数

1. **数据过期时间 (data_expire_seconds)**:
   - 类型: int
   - 默认值: 600 (10分钟)
   - 说明: 数据在内存中保存的时间，超时后自动清理

2. **输出Topic前缀 (output_topic_prefix)**:
   - 类型: str
   - 默认值: "MODULE-OUTPUT-"
   - 说明: 模块输出结果的topic前缀，完整topic为"前缀+模块名大写"

3. **窗口补全策略**:
   - **注意**：当前版本（v1.1）未实现窗口补全策略功能
   - 窗口不足时自动填充 None
   - 未来版本可能支持：'zero'（补零）、'linear'（线性插值）、'forward'（前向填充）等策略

### 性能相关配置

1. **线程池大小 (max_workers)**:
   - 类型: int
   - 默认值: 32
   - 说明: 同步服务中线程池的最大线程数，应根据实际场站规模调整

2. **Kafka批处理大小**:
   - 配置在Kafka客户端中
   - 建议根据数据量调整，平衡吞吐量和延迟
