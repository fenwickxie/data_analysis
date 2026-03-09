# API 文档

> **文档版本**：v2.0  
> **更新日期**：2026-03-09  
> **对应代码版本**：data_analysis v2.0.0 (branch: develop)

本文档详细说明data_analysis模块的公共API、回调函数规范和配置参数。

**修订说明**：
- v2.0 (2026-03-09): 全面对齐 v2.0.0 代码实现
    - `DataAnalysisService.__init__` 移除 `output_topic_prefix` 参数
    - `AsyncDataAnalysisService.__init__` 移除 `output_topic_prefix`，新增 `offset_commit_config`
    - `DataDispatcher.__init__` 新增 `enable_data_expiration` 参数
    - `DataDispatcher.update_topic_data` 返回类型由 `None` 改为 `bool`，新增 `timestamp` 参数
    - 配置相关描述全部更新为 YAML 格式（`config.yaml`）
    - `__version__` 修正为 2.0.0；新增 `ServiceBase` 说明
- v1.2 (2025-11-23): 融合同步/异步批次上传、数据可用性、全局数据缓存与多消费者诊断文档
- v1.1 (2025-11-07): 更新以匹配实际代码实现

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
    result_handler=None
)
```

**参数**:
- `module_name` (str, optional): 业务模块名称，用于标识当前处理模块。默认为None（自动从config.yaml的`module_name`字段读取）。
- `topics` (list, optional): 需要消费的topic列表，默认由`module_name`和配置自动推导。
- `kafka_config` (dict, optional): Kafka连接配置，默认为config.yaml中的`kafka`节。
- `data_expire_seconds` (int, optional): 数据过期时间(秒)，默认为600。
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
    result_handler=None,
    offset_commit_config=None
)
```

**参数**:
- `module_name` (str, optional): 业务模块名称，默认从 config.yaml 的 `module_name` 字段读取。
- `topics` (list, optional): 需要消费的 topic 列表，默认由 `module_name` 和配置自动推导。
- `kafka_config` (dict, optional): Kafka连接配置，默认为config.yaml中的`kafka`节。
- `data_expire_seconds` (int, optional): 数据过期时间(秒)，默认为600。
- `result_handler` (callable, optional): 结果处理回调 `async (station_id, result) -> None`，可为同步或异步函数。默认为None。
- `offset_commit_config` (dict, optional): 手动Offset提交配置，默认为config.yaml中的`offset_commit`节。

**异常**:
- `KafkaConnectionError`: 当Kafka连接失败时抛出。

#### start(callback=None, result_handler=None, batch_upload_handler=None)

启动异步数据解析服务。

**参数**:
- `callback` (function, optional): 回调函数(station_id, module_input)，每个场站数据更新时调用。默认为None。
- `result_handler` (function, optional): 单场站结果处理函数，签名 `(station_id, module_input, result)`。
- `batch_upload_handler` (function, optional): 批次结果上传回调，签名 `(batch_id, results_list)`，用于启用批次聚合上传。

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

### BatchResultAggregator 与批次上传

异步服务内置 `BatchResultAggregator`，用于将同一批消息里多个场站的模型输出聚合成一次上传：

- **启用方式**：调用 `AsyncDataAnalysisService.start(..., batch_upload_handler=my_handler)`。当 `_batch_upload_handler` 存在时，服务为每次触发生成 `batch_id`，格式为 `f"batch_{int(time.time()*1000)}_{id(batch)}"` （时间戳毫秒 + 对象地址，保证唯一性），并记录本批所有场站 ID。
- **Collector 生命周期**：`BatchResultAggregator` 会为每个 `batch_id` 创建 `BatchCollector`，跟踪 `expected_stations`、到齐进度以及 `batch_timeout`（默认 5 秒）。全部结果到齐或超时都会触发 `_batch_upload_handler(batch_id, results_list)`。
- **回调契约**：`results_list` 仅包含返回非 `None` 的场站结果，且系统会自动注入 `station_id` 字段；若某场站回调返回 `None` 或报错，它不会出现在列表中。`batch_upload_handler` 可为同步或异步函数。
- **降级策略**：无法解析批次（例如其他 topic 或缺少 `sendTime`）时自动回退到旧逻辑，仅调用 `result_handler`；未提供 `_batch_upload_handler` 时行为与 v1.1 相同。
- **扩展参数**：可在运行中调整 `service.batch_aggregator.batch_timeout` 或 `cleanup_interval`，以及通过 `BatchResultAggregator.cleanup_expired()` 主动清理长时间未完成批次。

### DataDispatcher 类

数据分发与依赖处理核心。

#### 初始化

```python
DataDispatcher(data_expire_seconds=600, enable_data_expiration=True)
```

**参数**:
- `data_expire_seconds` (int, optional): 数据过期时间(秒)，默认为600。

#### update_topic_data(station_id, topic, raw_data, timestamp=None)

更新指定场站、topic的数据窗口。

**参数**:
- `station_id` (str): 场站ID。
- `topic` (str): topic名称。
- `raw_data` (dict): 原始数据。
- `timestamp` (float, optional): 消息时间戳（秒，来自Kafka消息的 `timestamp/1000`），`None` 时自动取 `time.time()`。

**返回值**: `bool` — `True` 表示应触发数据就绪事件；`False` 表示同一秒内订单类 topic 正在聚合，暂不触发

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

## 数据可用性元信息

`dispatcher.get_all_inputs()` 在每个模块输入中注入 `_data_quality` 字段，帮助业务侧评估依赖 topic 的可用情况：

```python
{
        'stationId': 'STATION_001',
        'gunNo': [...],
        # ... 模块输入字段 ...
        '_data_quality': {
                'available_topics': ['SCHEDULE-CAR-ORDER', 'SCHEDULE-DEVICE-GUN', ...],
                'missing_topics': ['SCHEDULE-DEVICE-ERROR'],
                'total_topics': 8,
                'availability_ratio': 0.875
        }
}
```

- **可用性判定**：只要窗口中存在数据即视为可用，不对“数据新鲜度”做额外约束，适用于事件驱动+缓存复用模式。
- **业务示例**：
    - 关键 topic 缺失时直接跳过处理或使用降级算法。
    - 根据 `availability_ratio` 选择全量/降级/默认策略，例如 ≥0.9 走完整推理，0.6~0.9 走简化逻辑，低于 0.6 直接返回默认值。
    - 在 `result_handler` 中上报监控指标，如 `missing_topics` 数量、可用率直方图。
- **调优**：确保 `data_expire_seconds` 与慢速 topic 周期匹配（如价格月更则配置 35 天以上），以免缓存过期导致误判为缺失。

## 全局数据缓存与场站初始化顺序

- **缓存策略**：所有 `stationId` 为空、判定为全局的数据（当前为 `SCHEDULE-ENVIRONMENT-CALENDAR`）会被写入 `_global_data_cache`，无论此时是否已有场站注册；缓存只保留最新一份，内存占用可忽略。
- **广播流程**：
    1. 若已有场站，收到全局数据后立即为所有活跃场站调用 `dispatcher.update_topic_data` 并触发各自的数据事件。
    2. 新场站注册时，会在创建 worker 之前应用缓存中的所有全局数据，避免 worker 在首轮运行时读到不完整输入（修复 race condition）。
- **排障提示**：如果业务回调未读到全局字段，优先检查 `_global_data_cache` 是否为空、topic 是否被正确识别为全局、以及站点是否在全局数据之后才首次收到站点数据。

## 配置参数说明

### Kafka配置 (kafka in config.yaml)

> **v2.0变更**：所有配置通过 `config.yaml` 管理，不再直接编辑 `config.py`。

#### 嵌套格式（config.yaml）

```yaml
kafka:
  bootstrap_servers:
    - localhost:9092
  consumer:
    group_id: data_analysis_group
    auto_offset_reset: latest     # latest / earliest
    enable_auto_commit: false     # 推荐 false，由 OffsetManager 手动提交
    max_poll_records: 500
    session_timeout_ms: 30000
    multi_consumer_mode: false    # true = 每个topic独立consumer
  producer:
    acks: all
    retries: 3
    compression_type: gzip
```

> 也可通过代码参数 `kafka_config=` 传入同结构的 Python 字典（同时支持嵌套和扁平两种格式），优先级高于 YAML。

**参数说明**:

**消费者（consumer）**:
- `bootstrap_servers`: Kafka集群地址列表（也可放在顶层共享）
- `group_id`: 消费者组ID
- `auto_offset_reset`: 消费策略，`latest` / `earliest`
- `enable_auto_commit`: 推荐 `false`，由 `OffsetManager` 负责手动提交
- `max_poll_records`: 单次 poll 拉取最大记录数
- `multi_consumer_mode`: `true` 时每个 topic 创建独立 consumer，缓解热点 topic 占满配额

**生产者（producer）**:
- `acks`: 消息确认模式，`all`/`1`/`0`
- `retries`: 发送失败重试次数
- `compression_type`: 压缩方式 `gzip`/`snappy`/`lz4`/`none`

#### 多消费者模式 (multi_consumer_mode)

- **功能**：为每个 topic 创建独立 `AIOKafkaConsumer`，每个消费者拥有独立的 `max_poll_records` 配额，避免高频 topic 吞噬全部拉取额度；只有一个 topic 时自动降级成单消费者模式。
- **监控**：开启 `DEBUG` 日志可看到 `多消费者拉取统计 [topic:count,...]`；运行时可调用 `service.consumer.get_lag_info()` 查看每个 topic 的滞后情况。
- **空拉取诊断**：若某 topic 长期显示 0 条，优先确认是否确有新消息；必要时：
    1. 调整 `timeout_ms`（例如 3000-5000ms）与 `fetch_max_wait_ms`（若配置成 500ms 会导致 broker 过快返回空批次）。
    2. 检查 `fetch_min_bytes`，对低频 topic 可提升到 1024 以减少空返回。
    3. 通过 `kafka-consumer-groups.sh --describe` 或 `GetOffsetShell` 对比 LAG，确认是否真的积压。
- **资源预算**：topic 数量 = 消费者数量，需确保 broker 连接数和容器内存足以支撑；如资源紧张，可将 `multi_consumer_mode` 置 `False` 暂时回退单消费者模式。

#### group_id 与 offset 迁移策略

- 从单消费者切到多消费者或重启前存在大量积压时，旧 `group_id` 里保存的 offset 可能已经被 Kafka 清理，导致 `Fetch offset ... is out of range`。最佳实践是在切换模式或重启前 **修改 group_id**（如 `stack-charge-tcp-command-xfy-v2` 或带日期后缀）。
- 如果使用了每-topic 独立 group 的自定义补丁，可运行 `verify_group_id_fix.py` 确认生成的 group_id 是否符合 `{base_group_id}-{topic}` 格式。
- 遇到 offset 越界后，可依次操作：
    1. 调整 `auto_offset_reset` 为 `latest`（或在必要时改成 `earliest`）、重启服务；
    2. 修改 group_id 让 Kafka 视为全新消费者组；
    3. 如仍失败，使用 `kafka-consumer-groups --reset-offsets` 清理旧 offset。

#### OFFSET_COMMIT_CONFIG 与 OffsetManager

```python
OFFSET_COMMIT_CONFIG = {
        'commit_interval_seconds': 5.0,  # 定时提交间隔（秒）
        'commit_batch_size': 100,        # 累积消息数阈值
        'max_commit_retries': 3,
        'commit_retry_delay': 1.0,
}
```

- 异步服务通过 `OffsetManager` 管理提交逻辑：既支持“消息数量”触发，也支持“时间间隔”触发；提交失败会按配置重试，并保留 `_pending_offsets` 以便后续继续尝试。
- 如需低延迟确认，可将 `commit_interval_seconds` 降至 2-3 秒；高吞吐场景则可提升 `commit_batch_size` 至 500+，减少提交频率。
- 调试 offset 提交问题时，可将日志级别设为 `INFO`，观察“成功提交 X 个分区 offset”的日志；若出现多次失败，请先检查 Kafka ACL 及 group_id 是否有效。

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
- `frequency`: 每小时推送次数（整数）
- `modules`: 需要消费此数据的业务模块列表
- `window_size`: 数据窗口大小，单位为数据点数量

### 模块依赖配置 (MODULE_DEPENDENCIES)

```python
### 模块依赖配置 (module_dependencies in config.yaml)

```yaml
module_dependencies:
  electricity_price:
    - pv_prediction
    - evaluation_model
    - SOH_model
  station_guidance:
    - load_prediction
    - evaluation_model
  thermal_management:
    - load_prediction
    - operation_optimization
  operation_optimization:
    - load_prediction
```

**参数说明**:
- 键为模块名称，值为该模块依赖的其他模块列表
- 依赖模块的数据会自动包含在模块输入中

### 服务配置参数

1. **数据过期时间 (data_expire_seconds)**:
   - 类型: int
   - 默认值: 600 (10分钟)
   - 说明: 数据在内存中保存的时间，超时后自动清理

2. **上传限速 (upload_interval_seconds)**:
   - 配置于 `config.yaml` 的 `upload_interval_seconds` 字段
   - 类型: float，默认值: 0（不限速）
   - 说明: 连续两次模型触发之间的最短间隔；设为正数可避免数据洪峰时对算法服务的冲击

3. **窗口补全策略**:
   - 子类可重写 `ParserBase.parse_window()` 实现自定义插值逻辑
   - `DataDispatcher.set_padding_strategy()` 接口保留，当前由各 parser 自行实现补全

### 性能相关配置

1. **线程池大小 (max_workers)**:
   - 类型: int
   - 默认值: 32
   - 说明: 同步服务中线程池的最大线程数，应根据实际场站规模调整

2. **Kafka批处理大小**:
   - 配置在 `config.yaml` 的 `kafka.consumer.max_poll_records`
   - 建议根据数据量调整，平衡吞吐量和延迟
