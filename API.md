# API 文档

本文档详细说明data_analysis模块的公共API、回调函数规范和配置参数。

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
    output_topic_prefix="MODULE-OUTPUT-"
)
```

**参数**:
- `module_name` (str, optional): 业务模块名称，用于标识当前处理模块。默认为None。
- `topics` (list, optional): 需要消费的topic列表，默认为配置文件中所有topic。
- `kafka_config` (dict, optional): Kafka连接配置，默认为config.KAFKA_CONFIG。
- `data_expire_seconds` (int, optional): 数据过期时间(秒)，默认为600。
- `output_topic_prefix` (str, optional): 输出topic前缀，默认为"MODULE-OUTPUT-"。

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
    output_topic_prefix="MODULE-OUTPUT-"
)
```

**参数**:
- `module_name` (str, optional): 业务模块名称，用于标识当前处理模块。默认为None。
- `topics` (list, optional): 需要消费的topic列表，默认为配置文件中所有topic。
- `kafka_config` (dict, optional): Kafka连接配置，默认为config.KAFKA_CONFIG。
- `data_expire_seconds` (int, optional): 数据过期时间(秒)，默认为600。
- `output_topic_prefix` (str, optional): 输出topic前缀，默认为"MODULE-OUTPUT-"。

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

#### get_outputs(station_id)

获取指定场站所有模块的输入结构。

**参数**:
- `station_id` (str): 场站ID。

**返回值**: dict，包含所有模块的输入数据。

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

#### get_all_outputs(station_id)

获取指定场站所有模块的输入结构。

**参数**:
- `station_id` (str): 场站ID。

**返回值**: dict，包含所有模块的输入数据。

#### clean_expired()

清理过期数据。

**参数**: 无

**返回值**: None

#### set_padding_strategy(strategy)

设置窗口补全策略。

**参数**:
- `strategy` (str): 补全策略，'zero'（补零）、'linear'（线性插值）、'forward'（前向填充）、'missing'（缺失标记None）。

**返回值**: None

#### reload_config(config_mod)

热加载config模块，动态更新topic、窗口、依赖等。

**参数**:
- `config_mod` (module): 配置模块。

**返回值**: None

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

```python
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],  # Kafka服务器地址列表
    'group_id': 'data_analysis_group',        # 消费者组ID
    'auto_offset_reset': 'latest',            # 消费策略：latest/earliest
    'enable_auto_commit': True,               # 是否自动提交offset
}
```

**参数说明**:
- `bootstrap_servers`: Kafka集群地址列表，格式为['host1:port1', 'host2:port2']
- `group_id`: 消费者组ID，用于标识消费者组
- `auto_offset_reset`: 消费策略，'latest'从最新数据开始，'earliest'从最早数据开始
- `enable_auto_commit`: 是否自动提交消费offset

### Topic配置 (TOPIC_DETAIL)

```python
TOPIC_DETAIL = {
    'SCHEDULE-STATION-PARAM': {
        'fields': ['station_id', 'station_temp', 'lat', 'lng', ...],  # 字段列表
        'frequency': '新建站或配置更改时',                             # 推送频率
        'modules': ['load_prediction', 'operation_optimization', ...], # 需求模块
        'window_size': 1                                             # 窗口大小
    },
    # 其他topic配置...
}
```

**参数说明**:
- `fields`: 该topic包含的字段列表
- `frequency`: 数据推送频率描述
- `modules`: 需要消费此数据的业务模块列表
- `window_size`: 数据窗口大小，单位为数据点数量

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

3. **窗口补全策略 (padding_strategy)**:
   - 类型: str
   - 可选值: 'zero', 'linear', 'forward', 'missing'
   - 默认值: 'zero'
   - 说明: 当数据窗口不足时的补全策略

### 性能相关配置

1. **线程池大小 (max_workers)**:
   - 类型: int
   - 默认值: 32
   - 说明: 同步服务中线程池的最大线程数，应根据实际场站规模调整

2. **Kafka批处理大小**:
   - 配置在Kafka客户端中
   - 建议根据数据量调整，平衡吞吐量和延迟
