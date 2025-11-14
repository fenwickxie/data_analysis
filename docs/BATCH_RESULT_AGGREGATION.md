# 批次结果聚合功能说明

## 问题背景

### 原有问题
1. **单条 Kafka 消息包含多个场站数据**：`SCHEDULE-STATION-REALTIME-DATA` 消息中包含同一批所有场站的实时数据（可能有缺失）
2. **各场站独立处理**：每个场站有自己的 worker 线程，独立处理数据
3. **数据完整性不一致**：某些场站可能因为数据不全而无法输出结果，导致同一批数据的场站结果不统一
4. **上传分散**：原有架构中每个场站单独调用 `result_handler`，导致同一批数据的结果分散上传到 Kafka

### 业务需求
- **同一批数据的所有场站结果应该合并为一个列表，一次性上传到 Kafka**
- 每个场站输出结果是一个字典，最终组成一个列表上传

---

## 解决方案

### 核心思路
实现**批次级别的结果聚合机制**，而不是场站级别的独立处理：

1. **使用时间戳作为批次ID**：从 `SCHEDULE-STATION-REALTIME-DATA` 的最新 `sendTime` 提取时间戳
2. **跟踪批次的场站完成情况**：记录每个批次预期的场站集合和实际完成的场站结果
3. **等待批次完成**：当所有预期场站都处理完成后，统一触发 `result_handler`
4. **超时兜底**：设置批次超时时间（默认5秒），超时后强制提交已完成的结果

### 架构变更

#### 1. AsyncDataAnalysisService 新增字段
```python
# 批次结果聚合相关
self._batch_results = {}  # {batch_id: {'expected_stations': set(), 'results': {station_id: result}, 'completed': bool}}
self._batch_lock = asyncio.Lock()  # 保护批次数据的并发访问
self._batch_timeout = 5.0  # 批次超时时间（秒）
```

#### 2. 核心方法

##### `_init_batch(batch_id, expected_stations)`
初始化批次信息，记录预期的场站集合。

##### `_record_station_result(batch_id, station_id, result)`
记录场站处理结果，检查批次是否完成：
- 记录场站结果到批次中
- 检查是否所有预期场站都已完成
- 如果完成，触发 `_handle_batch_results`

##### `_handle_batch_results(batch_id)`
处理批次结果：
- 将所有场站结果合并为列表
- 调用 `result_handler(batch_id, batch_results_list)`
- 清理批次信息

##### `_cleanup_expired_batches()`
清理超时的批次：
- 在主循环中定期调用
- 强制提交超时批次的已完成结果

##### `_extract_batch_id(station_id, module_input)`
从 `module_input` 中提取批次ID：
- 从 `SCHEDULE-STATION-REALTIME-DATA` 的 `sendTime` 提取最新时间
- 转换为时间戳，构造批次ID（如 `SCHEDULE-STATION-REALTIME-DATA_1699999200`）

#### 3. 处理流程变更

**原流程**：
```
Kafka消息 → 解析场站数据 → 各场站worker独立处理 → 单独调用result_handler → 分散上传
```

**新流程**：
```
Kafka消息 → 解析场站数据 → 初始化批次信息（记录预期场站）
    ↓
各场站worker独立处理 → 提取批次ID → 记录结果到批次
    ↓
批次完成检查 → 所有场站完成 → 合并结果列表 → 统一调用result_handler → 批次上传
    ↓
（或）超时兜底 → 强制提交已完成结果
```

---

## API 变更

### callback 函数（保持不变）

```python
async def my_callback(station_id, module_input):
    """
    业务回调函数（单场站级别）
    
    Args:
        station_id: 场站ID
        module_input: 模块输入数据（已解析和对齐）
        
    Returns:
        dict: 单个场站的模型输出（通常是dict）
              例如：{"result": 123, "prediction": [1,2,3], "confidence": 0.95}
              或 None（数据不全时）
    """
    # 模型推理
    result = model_predict(module_input)
    return result  # 返回单个场站的结果
```

**注意**：
- `my_callback` 的签名和返回值**没有变化**
- 返回的是**单个场站的模型输出**（通常是dict）
- 框架会自动添加 `station_id` 字段到结果中

### result_handler 签名变更

**旧签名**：
```python
async def result_handler(station_id, module_input, result):
    """单场站级别的结果处理"""
    payload = {
        "station_id": station_id,
        "module": MODULE_NAME,
        "result": result,
    }
    await producer.send(output_topic, payload)
```

**新签名**：
```python
async def result_handler(batch_id, batch_results_list):
    """
    批次级别的结果处理器
    
    Args:
        batch_id: 批次ID（如 "SCHEDULE-STATION-REALTIME-DATA_1699999200"）
        batch_results_list: 批次结果列表，每个元素为场站的结果字典
            [
                {'station_id': 'A', 'result': 123, 'prediction': [1,2,3], ...},
                {'station_id': 'B', 'result': 456, 'prediction': [4,5,6], ...},
                ...
            ]
            
    说明：
        - 每个元素是单个场站的模型输出（my_callback 的返回值）
        - 框架自动添加了 'station_id' 字段
        - 只包含有效结果（result != None 的场站）
    """
    payload = {
        "batch_id": batch_id,
        "module": MODULE_NAME,
        "timestamp": time.time(),
        "stations": batch_results_list,  # 所有场站结果的列表
        "station_count": len(batch_results_list),
    }
    await producer.send(output_topic, payload)
```

**关键变化**：
1. 参数从 `(station_id, module_input, result)` 改为 `(batch_id, batch_results_list)`
2. `batch_results_list` 中每个元素是单个场站的完整输出（包含 `station_id`）
3. 批次级别调用，一次性上传所有场站结果

---

## 关键特性

### 1. 批次识别
- **批次ID构造**：`topic_timestamp`（如 `SCHEDULE-STATION-REALTIME-DATA_1699999200`）
- **时间戳来源**：从 `SCHEDULE-STATION-REALTIME-DATA` 的最新 `sendTime` 提取
- **降级处理**：如果无法识别批次，直接调用 `result_handler`（兼容旧逻辑）

### 2. 批次完成检查
- **预期场站**：在 `_process_message` 中记录本批消息涉及的所有场站
- **实际完成**：在 `_station_worker` 中记录每个场站的处理结果
- **完成条件**：`completed_stations >= expected_stations`

### 3. 超时机制
- **默认超时**：5秒
- **超时处理**：强制提交已完成的场站结果（部分结果）
- **日志记录**：记录超时批次和缺失场站

### 4. 线程安全
- 使用 `asyncio.Lock` 保护批次数据的并发访问
- 确保多个 worker 同时记录结果时不会出现竞态条件

---

## 数据流示例

### 场景：单条消息包含3个场站

1. **Kafka消息到达**（时间戳: 1699999200）
   ```json
   {
     "SCHEDULE-STATION-REALTIME-DATA": [
       {"stationId": "A", "sendTime": "2025-11-14 10:00:00", ...},
       {"stationId": "B", "sendTime": "2025-11-14 10:00:00", ...},
       {"stationId": "C", "sendTime": "2025-11-14 10:00:00", ...}
     ]
   }
   ```

2. **初始化批次**
   ```python
   batch_id = "SCHEDULE-STATION-REALTIME-DATA_1699999200"
   expected_stations = {"A", "B", "C"}
   ```

3. **各场站处理**（并发）
   ```python
   # 场站A: 调用 my_callback("A", module_input_A)
   result_A = {"result": 123, "prediction": [1,2,3], "confidence": 0.95}
   
   # 场站B: 调用 my_callback("B", module_input_B)
   result_B = None  # 数据不全，返回 None
   
   # 场站C: 调用 my_callback("C", module_input_C)
   result_C = {"result": 789, "prediction": [7,8,9], "confidence": 0.88}
   ```

4. **记录结果到批次**
   ```python
   # 框架自动记录每个场站的 result
   batch_results[batch_id]['results'] = {
       "A": {"result": 123, "prediction": [1,2,3], "confidence": 0.95},
       "B": None,  # 会被过滤掉
       "C": {"result": 789, "prediction": [7,8,9], "confidence": 0.88}
   }
   ```

5. **批次完成检查**
   ```python
   completed_stations = {"A", "B", "C"}  # 3/3 完成
   # 触发 _handle_batch_results
   ```

6. **合并结果并添加 station_id**
   ```python
   # 框架自动添加 station_id 到每个结果中
   batch_results_list = [
       {"station_id": "A", "result": 123, "prediction": [1,2,3], "confidence": 0.95},
       # B 的结果为 None，不包含在列表中
       {"station_id": "C", "result": 789, "prediction": [7,8,9], "confidence": 0.88}
   ]
   
   # 调用 result_handler
   result_handler("SCHEDULE-STATION-REALTIME-DATA_1699999200", batch_results_list)
   ```

7. **Kafka上传**
   ```json
   {
     "batch_id": "SCHEDULE-STATION-REALTIME-DATA_1699999200",
     "module": "load_prediction",
     "timestamp": 1699999200.123,
     "stations": [
       {"station_id": "A", "result": 123, "prediction": [1,2,3], "confidence": 0.95},
       {"station_id": "C", "result": 789, "prediction": [7,8,9], "confidence": 0.88}
     ],
     "station_count": 2
   }
   ```

---

## 配置参数

### 批次超时时间
```python
service = AsyncDataAnalysisService(
    module_name='load_prediction',
    # ... 其他参数
)
service._batch_timeout = 10.0  # 自定义超时时间（秒）
```

---

## 兼容性

### 降级处理
如果无法从 `module_input` 中提取批次ID（如非 `SCHEDULE-STATION-REALTIME-DATA` topic），将降级到旧逻辑：
- 直接调用 `result_handler(station_id, module_input, result)`
- 保持向后兼容

### 迁移指南
1. **更新 result_handler 签名**：从单场站改为批次级别
2. **调整 payload 结构**：从单个场站结果改为场站列表
3. **测试批次聚合**：验证多场站结果能否正确合并

---

## 日志示例

```
INFO: 初始化批次 SCHEDULE-STATION-REALTIME-DATA_1699999200，预期场站: {'A', 'B', 'C'}
INFO: 批次 SCHEDULE-STATION-REALTIME-DATA_1699999200 进度: 1/3，待完成: {'B', 'C'}
INFO: 批次 SCHEDULE-STATION-REALTIME-DATA_1699999200 进度: 2/3，待完成: {'C'}
INFO: 批次 SCHEDULE-STATION-REALTIME-DATA_1699999200 完成，场站: {'A', 'B', 'C'}
INFO: 批次 SCHEDULE-STATION-REALTIME-DATA_1699999200 结果已上传，包含 2 个场站
```

---

## 注意事项

1. **批次ID唯一性**：使用 `topic_timestamp` 作为批次ID，确保不同批次不会冲突
2. **超时时间设置**：根据实际处理时间调整 `_batch_timeout`，避免过早或过晚触发超时
3. **内存管理**：定期清理超时批次，避免 `_batch_results` 无限增长
4. **结果过滤**：只上传非 `None` 的场站结果，避免上传无效数据
5. **并发安全**：使用 `asyncio.Lock` 确保批次数据的线程安全

---

## 未来优化

1. **批次信息持久化**：将批次信息持久化到 Redis，支持服务重启后恢复
2. **动态超时时间**：根据批次大小和历史处理时间动态调整超时时间
3. **批次监控**：暴露批次处理进度和超时统计的 API
4. **部分结果策略**：支持配置部分结果的上传策略（如：50%完成即上传）
