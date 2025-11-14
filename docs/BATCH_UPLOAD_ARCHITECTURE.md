# 批次结果聚合架构说明

## 问题背景

### 原有问题

1. **数据不完整**：Kafka单条消息中某些场站数据可能缺失
2. **结果分散**：各场站独立输出，无法批量上传
3. **阻塞风险**：某个场站失败会阻塞整体流程
4. **超时困境**：部分场站处理慢，导致永久等待

### 业务需求

1. 同一批数据的各场站输出应该聚合到一个列表中上传
2. 模型输出是单场站的（每个场站独立处理）
3. 对于SCHEDULE-STATION-REALTIME-DATA等topic，单条消息就是同一批所有场站的实时数据
4. 允许部分场站失败，不阻塞整体上传

## 解决方案

### 核心架构

```
┌─────────────────────────────────────────────────────────────┐
│           consumer.getmany(timeout_ms=1000)                  │
│           一次获取的所有消息 = 一个批次                        │
│  [                                                           │
│    消息1: {"stationData": [{"stationId": "S001"}, ...]},    │
│    消息2: {"stationData": [{"stationId": "S002"}, ...]},    │
│    消息3: {"stationData": [{"stationId": "S003"}, ...]},    │
│  ]                                                           │
└─────────────────────────────────────────────────────────────┘
                          ↓
           ┌──────────────┴──────────────┐
           │  _main_loop()               │
           │  - 生成批次ID                │
           │  - 收集所有场站ID            │
           │  - 创建批次                  │
           │  - 分发给各场站Worker        │
           └──────────────┬──────────────┘
                          ↓
        ┌─────────────────┼─────────────────┐
        ↓                 ↓                 ↓
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│ Station       │  │ Station       │  │ Station       │
│ Worker A      │  │ Worker B      │  │ Worker C      │
│               │  │               │  │               │
│ ✓ 处理成功     │  │ ✗ 处理失败     │  │ ✓ 处理成功     │
│ result_A      │  │ None          │  │ result_C      │
└───────┬───────┘  └───────┬───────┘  └───────┬───────┘
        │                  │                  │
        └──────────────────┼──────────────────┘
                          ↓
           ┌──────────────────────────┐
           │  BatchCollector          │
           │  - 收集各场站结果         │
           │  - 追踪完成度             │
           │  - 超时监控               │
           └──────────────┬───────────┘
                          ↓
                    等待条件：
                    1. 所有场站完成
                    或
                    2. 超时 (5秒)
                          ↓
           ┌──────────────────────────┐
           │  批次上传                 │
           │  results_list:           │
           │  [result_A, result_C]    │
           │  (过滤掉None)             │
           └──────────────────────────┘
```

### 关键组件

#### 1. BatchResultAggregator（批次聚合器）

**职责**：
- 管理所有批次的生命周期
- 创建和销毁BatchCollector
- 监控批次超时
- 清理过期批次

**核心方法**：
```python
async def create_batch(batch_id, expected_stations, upload_callback)
async def get_or_create_batch(...)
async def cleanup_expired(max_age)
```

#### 2. BatchCollector（批次收集器）

**职责**：
- 收集单个批次的所有场站结果
- 追踪完成进度
- 触发批次上传

**状态追踪**：
```python
self.expected_stations = {"S001", "S002", "S003"}  # 期望场站
self._results = {
    "S001": result_A,  # 成功
    "S002": None,      # 失败
    "S003": result_C   # 成功
}
self._uploaded = False  # 是否已上传
```

**完成条件**：
- 所有期望场站都提交了结果（包括None）
- 或超时（5秒）

#### 3. 场站Worker改造

**原有流程**：
```python
result = callback(station_id, module_input)
result_handler(station_id, module_input, result)  # 单场站处理
```

**新增流程**：
```python
result = callback(station_id, module_input)

# 提交到批次聚合器
if batch_id:
    batch_collector.add_result(station_id, result)

# 保持原有逻辑（可选）
result_handler(station_id, module_input, result)
```

### 数据流

#### 1. 批次创建

```python
# _main_loop() 中
batch = await consumer.getmany(timeout_ms=1000)  # 一次获取多条消息

# 生成批次ID（基于时间戳和批次对象ID）
batch_id = f"batch_{int(time.time() * 1000)}_{id(batch)}"

# 收集所有消息中的场站ID
all_station_ids = []
for msg in batch:
    station_ids = extract_stations_from_message(msg)
    all_station_ids.extend(station_ids)

# 去重并创建批次
unique_stations = list(set(all_station_ids))
batch_collector = await batch_aggregator.create_batch(
    batch_id=batch_id,
    expected_stations=unique_stations,
    upload_callback=batch_upload_handler
)
```

#### 2. 结果收集

```python
# _station_worker() 中
result = await callback(station_id, module_input)  # 可能是dict或None

await batch_collector.add_result(station_id, result)
```

#### 3. 批次上传

```python
# 自动触发（全部完成或超时）
results_list = [r for r in results.values() if r is not None]

await upload_callback(batch_id, results_list)
# results_list = [result_A, result_C]  # 只包含成功的
```

## 容错机制

### 1. 场站级容错

**问题**：某个场站处理失败
**解决**：返回None，继续收集其他场站

```python
try:
    result = process_data(...)
    return result
except Exception as e:
    logging.error(f"场站 {station_id} 失败: {e}")
    return None  # 不阻塞批次
```

### 2. 批次级容错

**问题**：部分场站永远不返回结果
**解决**：超时机制

```python
try:
    await asyncio.wait_for(
        collector.wait_completion(),
        timeout=5.0
    )
except asyncio.TimeoutError:
    # 强制完成，上传已有结果
    await collector.force_complete()
```

### 3. 上传级容错

**问题**：上传失败
**解决**：记录日志，不影响后续批次

```python
try:
    await upload_callback(batch_id, results_list)
except Exception as e:
    logging.error(f"批次 {batch_id} 上传失败: {e}")
    # 批次标记为已完成，避免重复上传
```

## 性能优化

### 1. 并行处理

各场站Worker并行运行，互不阻塞：
```
场站A: ████████ (0.8s)
场站B: ████ (0.4s)
场站C: ██████ (0.6s)
总耗时: max(0.8, 0.4, 0.6) = 0.8s
```

### 2. 超时控制

避免慢速场站拖慢整体：
```
场站A: ████████ (0.8s) ✓
场站B: ████ (0.4s) ✓
场站C: ████████████████████████ (超时) ✗
批次: 5秒后上传 [A, B]
```

### 3. 内存管理

自动清理过期批次：
```python
# 每60秒清理一次
async def cleanup_expired(max_age=300):
    # 删除超过5分钟的批次
```

## 配置调优

### 超时时间选择

| 场景 | 处理时间 | 推荐超时 | 说明 |
|------|----------|----------|------|
| 快速查询 | < 100ms | 2s | 预留20倍余量 |
| 模型推理 | 100-500ms | 5s (默认) | 预留10倍余量 |
| 复杂计算 | > 500ms | 10s | 预留20倍余量 |

### 批次大小影响

| 场站数 | 内存占用 | 处理时间 | 建议 |
|--------|----------|----------|------|
| < 10 | 低 | 快 | 无需优化 |
| 10-100 | 中 | 中 | 默认配置 |
| > 100 | 高 | 慢 | 增加超时 |

## 监控指标

### 关键指标

1. **批次完成率**：成功上传批次数 / 总批次数
2. **场站成功率**：成功场站数 / 期望场站数
3. **平均耗时**：批次创建到上传的平均时间
4. **超时率**：超时批次数 / 总批次数

### 日志示例

```
INFO: 创建批次 SCHEDULE-STATION-REALTIME-DATA_0_12345_1700000000，期望场站数: 10
INFO: 批次 ... 收到场站 S001 结果，进度: 1/10
INFO: 批次 ... 收到场站 S002 结果，进度: 2/10
...
INFO: 批次 ... 所有场站已完成，触发上传
INFO: 批次 ... 上传结果: 成功=8, 失败=1, 无输出=1
```

或

```
WARNING: 批次 ... 超时 (5秒)，已完成 7/10 个场站
INFO: 强制完成批次 ...，已完成 7/10 个场站
INFO: 批次 ... 上传结果: 成功=6, 失败=3, 无输出=1
```

## 对比总结

### 改造前

```
消息 → 场站A → 单独上传A
     → 场站B → 单独上传B (失败，阻塞)
     → 场站C → (等待中...)
```

**问题**：
- 场站B失败，阻塞整体流程
- 无法批量上传
- 没有超时保护

### 改造后

```
消息 → 批次创建 → 场站A → 提交A ↘
                → 场站B → 提交None → 批次收集 → 5秒后批量上传 [A, C]
                → 场站C → 提交C ↗
```

**优势**：
- ✓ 场站B失败不阻塞
- ✓ 批量上传 [A, C]
- ✓ 5秒超时保护
- ✓ 自动过滤None

## 使用建议

1. **返回值规范**：callback必须返回dict或None
2. **超时合理**：根据实际处理时间调整timeout
3. **日志完善**：记录失败场站信息
4. **监控完备**：追踪批次成功率和耗时
5. **容量规划**：评估并发批次数，避免内存溢出

## 扩展性

### 支持其他聚合策略

当前：等待全部完成或超时
扩展：可以支持"达到N%即上传"

```python
if len(self._results) >= self.expected_count * 0.8:  # 80%完成
    await self._do_upload()
```

### 支持优先级

高优先级批次更短的超时时间：

```python
batch_collector = BatchCollector(
    ...,
    timeout=3.0 if is_high_priority else 5.0
)
```
