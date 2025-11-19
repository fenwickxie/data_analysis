# 多消费者模式下某些 Topic 没有拉取到数据的诊断

## 问题描述

在多消费者模式下，某些 topic 的消费者没有拉取到数据。

## 是否是异步拉取的问题？

**不是异步拉取的 bug**，这是正常行为。原因如下：

### 并发拉取的工作原理

```python
# 代码逻辑
tasks = [_fetch_from_consumer(consumer1), _fetch_from_consumer(consumer2), ...]
results = await asyncio.gather(*tasks)  # 并发执行
```

**行为**：
- 所有消费者**同时**向 Kafka 发起 `getmany()` 请求
- 每个消费者等待 `timeout_ms`（默认 1000ms）
- 如果在超时时间内**某个 topic 没有新消息**，该消费者返回空列表 `[]`
- 这是**预期行为**，不是错误

## 为什么某些 Topic 返回空？

### 可能原因1：该 Topic 确实没有新消息 ✅ 正常

```
场景：
- Consumer A (Topic1): offset=100, latest=100 → 已消费到最新，返回 []
- Consumer B (Topic2): offset=50, latest=150 → 有100条新消息，返回 [msg...]
- Consumer C (Topic3): offset=200, latest=200 → 已消费到最新，返回 []

结果：只有 Topic2 有数据，这是正常的！
```

### 可能原因2：timeout_ms 太短 ⚠️ 可优化

```
场景：
- timeout_ms = 1000ms（1秒）
- Topic A 每 0.5 秒产生一次消息 → 能拉到数据
- Topic B 每 2 秒产生一次消息 → 超时返回空 []
- Topic C 每 5 秒产生一次消息 → 超时返回空 []

解决：增加 timeout_ms
```

### 可能原因3：fetch_max_wait_ms 配置影响 ⚠️ 可优化

```
当前配置：
fetch_max_wait_ms = 500ms

影响：
- Kafka broker 最多等待 500ms 就返回结果
- 即使 timeout_ms = 5000，如果 500ms 内没数据也会返回空
- 优先级：fetch_max_wait_ms < timeout_ms

解决：增加 fetch_max_wait_ms 或删除此配置（使用 Kafka 默认值）
```

## 诊断步骤

### 步骤1：启用调试日志

已添加详细的拉取统计日志，在 `main.py` 中设置日志级别：

```python
import logging

# 启用 DEBUG 级别
logging.basicConfig(
    level=logging.DEBUG,  # 改为 DEBUG
    format='[%(asctime)s] %(levelname)s %(name)s %(message)s'
)
```

### 步骤2：观察日志输出

启动后会看到类似的日志：

```
[DEBUG] 多消费者拉取统计 [SCHEDULE-STATION-PARAM:10, SCHEDULE-CAR-ORDER:0, SCHEDULE-CAR-PRICE:15, SCHEDULE-DEVICE-HOST-DCDC:0, SCHEDULE-DEVICE-STORAGE:8, MODULE-OUTPUT-PV_PREDICTION:0] 总计: 33 条消息
```

**分析**：
- `SCHEDULE-STATION-PARAM:10` → 拉取到 10 条
- `SCHEDULE-CAR-ORDER:0` → 没有新消息（正常）
- `SCHEDULE-DEVICE-HOST-DCDC:0` → 没有新消息（正常）

### 步骤3：判断是否正常

#### ✅ 正常情况

如果某些 topic 持续为 0，但：
- 这些 topic 的生产者确实没有发送数据
- 或者已经消费到最新位置
- **这是正常的**，不需要处理

#### ⚠️ 异常情况

如果某些 topic 持续为 0，但：
- 生产者在持续发送数据
- Kafka 中有大量积压消息
- **这需要调查**

### 步骤4：检查 Topic 的实际状态

使用 Kafka 命令行工具检查：

```bash
# 检查 topic 的最新 offset
kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list 10.8.4.40:35888 \
  --topic SCHEDULE-CAR-ORDER \
  --time -1

# 检查消费者组的消费进度
kafka-consumer-groups.sh \
  --bootstrap-server 10.8.4.40:35888 \
  --group stack-charge-tcp-command-xfy \
  --describe
```

输出示例：
```
TOPIC                    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
SCHEDULE-CAR-ORDER       0          12850          12850           0    ← 没有积压，正常
SCHEDULE-STATION-PARAM   0          5000           5100            100  ← 有100条积压
```

- `LAG = 0` → 已消费到最新，返回空是正常的
- `LAG > 0` → 有积压但没拉到数据，需要优化配置

## 解决方案

### 方案1：优化 timeout_ms（推荐）

如果确认有积压但拉不到数据，增加超时时间：

```python
# 在调用 getmany 时
batch = await consumer_client.getmany(
    timeout_ms=5000,  # 从 1000ms 增加到 5000ms
    max_records=None
)
```

### 方案2：调整 fetch_max_wait_ms（推荐）

删除或增加 `fetch_max_wait_ms`：

```python
# d_a/config.py
KAFKA_CONFIG = {
    'consumer': {
        # 方案 A：删除此配置，使用 Kafka 默认值（推荐）
        # "fetch_max_wait_ms": 500,  # ← 注释掉
        
        # 方案 B：增加等待时间
        "fetch_max_wait_ms": 2000,  # 从 500ms 增加到 2000ms
        
        # 其他配置保持不变
        "fetch_min_bytes": 1,
    }
}
```

**推荐**：直接删除 `fetch_max_wait_ms`，让 Kafka 使用默认值（500ms 已经足够大了）。

### 方案3：增加 max_poll_records（如果消息积压严重）

```python
# d_a/config.py
KAFKA_CONFIG = {
    'consumer': {
        'max_poll_records': 500,  # 从 200 增加到 500
    }
}
```

### 方案4：使用重试机制（适用于消息生产频率低的场景）

```python
# 示例：多次拉取直到所有 topic 都有数据
async def fetch_until_all_topics_have_data(consumer_client, required_topics, max_attempts=5):
    """持续拉取直到所有必需的 topic 都有数据"""
    all_messages = []
    topics_with_data = set()
    
    for attempt in range(max_attempts):
        batch = await consumer_client.getmany(timeout_ms=2000)
        
        # 统计哪些 topic 有数据
        for msg in batch:
            topics_with_data.add(msg.topic)
        
        all_messages.extend(batch)
        
        # 检查是否所有必需的 topic 都有数据
        if required_topics.issubset(topics_with_data):
            logging.info(f"所有必需的 topic 都有数据，拉取完成")
            break
        
        missing = required_topics - topics_with_data
        logging.debug(f"第 {attempt+1} 次拉取，缺失 topic: {missing}")
    
    return all_messages
```

## 配置调优建议

### 当前配置分析

```python
'consumer': {
    'max_poll_records': 200,          # 每个消费者每次最多拉取 200 条
    'fetch_max_wait_ms': 500,         # Kafka broker 最多等待 500ms
    'fetch_min_bytes': 1,             # 至少 1 字节就返回
    'max_partition_fetch_bytes': 100MB,
    'fetch_max_bytes': 500MB,
}
```

**问题**：
- `fetch_max_wait_ms=500` + `fetch_min_bytes=1` → 只要有 1 字节数据或等待 500ms 就返回
- 如果某个 topic 在 500ms 内没有数据，就返回空

### 推荐配置

#### 配置 A：平衡模式（推荐）

```python
'consumer': {
    'max_poll_records': 500,          # 增加拉取上限
    # 'fetch_max_wait_ms': 500,       # 删除，使用默认值
    'fetch_min_bytes': 1024,          # 至少 1KB 才返回（减少空拉取）
    'max_partition_fetch_bytes': 100 * 1024 * 1024,
    'fetch_max_bytes': 500 * 1024 * 1024,
}

# 调用时
batch = await consumer_client.getmany(timeout_ms=3000)  # 增加到 3 秒
```

#### 配置 B：高吞吐模式

```python
'consumer': {
    'max_poll_records': 1000,         # 大批量拉取
    'fetch_max_wait_ms': 1000,        # 等待更长时间
    'fetch_min_bytes': 10240,         # 至少 10KB
}

# 调用时
batch = await consumer_client.getmany(timeout_ms=5000)
```

#### 配置 C：实时模式

```python
'consumer': {
    'max_poll_records': 100,          # 小批量，快速处理
    'fetch_max_wait_ms': 100,         # 快速返回
    'fetch_min_bytes': 1,             # 有数据就返回
}

# 调用时，高频率拉取
batch = await consumer_client.getmany(timeout_ms=1000)
```

## 总结

### 异步拉取不是问题

- ✅ 并发拉取是正确的设计
- ✅ 某些 topic 返回空是正常行为（如果确实没有新消息）
- ✅ `asyncio.gather` 确保所有消费者都有机会拉取

### 可能的真正问题

1. **配置问题**：`fetch_max_wait_ms` 太短，或 `timeout_ms` 太短
2. **业务问题**：某些 topic 的生产频率确实很低
3. **Kafka 问题**：网络延迟或 broker 性能问题

### 建议的行动方案

1. **启用调试日志**（已添加）→ 观察每个 topic 的拉取情况
2. **使用 Kafka 工具**检查 LAG → 确认是否有积压
3. **调整配置**：
   - 删除 `fetch_max_wait_ms` 或增加到 1000ms
   - 增加 `timeout_ms` 到 3000-5000ms
   - 考虑增加 `max_poll_records` 到 500
4. **如果确认配置无误**：那么返回空就是正常的，说明那些 topic 没有新消息

---

**诊断工具**：已在代码中添加详细的拉取统计日志  
**下一步**：启用 DEBUG 日志，观察实际拉取情况
