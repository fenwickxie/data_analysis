# 多消费者模式解决消息积压问题

## 问题背景

当服务长时间停机或订阅的某些 topic 生产速度很快时，会累积大量未消费的消息。由于 Kafka 消费者的 `max_poll_records` 限制了**单次拉取的总消息数**，单个消费者订阅多个 topic 时可能出现：

- **Topic 饥饿问题**：某些积压严重的 topic 占用所有拉取配额，导致其他 topic 的消息无法被拉取
- **数据不完整**：模块无法获取所有依赖 topic 的数据，导致无法生成完整的模型输入
- **系统阻塞**：数据永远无法完整，模型一直无输出

### 问题示例

假设订阅 5 个 topic，`max_poll_records=3000`：

```
Topic A: 10,000 条积压消息
Topic B: 5,000 条积压消息
Topic C: 100 条积压消息
Topic D: 50 条积压消息
Topic E: 10 条积压消息
```

单次 poll 可能返回：
- Topic A: 2500 条
- Topic B: 500 条
- Topic C/D/E: 0 条 ❌

导致永远无法获取 C/D/E 的数据，模型输入不完整。

## 解决方案：多消费者模式

### 架构设计

为**每个 topic 创建独立的消费者实例**，每个消费者有自己的 `max_poll_records` 配额和**独立的 consumer group**：

```
Topic A → Consumer A (group_id: mygroup-TopicA, max_poll_records: 500)
Topic B → Consumer B (group_id: mygroup-TopicB, max_poll_records: 500)
Topic C → Consumer C (group_id: mygroup-TopicC, max_poll_records: 500)
Topic D → Consumer D (group_id: mygroup-TopicD, max_poll_records: 500)
Topic E → Consumer E (group_id: mygroup-TopicE, max_poll_records: 500)
```

**关键设计**：
- ✅ 每个 topic 使用**独立的 `group_id`**（格式：`{base_group_id}-{topic_name}`）
- ✅ 避免多个消费者在同一个 consumer group 内互相干扰
- ✅ 防止频繁的 rebalance 和 offset 冲突
- ✅ 每个消费者组独立管理自己的 offset，互不影响

**为什么要使用独立的 group_id？**

如果所有消费者使用相同的 group_id：
```
❌ 错误示例：所有消费者使用 "mygroup"
- Kafka 认为它们是同一个消费者组
- 触发分区重新分配（rebalance）
- 可能读取到其他 topic 的历史 offset
- 导致 "Fetch offset is out of range" 错误
```

使用独立的 group_id：
```
✅ 正确示例：每个 topic 有独立的消费者组
- Consumer A: group_id = "mygroup-TopicA"
- Consumer B: group_id = "mygroup-TopicB"
- 每个消费者组独立管理 offset
- 不会互相干扰，不会触发 rebalance
```

### 优势

1. **消除 Topic 饥饿**：每个 topic 都有独立的拉取配额
2. **并发拉取**：使用 `asyncio.gather` 并发从所有 topic 拉取消息
3. **数据完整性**：确保所有 topic 的消息都能被消费
4. **独立控制**：可以为不同 topic 设置不同的消费参数
5. **避免 Rebalance**：独立的 consumer group 防止互相干扰
6. **Offset 隔离**：每个 topic 的 offset 独立管理，不会冲突

## 配置方法

### 1. 启用多消费者模式

在 `config.py` 中配置：

```python
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'consumer': {
        'group_id': 'my-service-group',
        'auto_offset_reset': 'latest',
        'multi_consumer_mode': True,  # ✅ 启用多消费者模式
        'max_poll_records': 500,      # 每个消费者的拉取上限
        'enable_auto_commit': False,
    },
    # ... 其他配置
}
```

### 2. 关键参数说明

| 参数 | 说明 | 推荐值 |
|------|------|--------|
| `multi_consumer_mode` | 是否启用多消费者模式 | `True`（订阅多个 topic 时） |
| `max_poll_records` | **每个消费者**的拉取上限 | 500-1000（避免单次拉取太多） |
| `session_timeout_ms` | 会话超时时间 | 30000（30秒，应对多消费者） |
| `max_poll_interval_ms` | 两次 poll 的最大间隔 | 300000（5分钟） |

### 3. 计算资源消耗

假设订阅 N 个 topic：

- **单消费者模式**：1 个连接，总计 `max_poll_records` 条/次
- **多消费者模式**：N 个连接，每个 `max_poll_records` 条/次，总计 `N * max_poll_records` 条/次

**建议**：
- 小于 5 个 topic：`max_poll_records = 500`
- 5-10 个 topic：`max_poll_records = 300`
- 10+ 个 topic：`max_poll_records = 200`

## 使用示例

### 完整配置示例

```python
# config.py
KAFKA_CONFIG = {
    'bootstrap_servers': ['10.8.4.40:35888'],
    'consumer': {
        'group_id': 'data-analysis-service',
        'auto_offset_reset': 'latest',
        'multi_consumer_mode': True,      # 启用多消费者模式
        'max_poll_records': 500,          # 每个 topic 每次拉取 500 条
        'enable_auto_commit': False,      # 手动提交 offset
        'session_timeout_ms': 30000,      # 30 秒会话超时
        'max_poll_interval_ms': 300000,   # 5 分钟 poll 间隔
    },
    'producer': {
        'acks': 'all',
        'compression_type': 'gzip',
    }
}

# 模块订阅的 topic
MODULE_TO_TOPICS = {
    'load_prediction': [
        'SCHEDULE-STATION-PARAM',
        'SCHEDULE-STATION-REALTIME-DATA',
        'SCHEDULE-CAR-ORDER',
        'SCHEDULE-DEVICE-GUN',
        'SCHEDULE-ENVIRONMENT-CALENDAR',
    ]
}
```

### 服务启动（无需代码修改）

```python
from d_a.analysis_service import AsyncDataAnalysisService

async def main():
    service = AsyncDataAnalysisService(module_name="load_prediction")
    
    async def my_callback(station_id, module_input):
        # 业务逻辑
        result = await my_model_predict(module_input)
        result["station_id"] = station_id
        return result
    
    await service.start(callback=my_callback)
    # 服务会自动使用多消费者模式
```

## 监控与诊断

### 1. 查看消息积压

```python
# 在服务中添加监控
service = AsyncDataAnalysisService(module_name="load_prediction")
await service.start(callback=my_callback)

# 获取 lag 信息
lag_info = service.consumer.get_lag_info()
print(lag_info)

# 输出示例：
# {
#     'SCHEDULE-STATION-PARAM': {0: {'current_offset': 1000, 'lag': 'N/A'}},
#     'SCHEDULE-STATION-REALTIME-DATA': {0: {'current_offset': 5000, 'lag': 'N/A'}},
#     ...
# }
```

### 2. 日志输出

启用多消费者模式时，会看到如下日志：

```
INFO: 启用多消费者模式，为 5 个topic创建独立消费者
INFO: Topic SCHEDULE-STATION-PARAM 消费者已启动，max_poll_records=500
INFO: Topic SCHEDULE-STATION-REALTIME-DATA 消费者已启动，max_poll_records=500
...
```

### 3. 性能指标

多消费者模式的性能特点：

| 指标 | 单消费者 | 多消费者 |
|------|----------|----------|
| Topic 公平性 | ❌ 不保证 | ✅ 保证 |
| 并发拉取 | ❌ 串行 | ✅ 并行 |
| 连接数 | 1 | N（topic 数量） |
| 内存占用 | 低 | 中等（N 倍） |
| 数据完整性 | ⚠️ 可能缺失 | ✅ 完整 |

## 最佳实践

### 1. 何时启用多消费者模式

✅ **推荐启用**：
- 订阅 3 个以上 topic
- 存在消息积压场景
- 数据完整性要求高
- 不同 topic 的消息量差异大

❌ **不推荐启用**：
- 只订阅 1-2 个 topic
- 消息量很小（每秒几条）
- 资源受限环境

### 2. 参数调优

**消息积压场景**（追赶进度）：
```python
'max_poll_records': 1000,         # 增大单次拉取量
'session_timeout_ms': 60000,      # 增加超时时间
'max_poll_interval_ms': 600000,   # 10 分钟
```

**实时消费场景**（正常运行）：
```python
'max_poll_records': 300,          # 减小延迟
'session_timeout_ms': 30000,
'max_poll_interval_ms': 300000,
```

### 3. 故障恢复

多消费者模式下，单个 topic 消费失败不会阻塞其他 topic：

```python
# kafka_client.py 中的错误处理
async def getmany(self, timeout_ms=1000, max_records=None):
    if self._multi_consumer_mode:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                topic = list(self._topic_consumers.keys())[i]
                logging.error(f"从topic {topic} 拉取消息失败: {result}")
                # 其他 topic 继续正常消费 ✅
```

## 技术细节

### 1. 并发拉取实现

```python
async def getmany(self, timeout_ms=1000, max_records=None):
    if self._multi_consumer_mode:
        # 为每个 topic 创建拉取任务
        tasks = []
        for topic, consumer in self._topic_consumers.items():
            tasks.append(self._fetch_from_consumer(consumer, timeout_ms, max_records))
        
        # 并发执行所有拉取任务
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 合并所有消息
        batch = []
        for result in results:
            if isinstance(result, list):
                batch.extend(result)
        
        return batch
```

### 2. Offset 提交

多消费者模式下，offset 分别提交到各自的消费者：

```python
async def commit_offsets(self, offsets=None):
    if self._multi_consumer_mode:
        for topic, consumer in self._topic_consumers.items():
            # 过滤出该 topic 的 offsets
            topic_offsets = {
                tp: om for tp, om in offsets.items() 
                if tp.topic == topic
            }
            if topic_offsets:
                await consumer.commit(topic_offsets)
```

### 3. 资源清理

```python
async def stop(self):
    if self._multi_consumer_mode:
        # 停止所有 topic 的消费者
        for topic, consumer in self._topic_consumers.items():
            await consumer.stop()
            logging.info(f"Topic {topic} 消费者已停止")
```

## 常见问题

### Q1: 多消费者模式会增加多少资源开销？

**A**: 主要是网络连接和内存：
- **连接数**：N 个 topic = N 个连接（每个约 1-2 MB 内存）
- **内存缓冲**：每个消费者的消息缓冲区独立
- **CPU**：并发拉取可能略微增加 CPU 使用

**估算**：10 个 topic ≈ 20-30 MB 额外内存

### Q2: 是否会影响消费顺序？

**A**: 
- **单 topic 内**：保持分区级别的顺序 ✅
- **跨 topic**：不保证相对顺序（但业务通常不需要）

### Q3: 如何回退到单消费者模式？

**A**: 只需修改配置：

```python
KAFKA_CONFIG = {
    'consumer': {
        'multi_consumer_mode': False,  # 或直接删除此行
        # ...
    }
}
```

### Q4: 多消费者模式是否会导致重复消费？

**A**: 不会。每个消费者使用**独立的 `group_id`**（格式：`{base_group_id}-{topic_name}`），完全隔离，不会产生冲突。

### Q5: 为什么会出现 "Fetch offset is out of range" 错误？

**A**: 如果多个消费者使用相同的 `group_id`，会导致：
- 频繁的 rebalance（每启动一个消费者就触发一次）
- Offset 被其他消费者覆盖
- 消息丢失或越界

**解决方案**：当前版本已修复，每个 topic 自动使用独立的 `group_id`。

## 迁移指南

### 从单消费者迁移到多消费者

1. **备份当前配置**
2. **更新配置文件**：
   ```python
   'multi_consumer_mode': True,
   'max_poll_records': 500,  # 降低每个消费者的拉取量
   ```
3. **重启服务**：自动应用新模式
4. **观察日志**：确认所有 topic 消费者启动成功
5. **监控性能**：对比消费速度和数据完整性

### 回滚方案

如果出现问题，立即回滚：

1. 设置 `'multi_consumer_mode': False`
2. 重启服务
3. 检查 offset 是否正确（应该是自动的）

## 总结

多消费者模式通过为每个 topic 创建独立的消费者，彻底解决了消息积压时的 topic 饥饿问题，确保了数据的完整性和系统的稳定性。

**核心优势**：
- ✅ 消除 topic 饥饿，保证数据完整
- ✅ 并发拉取，提高消费效率
- ✅ 独立控制，灵活配置
- ✅ 故障隔离，单 topic 失败不影响其他

**适用场景**：
- 订阅多个 topic（3+ 个）
- 存在消息积压风险
- 数据完整性要求高
- 不同 topic 消息量差异大

只需在配置中设置 `multi_consumer_mode: True`，无需修改业务代码即可启用。
