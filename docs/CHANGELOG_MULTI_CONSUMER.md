# 更新日志 - 多消费者模式

## 版本：2025-11-19

### 问题描述

当服务长时间停机或某些 topic 消息积压时，由于 Kafka 消费者的 `max_poll_records` 限制了单次拉取的**总消息数**，导致：

1. **Topic 饥饿**：积压严重的 topic 占用所有配额，其他 topic 无法拉取消息
2. **数据不完整**：模块无法获取所有依赖 topic 的数据
3. **系统阻塞**：模型永远无法生成完整输入，一直无输出

### 解决方案

实现了**真正的多消费者模式**：为每个 topic 创建独立的消费者实例，每个消费者有独立的 `max_poll_records` 配额。

### 主要变更

#### 1. kafka_client.py

**新增功能**：
- `_multi_consumer_mode`：多消费者模式开关
- `_topic_consumers`：每个 topic 的独立消费者字典
- 并发拉取消息（`asyncio.gather`）
- 独立的 offset 提交逻辑
- `get_lag_info()`：监控消息积压

**关键实现**：
```python
class AsyncKafkaConsumerClient:
    def __init__(self, topics, config, loop=None):
        # ...
        self._multi_consumer_mode = False
        self._topic_consumers = {}  # {topic: consumer}
    
    async def start(self):
        if self._multi_consumer_mode and len(self.topics) > 1:
            # 为每个 topic 创建独立消费者
            for topic in self.topics:
                consumer = AIOKafkaConsumer(topic, ...)
                await consumer.start()
                self._topic_consumers[topic] = consumer
    
    async def getmany(self, timeout_ms=1000, max_records=None):
        if self._multi_consumer_mode:
            # 并发拉取所有 topic 的消息
            tasks = [self._fetch_from_consumer(c, ...) for c in self._topic_consumers.values()]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # 合并消息
            return merged_batch
```

#### 2. config.py

**配置更新**：
```python
KAFKA_CONFIG = {
    'consumer': {
        'multi_consumer_mode': True,   # 启用多消费者模式
        'max_poll_records': 500,       # 降低单个消费者的拉取量（原 3000）
        # ...
    }
}
```

#### 3. main.py

**新增监控输出**：
```python
# 显示多消费者模式状态
print(f"多消费者模式: {service.consumer._multi_consumer_mode}")
print(f"订阅的 topic 数量: {len(service.consumer._topic_consumers)}")

# 显示消息积压情况
lag_info = service.consumer.get_lag_info()
```

#### 4. 文档

新增 `docs/MULTI_CONSUMER_SOLUTION.md`，包含：
- 问题背景和原理分析
- 架构设计和优势
- 完整配置示例
- 监控与诊断方法
- 最佳实践和常见问题

### 功能特性

#### ✅ 已实现

1. **多消费者模式**
   - 为每个 topic 创建独立消费者
   - 自动检测并启用（配置驱动）
   - 向后兼容单消费者模式

2. **并发拉取**
   - 使用 `asyncio.gather` 并行拉取所有 topic
   - 提高消费效率
   - 异常隔离（单 topic 失败不影响其他）

3. **独立 Offset 管理**
   - 每个消费者独立提交 offset
   - 支持批量提交
   - 失败重试机制

4. **消息积压监控**
   - `get_lag_info()` 获取每个 topic 的 lag
   - 日志输出消费者启动状态
   - 支持运行时监控

5. **优雅停机**
   - 停止所有消费者
   - 提交剩余 offset
   - 清理资源

### 使用方法

#### 启用多消费者模式

**方法 1：全局配置（推荐）**
```python
# config.py
KAFKA_CONFIG = {
    'consumer': {
        'multi_consumer_mode': True,
        'max_poll_records': 500,
    }
}
```

**方法 2：动态配置**
```python
kafka_config = {
    'consumer': {
        'multi_consumer_mode': True,
        'max_poll_records': 500,
    }
}
service = AsyncDataAnalysisService(
    module_name="load_prediction",
    kafka_config=kafka_config
)
```

#### 监控积压情况

```python
# 获取 lag 信息
lag_info = service.consumer.get_lag_info()
for topic, partitions in lag_info.items():
    print(f"{topic}: {len(partitions)} 个分区")
```

### 性能对比

| 指标 | 单消费者模式 | 多消费者模式 |
|------|-------------|-------------|
| **Topic 公平性** | ❌ 不保证 | ✅ 保证 |
| **数据完整性** | ⚠️ 可能缺失 | ✅ 完整 |
| **并发拉取** | ❌ 串行 | ✅ 并行 |
| **单次拉取量** | 3000 条（总） | 500 条/topic |
| **连接数** | 1 | N（topic 数） |
| **内存占用** | 低 | 中等（+20-30MB） |
| **故障隔离** | ❌ 全局影响 | ✅ 独立隔离 |

### 测试场景

推荐测试以下场景：

1. **正常消费**：验证多个 topic 同时消费
2. **消息积压**：停机后重启，观察是否能获取所有 topic 的数据
3. **单 topic 失败**：模拟一个 topic 消费失败，其他 topic 正常
4. **资源占用**：对比单/多消费者模式的内存和连接数

### 配置建议

#### 小规模场景（3-5 个 topic）
```python
'multi_consumer_mode': True,
'max_poll_records': 500,
'session_timeout_ms': 30000,
```

#### 中等规模场景（5-10 个 topic）
```python
'multi_consumer_mode': True,
'max_poll_records': 300,
'session_timeout_ms': 45000,
```

#### 大规模场景（10+ 个 topic）
```python
'multi_consumer_mode': True,
'max_poll_records': 200,
'session_timeout_ms': 60000,
'max_poll_interval_ms': 600000,
```

### 故障排查

#### 1. 多消费者模式未启用

**症状**：日志中看不到 "启用多消费者模式" 的输出

**原因**：
- 配置未设置 `multi_consumer_mode: True`
- 只订阅了 1 个 topic（自动降级）

**解决**：检查配置文件

#### 2. 某个 topic 无法消费

**症状**：某个 topic 的消息一直无法拉取

**排查**：
```python
# 检查消费者是否启动
consumers = service.consumer.get_consumer()
print(consumers)  # 应该是一个字典

# 检查 lag
lag_info = service.consumer.get_lag_info()
```

#### 3. 内存占用增加

**原因**：每个消费者都有独立的缓冲区

**解决**：
- 降低 `max_poll_records`
- 减少订阅的 topic 数量
- 如果资源受限，可以禁用多消费者模式

### 向后兼容性

✅ **完全向后兼容**：
- 不启用 `multi_consumer_mode` 时，行为与之前完全一致
- 现有代码无需修改
- 只需更新配置即可启用

### 注意事项

1. **连接数增加**：每个 topic 一个连接，确保 Kafka 服务器支持
2. **内存占用**：每个消费者约 2-3 MB，N 个 topic ≈ 20-30 MB 额外内存
3. **Consumer Group**：所有消费者使用相同的 `group_id`，不会冲突
4. **顺序保证**：单 topic 内保持分区级别的顺序，跨 topic 不保证

### 未来优化

可能的后续改进方向：

1. **动态调整拉取量**：根据 lag 自动调整 `max_poll_records`
2. **优先级策略**：为重要 topic 分配更高的拉取配额
3. **更精确的 Lag 监控**：集成 Kafka AdminClient 获取准确的 lag 值
4. **自动降级**：检测到资源不足时自动切换到单消费者模式

### 相关文档

- [多消费者模式详细文档](./MULTI_CONSUMER_SOLUTION.md)
- [配置参考](../d_a/config.py)
- [使用示例](../d_a/main.py)

### 作者

xie.fangyu

### 日期

2025-11-19
