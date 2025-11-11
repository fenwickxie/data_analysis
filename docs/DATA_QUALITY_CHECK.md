# 数据可用性检查机制

## 设计理念

在事件驱动模式下：
- **快速topic更新时触发处理**（如 CAR-ORDER 1秒/条）
- **慢速topic使用最新缓存数据**（如 CAR-PRICE 1月/条，使用上次缓存的数据）
- **只需确保topic有数据即可**，不检查数据新鲜度或窗口填满程度

> 核心思想：当最快频率topic更新时，其它慢速topic使用相对于它本身最新的一个缓存数据。

---

## 可能的数据不可用场景

### 1. 新场站首次启动
```python
# 场站刚启动，第一条消息到达
# 只有一个 topic 有数据，其他 topic 都是空
```

### 2. Topic数据延迟/丢失
```python
# Kafka分区异常、消费者重启、生产者故障等
# 导致某些 topic 一直没有数据
```

### 3. Topic配置错误
```python
# 某个 topic 未被正确订阅
# 或 topic 名称配置错误
```

---

## 解决方案：数据可用性元信息

### 返回数据结构

`get_module_input()` 返回的数据现在包含 `_data_quality` 字段：

```python
{
    'stationId': 'STATION_001',
    
    # 业务数据字段...
    'gunNo': [1, 2, 3],
    'outputPower': [50.5, 60.2, 55.8],
    'feeNo1': [0.5, 0.5, 0.5],
    # ...
    
    # 🔥 数据可用性元信息
    '_data_quality': {
        'available_topics': [          # 有数据的 topic（不管新旧）
            'SCHEDULE-CAR-ORDER',
            'SCHEDULE-DEVICE-GUN',
            'SCHEDULE-CAR-PRICE',      # 可能是1个月前的缓存数据，但可用
            'SCHEDULE-DEVICE-STORAGE'
        ],
        'missing_topics': [            # 完全没有数据的 topic
            'SCHEDULE-DEVICE-ERROR'
        ],
        'total_topics': 8,             # 该模块需要的 topic 总数
        'availability_ratio': 0.875    # 可用率 (7/8 = 87.5%)
    }
}
```

### 关键点说明

1. **available_topics**：只要该topic在窗口中有数据就算可用
   - ✅ 快速topic的最新数据（1秒前）
   - ✅ 慢速topic的缓存数据（1个月前）
   - 都算"可用"，因为慢速数据本身更新频率就慢

2. **missing_topics**：窗口完全为空的topic
   - ❌ 从未收到过数据
   - ❌ 或数据已被 `clean_expired()` 清理

3. **不检查数据新鲜度**：事件驱动模式下，慢速数据使用缓存是正常的

---

## 业务模块使用示例

### 示例 1：检查数据可用性后再处理

```python
def my_callback(station_id, module_input):
    """业务回调函数"""
    
    # 获取数据可用性信息
    quality = module_input.get('_data_quality', {})
    
    # 策略1：可用率低于阈值则跳过处理
    if quality.get('availability_ratio', 0) < 0.8:
        logging.warning(
            f"[{station_id}] 数据可用率不足: {quality['availability_ratio']:.1%}, "
            f"缺失 topics: {quality.get('missing_topics', [])}"
        )
        return None  # 跳过本次处理
    
    # 策略2：检查关键 topic 是否可用
    critical_topics = ['SCHEDULE-CAR-ORDER', 'SCHEDULE-DEVICE-GUN']
    available = quality.get('available_topics', [])
    
    if not all(topic in available for topic in critical_topics):
        logging.error(f"[{station_id}] 关键 topic 数据不可用")
        return None
    
    # 数据可用，继续处理（慢速topic使用缓存数据是正常的）
    result = process_station_data(module_input)
    return result
```

### 示例 2：降级处理策略

```python
def my_callback(station_id, module_input):
    """带降级策略的业务回调"""
    
    quality = module_input.get('_data_quality', {})
    availability = quality.get('availability_ratio', 0)
    
    # 全部数据可用
    if availability >= 0.9:
        return full_algorithm(module_input)
    
    # 部分数据可用（使用简化算法）
    elif availability >= 0.6:
        logging.info(f"[{station_id}] 使用简化算法处理部分数据")
        return simplified_algorithm(module_input)
    
    # 数据严重不足
    else:
        logging.warning(f"[{station_id}] 数据不足，使用默认策略")
        return default_strategy(station_id)
```

### 示例 3：数据可用性监控

```python
def result_handler(station_id, module_input, result):
    """结果处理函数，同时记录数据可用性"""
    
    quality = module_input.get('_data_quality', {})
    
    # 记录监控指标
    metrics = {
        'station_id': station_id,
        'timestamp': time.time(),
        'availability': quality.get('availability_ratio', 0),
        'missing_count': len(quality.get('missing_topics', [])),
        'available_count': len(quality.get('available_topics', [])),
    }
    
    # 发送到监控系统
    send_to_prometheus(metrics)
    
    # 数据可用性告警
    if metrics['availability'] < 0.5:
        alert_low_data_availability(station_id, quality)
    
    # 保存结果
    save_result(station_id, result)
```

---

## 配置参数

### 数据过期时间

在 `DataDispatcher` 初始化时配置，用于 `clean_expired()` 清理过期数据：

```python
# 默认 600 秒（10分钟）
dispatcher = DataDispatcher(data_expire_seconds=600)

# 建议：根据慢速topic的频率设置
# 例如：CAR-PRICE 1个月更新一次，设置为 35 天确保不被清理
dispatcher = DataDispatcher(data_expire_seconds=3024000)  # 35天
```

### 窗口大小

在 `config.py` 的 `TOPIC_DETAIL` 中配置：

```python
TOPIC_DETAIL = {
    "SCHEDULE-CAR-ORDER": {
        "window_size": 60,      # 快速 topic: 保留 60 条（约 1 分钟历史）
        "fields": ["gunNo", "orderNo", ...],
    },
    "SCHEDULE-CAR-PRICE": {
        "window_size": 1,       # 慢速 topic: 只保留最新 1 条即可
        "fields": ["feeNo1", "feeNo2", ...],
    },
    # ...
}
```

---

## 注意事项

### 1. 慢速数据使用缓存是正常的
```python
# ✅ 正常场景：
# - CAR-ORDER (1秒) 刚更新 → 触发处理
# - CAR-PRICE (1月) 使用 30 天前的缓存数据
# 这是预期行为，不是数据问题！
```

### 2. 性能影响
- 数据可用性检查开销极小（<0.5ms）
- 已使用 `threading.RLock` 确保线程安全

### 3. 向后兼容
- `_data_quality` 字段不会影响现有业务逻辑
- 业务模块可以选择性使用或忽略

### 4. 最佳实践
```python
# ✅ 推荐：检查关键topic是否可用
critical_topics = ['SCHEDULE-CAR-ORDER', 'SCHEDULE-DEVICE-GUN']
available = quality.get('available_topics', [])
if all(t in available for t in critical_topics):
    process(module_input)

# ❌ 不推荐：检查数据新鲜度（事件驱动模式下无意义）
# if data_age < 60:  # 不需要这样检查
#     process(module_input)
```

---

## 常见问题

### Q1: 为什么我的可用率总是很低？
**A:** 检查以下原因：
1. Kafka topic 是否都在正常生产数据
2. 消费者组是否正确订阅所有 topic
3. `data_expire_seconds` 配置是否过短（慢速topic数据被清理）
4. 是否是新场站刚启动，数据还在收集中

### Q2: 慢速topic的数据很旧，会不会影响结果？
**A:** 不会，这是正常的：
- 慢速topic（如CAR-PRICE 1月更新一次）本身就是缓慢变化的数据
- 事件驱动模式下，使用其最新缓存值是预期行为
- 业务算法应该设计为能处理这种时间尺度差异

### Q3: 可以只检查部分 topic 的可用性吗？
**A:** 可以，在业务回调中自定义检查逻辑：
```python
critical_topics = ['SCHEDULE-CAR-ORDER', 'SCHEDULE-DEVICE-GUN']
available = quality.get('available_topics', [])
is_critical_available = all(t in available for t in critical_topics)
```

### Q4: 如何处理某个topic一直没有数据的情况？
**A:** 有几种策略：
```python
# 策略1：跳过处理
if 'CRITICAL-TOPIC' in quality.get('missing_topics', []):
    return None

# 策略2：使用默认值
if 'OPTIONAL-TOPIC' in quality.get('missing_topics', []):
    module_input['field'] = default_value
    
# 策略3：降级算法
if quality.get('availability_ratio', 0) < 0.8:
    return simplified_algorithm(module_input)
```

---

## 相关文件

- `d_a/dispatcher.py`: 数据质量检查实现
- `d_a/analysis_service.py`: 事件驱动触发机制
- `d_a/config.py`: 窗口大小和过期时间配置
