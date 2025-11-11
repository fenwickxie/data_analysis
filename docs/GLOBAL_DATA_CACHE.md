# 全局数据缓存机制

## 问题描述

之前的全局数据处理存在严重问题：**如果全局数据（如 `SCHEDULE-ENVIRONMENT-CALENDAR`）在还没有任何场站注册时到达，数据会被跳过并丢失。**

### 原有逻辑的问题

```python
# ❌ 旧逻辑（有问题）
if station_id == '__global__':
    known_stations = list(self._station_threads.keys())
    if not known_stations:
        # 如果还没有场站，跳过 ← 数据丢失！
        continue
    # 广播到所有场站
    for sid in known_stations:
        self.dispatcher.update_topic_data(sid, topic, station_data)
```

**问题场景**：
1. 系统启动时，先收到全局数据（`SCHEDULE-ENVIRONMENT-CALENDAR`）
2. 此时还没有场站注册（`_station_threads` 为空）
3. 数据被 `continue` 跳过，永久丢失
4. 后续场站注册时，无法获取这些全局数据

---

## 解决方案：全局数据缓存机制

### 核心思路

1. **缓存全局数据**：收到全局数据时，无论是否有场站，都先缓存起来
2. **立即广播**：如果有已注册的场站，立即广播
3. **新场站补偿**：新场站注册时，自动应用所有缓存的全局数据

### 实现细节

#### 1. 添加缓存字段

在 `AsyncDataAnalysisService` 和 `DataAnalysisService` 中添加：

```python
self._global_data_cache = {}  # 缓存全局数据: {topic: latest_data}
```

#### 2. 修改全局数据处理逻辑

```python
# ✅ 新逻辑（修复后）
if station_id == '__global__':
    # 1. 缓存全局数据（最新的）
    self._global_data_cache[topic] = station_data
    
    # 2. 广播到所有已知场站
    known_stations = list(self._station_threads.keys())
    if known_stations:
        logging.info(f"广播全局数据 topic={topic} 到 {len(known_stations)} 个场站")
        for sid in known_stations:
            self.dispatcher.update_topic_data(sid, topic, station_data)
            if sid in self._station_data_events:
                self._station_data_events[sid].set()
    else:
        logging.info(f"全局数据已缓存 topic={topic}，等待场站注册")
    continue
```

#### 3. 新场站注册时应用缓存

```python
# 创建场站任务
if station_id not in self._station_threads:
    # ... 创建任务的代码 ...
    
    # 新场站注册时，应用所有缓存的全局数据
    if self._global_data_cache:
        logging.info(f"新场站 {station_id} 注册，应用 {len(self._global_data_cache)} 个全局数据")
        for global_topic, global_data in self._global_data_cache.items():
            self.dispatcher.update_topic_data(station_id, global_topic, global_data)
```

---

## 工作流程

### 场景1：先有全局数据，后有场站

```
时间线：
T1: 收到 SCHEDULE-ENVIRONMENT-CALENDAR (全局数据)
    → 缓存到 _global_data_cache['SCHEDULE-ENVIRONMENT-CALENDAR']
    → 没有场站，只缓存，不广播
    → 日志: "全局数据已缓存 SCHEDULE-ENVIRONMENT-CALENDAR，等待场站注册"

T2: 收到 SCHEDULE-STATION-PARAM (场站 S1 的数据)
    → 创建场站 S1 的任务
    → 检测到 _global_data_cache 不为空
    → 将缓存的全局数据应用到场站 S1
    → 日志: "新场站 S1 注册，应用 1 个全局数据"
    → dispatcher 中场站 S1 现在同时拥有场站数据和全局数据

T3: 收到 SCHEDULE-STATION-PARAM (场站 S2 的数据)
    → 创建场站 S2 的任务
    → 将缓存的全局数据应用到场站 S2
    → 日志: "新场站 S2 注册，应用 1 个全局数据"
```

### 场景2：先有场站，后有全局数据

```
时间线：
T1: 收到 SCHEDULE-STATION-PARAM (场站 S1, S2 的数据)
    → 创建场站 S1, S2 的任务
    → _global_data_cache 为空，无需应用

T2: 收到 SCHEDULE-ENVIRONMENT-CALENDAR (全局数据)
    → 缓存到 _global_data_cache['SCHEDULE-ENVIRONMENT-CALENDAR']
    → 检测到有 2 个已注册场站
    → 立即广播到 S1, S2
    → 日志: "广播全局数据 SCHEDULE-ENVIRONMENT-CALENDAR 到 2 个场站"

T3: 收到 SCHEDULE-STATION-PARAM (场站 S3 的数据)
    → 创建场站 S3 的任务
    → 将缓存的全局数据应用到场站 S3
    → 日志: "新场站 S3 注册，应用 1 个全局数据"
    → S3 也能获取到之前的全局数据
```

### 场景3：全局数据更新

```
时间线：
T1: 收到 SCHEDULE-ENVIRONMENT-CALENDAR (2025年11月的日历)
    → 缓存: _global_data_cache['SCHEDULE-ENVIRONMENT-CALENDAR'] = 11月数据
    → 广播到所有场站

T2: 收到 SCHEDULE-ENVIRONMENT-CALENDAR (2025年12月的日历)
    → 更新缓存: _global_data_cache['SCHEDULE-ENVIRONMENT-CALENDAR'] = 12月数据
    → 广播到所有场站
    → 新注册的场站会获取最新的12月数据（而非11月）
```

---

## 优势

### 1. **数据不丢失**
   - 无论全局数据何时到达，都会被缓存
   - 后注册的场站也能获取全局数据

### 2. **自动同步**
   - 新场站注册时自动应用所有全局数据
   - 无需手动触发同步

### 3. **保持最新**
   - 缓存始终保存最新的全局数据
   - 全局数据更新时，新场站获取最新版本

### 4. **日志清晰**
   - 明确区分"缓存"、"广播"、"应用缓存"操作
   - 便于调试和监控

---

## 内存管理

### 当前策略

- 每个全局 topic 只缓存**最新的一条数据**
- 旧数据会被新数据覆盖
- 缓存大小取决于全局 topic 数量（预计 1-3 个）

### 典型内存占用

假设只有 `SCHEDULE-ENVIRONMENT-CALENDAR` 一个全局 topic：

```python
{
    'SCHEDULE-ENVIRONMENT-CALENDAR': {
        'calendar': [30天的日历数据]  # 约 2-5 KB
    }
}
```

**总内存**: < 10 KB （可忽略不计）

### 未来优化（如需要）

如果全局数据非常多或非常大，可以添加：

1. **LRU缓存**：限制最多缓存N个全局topic
2. **过期机制**：缓存数据超过TTL后自动清除
3. **压缩存储**：对大数据进行压缩

---

## 已知的全局数据 Topic

当前只有 **1个** 全局数据 topic：

| Topic | 说明 | 数据格式 | 缓存必要性 |
|-------|------|----------|-----------|
| `SCHEDULE-ENVIRONMENT-CALENDAR` | 日历数据（节假日、周末） | `{'calendar': [...]}` | ✅ 高 |

**识别规则**: 数据字典中包含 `calendar` 字段

---

## 测试建议

### 单元测试

```python
def test_global_data_cache():
    # 1. 先发送全局数据
    service.process_message(global_calendar_msg)
    assert 'SCHEDULE-ENVIRONMENT-CALENDAR' in service._global_data_cache
    
    # 2. 注册新场站
    service.process_message(station_param_msg)
    
    # 3. 验证新场站获取了全局数据
    inputs = service.dispatcher.get_all_inputs('new_station_id')
    assert 'SCHEDULE-ENVIRONMENT-CALENDAR' in inputs
```

### 集成测试

1. 启动服务（无场站）
2. 发送全局数据到 Kafka
3. 发送场站数据到 Kafka
4. 验证场站能够获取全局数据
5. 检查日志是否正确

---

## 总结

通过引入全局数据缓存机制，解决了：

✅ **数据丢失问题**：全局数据无论何时到达都不会丢失  
✅ **新场站问题**：新场站注册时自动获取所有全局数据  
✅ **数据一致性**：所有场站都能获取到全局数据  
✅ **内存可控**：只缓存最新数据，内存占用极小（< 10KB）  

修改后的代码更加健壮，能够处理各种消息到达顺序。
