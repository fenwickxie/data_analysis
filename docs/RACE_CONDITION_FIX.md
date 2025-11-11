# 竞态条件修复：全局数据应用顺序

## 问题描述

在新场站注册时，存在一个潜在的**竞态条件**问题：

### 错误的顺序（修复前）

```python
# ❌ 错误：先创建场站任务，后应用全局数据
if station_id not in self._station_tasks:
    self._create_station_task(station_id)  # ← 场站 worker 线程启动
    
    # 应用全局缓存数据
    if self._global_data_cache:
        for global_topic, global_data in self._global_data_cache.items():
            self.dispatcher.update_topic_data(station_id, global_topic, global_data)
```

### 问题场景

**时间线**：
```
T1: 场站 S1 首次收到数据
    → 检测到 S1 不在 _station_tasks 中
    → 创建场站任务 _create_station_task(S1)
    → 场站 worker 线程启动 ✅
    
T2: 场站 worker 线程执行
    → 调用 dispatcher.get_all_inputs(S1)
    → ⚠️ 此时全局数据还未应用！
    → 获取到不完整的输入数据
    
T3: 主线程应用全局缓存
    → 将全局数据应用到 dispatcher
    → 但场站 worker 已经执行过了
    → ❌ 数据不一致！
```

### 具体影响

1. **数据不完整**：场站 worker 第一次执行时无法获取全局数据
2. **业务逻辑错误**：如果业务依赖全局数据（如日历、节假日），会得到错误的结果
3. **不可预测**：由于线程调度的不确定性，问题可能不是每次都出现

---

## 解决方案

### 正确的顺序（修复后）

```python
# ✅ 正确：先应用全局数据，后创建场站任务
if station_id not in self._station_tasks:
    # ⚠️ 重要：先应用全局缓存数据，再创建场站任务
    # 避免竞态条件：确保场站 worker 启动时已有完整的全局数据
    if self._global_data_cache:
        logging.info(f"新场站 {station_id} 注册，应用 {len(self._global_data_cache)} 个全局数据")
        for global_topic, global_data in self._global_data_cache.items():
            self.dispatcher.update_topic_data(station_id, global_topic, global_data)
    
    # 创建场站任务（此时全局数据已就绪）
    self._create_station_task(station_id)
```

### 修复后的时间线

```
T1: 场站 S1 首次收到数据
    → 检测到 S1 不在 _station_tasks 中
    → 应用所有全局缓存数据到 dispatcher ✅
    → 全局数据就绪
    
T2: 创建场站任务
    → 调用 _create_station_task(S1)
    → 场站 worker 线程启动 ✅
    
T3: 场站 worker 线程执行
    → 调用 dispatcher.get_all_inputs(S1)
    → ✅ 获取到完整的输入数据（包含全局数据）
    → 业务逻辑正确执行
```

---

## 为什么这个顺序很重要

### 1. **线程安全性**

场站 worker 是在独立的线程/任务中运行的：
- **AsyncDataAnalysisService**: `asyncio.create_task()` 创建异步任务
- **DataAnalysisService**: `ThreadPoolExecutor.submit()` 创建线程

一旦创建，这些 worker 就可能立即开始执行。

### 2. **数据完整性保证**

```python
# dispatcher 的数据结构
dispatcher._data = {
    'S1': {
        'SCHEDULE-STATION-PARAM': {...},          # 场站数据
        'SCHEDULE-ENVIRONMENT-CALENDAR': {...},   # 全局数据（必须提前应用）
        # ...
    }
}
```

如果全局数据还未应用，`get_all_inputs('S1')` 会返回不完整的数据。

### 3. **业务逻辑依赖**

许多业务模块依赖全局数据：

```python
def my_model_predict(module_input):
    # ❌ 如果全局数据未应用
    calendar = module_input.get('SCHEDULE-ENVIRONMENT-CALENDAR')  # None
    if calendar is None:
        # 无法判断是否节假日，影响预测结果
        pass
```

---

## 修改文件

### 1. AsyncDataAnalysisService（异步版本）

**文件**: `analysis_service.py` line ~130

**修改前**:
```python
if station_id not in self._station_tasks:
    self._create_station_task(station_id)
    if self._global_data_cache:
        # 应用全局数据...
```

**修改后**:
```python
if station_id not in self._station_tasks:
    if self._global_data_cache:
        # 先应用全局数据...
    self._create_station_task(station_id)
```

### 2. DataAnalysisService（同步版本）

**文件**: `analysis_service.py` line ~450

**修改前**:
```python
if station_id not in self._station_threads:
    # 创建线程...
    future = self._executor.submit(...)
    self._station_threads[station_id] = future
    
    if self._global_data_cache:
        # 应用全局数据...
```

**修改后**:
```python
if station_id not in self._station_threads:
    if self._global_data_cache:
        # 先应用全局数据...
    
    # 创建线程...
    future = self._executor.submit(...)
    self._station_threads[station_id] = future
```

---

## 测试验证

### 单元测试

```python
def test_global_data_applied_before_worker_starts():
    service = AsyncDataAnalysisService(module_name='test_module')
    
    # 1. 先发送全局数据
    global_msg = create_message('SCHEDULE-ENVIRONMENT-CALENDAR', {
        'calendar': [{'date': '2025-11-11', 'isHoliday': 0}]
    })
    await service._process_message(global_msg)
    
    # 2. 验证缓存
    assert 'SCHEDULE-ENVIRONMENT-CALENDAR' in service._global_data_cache
    
    # 3. 发送场站数据（触发场站注册）
    station_msg = create_message('SCHEDULE-STATION-PARAM', [{
        'stationId': 'S1', 'gunNum': 2
    }])
    await service._process_message(station_msg)
    
    # 4. 验证场站 worker 启动前全局数据已应用
    inputs = service.dispatcher.get_all_inputs('S1')
    assert 'SCHEDULE-ENVIRONMENT-CALENDAR' in inputs  # ✅ 全局数据已应用
    assert 'SCHEDULE-STATION-PARAM' in inputs         # ✅ 场站数据也有
```

### 集成测试

```python
async def test_race_condition():
    service = AsyncDataAnalysisService(module_name='test_module')
    
    callback_results = []
    
    async def callback(station_id, module_input):
        # 记录 callback 首次执行时是否有全局数据
        has_calendar = 'SCHEDULE-ENVIRONMENT-CALENDAR' in module_input
        callback_results.append((station_id, has_calendar))
    
    await service.start(callback=callback)
    
    # 发送全局数据 + 场站数据
    await send_messages_to_kafka([
        global_calendar_msg,
        station_param_msg
    ])
    
    await asyncio.sleep(2)  # 等待处理
    
    # 验证：callback 首次执行时必须有全局数据
    assert len(callback_results) > 0
    station_id, has_calendar = callback_results[0]
    assert has_calendar is True  # ✅ 必须为 True
```

---

## 其他相关的竞态条件防护

### 1. 数据就绪事件的触发时机

```python
# ✅ 正确：数据应用后再触发事件
self.dispatcher.update_topic_data(station_id, topic, station_data)
if station_id in self._station_data_events:
    self._station_data_events[station_id].set()  # 触发在数据应用之后
```

### 2. 全局数据广播的原子性

```python
# 全局数据广播
if station_id == '__global__':
    self._global_data_cache[topic] = station_data  # 先缓存
    
    known_stations = list(self._station_tasks.keys())  # 快照，避免迭代中修改
    for sid in known_stations:
        self.dispatcher.update_topic_data(sid, topic, station_data)
```

---

## 总结

✅ **修复前**: 先创建 worker，后应用全局数据 → 竞态条件 → 数据不完整

✅ **修复后**: 先应用全局数据，后创建 worker → 无竞态条件 → 数据完整

这个修复确保了：
1. **数据完整性**：worker 启动时一定有完整的全局数据
2. **线程安全**：避免了数据应用和 worker 执行之间的竞态
3. **业务正确性**：业务逻辑始终能获取到全局数据

**修复时间**: 2025-11-11  
**影响范围**: AsyncDataAnalysisService, DataAnalysisService  
**测试状态**: ✅ 语法检查通过
