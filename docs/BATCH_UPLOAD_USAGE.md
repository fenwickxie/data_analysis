# 批次结果聚合上传使用指南

## 概述

批次结果聚合器 (`BatchResultAggregator`) 解决了以下问题：

1. **数据不完整问题**：单个Kafka消息中某些场站数据可能缺失
2. **批次上传需求**：同一批数据的所有场站结果需要聚合到一个列表中上传
3. **容错机制**：允许部分场站出错，不阻塞整体上传
4. **超时保护**：即使部分场站未完成，也会在超时后触发上传

## 工作流程

```
Kafka消息 (包含多个场站数据)
    ↓
创建批次 (batch_id, expected_stations)
    ↓
各场站Worker并行处理
    ↓  ↓  ↓
场站A → 成功输出result_A
场站B → 处理失败 (None)
场站C → 成功输出result_C
    ↓  ↓  ↓
BatchCollector 收集结果
    ↓
等待全部完成 或 超时 (5秒)
    ↓
批次上传: [result_A, result_C]  (过滤掉None)
```

## 使用方法

### 1. 基本用法

```python
import asyncio
from d_a.analysis_service import AsyncDataAnalysisService

# 定义批次上传回调
async def batch_upload_handler(batch_id, results_list):
    """
    批次上传回调
    
    Args:
        batch_id: 批次ID (格式: "topic_partition_offset_timestamp")
        results_list: 所有成功场站的输出结果列表
            [
                {"station_id": "S001", "data": {...}},
                {"station_id": "S002", "data": {...}},
                ...
            ]
    """
    print(f"批次 {batch_id} 上传 {len(results_list)} 个场站结果")
    
    # 上传到Kafka或其他目标
    # await kafka_producer.send("output_topic", value=results_list)

# 定义单场站处理回调
async def process_station(station_id, module_input):
    """
    处理单个场站的数据
    
    Returns:
        dict: 场站输出结果（必须是字典）
        None: 处理失败
    """
    try:
        # 业务处理逻辑
        result = {
            "station_id": station_id,
            "data": module_input,
            # ... 其他输出字段
        }
        return result
    except Exception as e:
        print(f"场站 {station_id} 处理失败: {e}")
        return None  # 返回None表示失败

# 启动服务
service = AsyncDataAnalysisService(
    module_name="your_module",
    topics=["SCHEDULE-STATION-REALTIME-DATA"],
)

await service.start(
    callback=process_station,
    batch_upload_handler=batch_upload_handler
)
```

### 2. 完整示例

```python
import asyncio
import json
from d_a.analysis_service import AsyncDataAnalysisService
from aiokafka import AIOKafkaProducer

class ModelService:
    def __init__(self):
        self.service = AsyncDataAnalysisService(
            module_name="model_inference",
            topics=["SCHEDULE-STATION-REALTIME-DATA"],
        )
        self.producer = None
    
    async def start(self):
        # 创建Kafka生产者
        self.producer = AIOKafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await self.producer.start()
        
        # 启动服务
        await self.service.start(
            callback=self.process_station,
            batch_upload_handler=self.upload_batch
        )
    
    async def process_station(self, station_id, module_input):
        """单场站处理"""
        try:
            # 模型推理
            prediction = await self.run_model(station_id, module_input)
            
            # 返回结果（必须是字典）
            return {
                "station_id": station_id,
                "timestamp": module_input.get("_data_quality", {}).get("last_update"),
                "prediction": prediction,
                "confidence": 0.95
            }
        except Exception as e:
            print(f"场站 {station_id} 模型推理失败: {e}")
            return None  # 返回None，该场站不会阻塞批次上传
    
    async def upload_batch(self, batch_id, results_list):
        """批次上传"""
        if not results_list:
            print(f"批次 {batch_id} 没有成功的结果，跳过上传")
            return
        
        # 构建上传消息
        message = {
            "batch_id": batch_id,
            "count": len(results_list),
            "results": results_list
        }
        
        # 上传到Kafka
        try:
            await self.producer.send("MODEL_OUTPUT_TOPIC", value=message)
            print(f"批次 {batch_id} 上传成功: {len(results_list)} 个场站")
        except Exception as e:
            print(f"批次 {batch_id} 上传失败: {e}")
    
    async def run_model(self, station_id, module_input):
        """模型推理逻辑"""
        # 这里实现你的模型推理
        await asyncio.sleep(0.1)  # 模拟推理耗时
        return {"predicted_value": 42}
    
    async def stop(self):
        await self.service.stop()
        if self.producer:
            await self.producer.stop()

# 运行
async def main():
    service = ModelService()
    await service.start()
    
    # 运行一段时间
    await asyncio.sleep(3600)
    
    await service.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

## 关键特性

### 1. 容错机制

- **场站失败不阻塞**：某个场站处理失败（返回None）不会影响其他场站
- **超时保护**：5秒后自动触发上传，即使部分场站未完成
- **结果过滤**：自动过滤掉None结果，只上传成功的场站

### 2. 批次管理

- **自动批次创建**：根据消息的 `partition_offset_timestamp` 自动创建批次
- **期望场站追踪**：记录消息中包含的所有场站ID
- **完成度监控**：实时追踪已完成场站数 / 期望场站数

### 3. 性能优化

- **并行处理**：各场站Worker并行处理，互不阻塞
- **异步上传**：上传操作不阻塞消息消费
- **自动清理**：定期清理过期批次，避免内存泄漏

## 配置参数

```python
service = AsyncDataAnalysisService(
    module_name="your_module",
    topics=["YOUR_TOPIC"],
    batch_timeout=5.0,  # 批次超时时间（秒）
    cleanup_interval=60.0  # 清理间隔（秒）
)
```

### 超时时间建议

- **快速模型** (< 100ms): `batch_timeout=2.0`
- **中等模型** (100-500ms): `batch_timeout=5.0` (默认)
- **慢速模型** (> 500ms): `batch_timeout=10.0`

## 日志输出

```
INFO: 创建批次 SCHEDULE-STATION-REALTIME-DATA_0_12345_1700000000，期望场站数: 10
INFO: 批次 SCHEDULE-STATION-REALTIME-DATA_0_12345_1700000000 收到场站 S001 结果，进度: 1/10
INFO: 批次 SCHEDULE-STATION-REALTIME-DATA_0_12345_1700000000 收到场站 S002 结果，进度: 2/10
...
WARNING: 批次 SCHEDULE-STATION-REALTIME-DATA_0_12345_1700000000 超时 (5秒)，已完成 8/10 个场站
INFO: 批次 SCHEDULE-STATION-REALTIME-DATA_0_12345_1700000000 上传结果: 成功=7, 失败=2, 无输出=1
```

## 注意事项

1. **返回值必须是字典**：`callback` 返回的结果必须是 `dict` 类型，或 `None` 表示失败
2. **批次ID唯一性**：批次ID基于消息的 `partition_offset_timestamp`，确保唯一性
3. **上传回调异步**：`batch_upload_handler` 可以是同步或异步函数
4. **结果列表格式**：`results_list` 是所有成功场站的结果字典组成的列表

## 错误处理

```python
async def process_station(station_id, module_input):
    try:
        # 检查数据质量
        data_quality = module_input.get('_data_quality', {})
        if not data_quality.get('available_topics'):
            print(f"场站 {station_id} 数据不完整，跳过处理")
            return None
        
        # 业务处理
        result = do_something(module_input)
        return result
    
    except KeyError as e:
        print(f"场站 {station_id} 缺少必要字段: {e}")
        return None
    
    except Exception as e:
        print(f"场站 {station_id} 处理异常: {e}")
        return None
```

## 监控指标

可以在 `batch_upload_handler` 中记录以下指标：

```python
async def upload_batch(batch_id, results_list):
    metrics = {
        "batch_id": batch_id,
        "success_count": len(results_list),
        "timestamp": time.time()
    }
    
    # 上报到监控系统
    # await metrics_client.report(metrics)
```

## 性能测试

典型性能指标（100个场站，5秒超时）：

- **全部成功**：所有场站在2秒内完成，批次在2秒后上传
- **部分失败**：80个成功，20个失败，批次在5秒后上传
- **极端情况**：10个成功，90个超时，批次在5秒后上传（不阻塞）

## 常见问题

### Q: 如果所有场站都失败了怎么办？

A: `results_list` 会是空列表，可以在 `batch_upload_handler` 中检查并跳过上传。

### Q: 批次超时后，慢速场站的结果还会上传吗？

A: 不会。超时后批次已关闭，后续结果会被忽略。

### Q: 如何调整超时时间？

A: 修改 `BatchResultAggregator(batch_timeout=5.0)` 参数。

### Q: 批次结果是否保证顺序？

A: 不保证。`results_list` 中的顺序取决于场站完成的先后顺序。
