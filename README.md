# 数据解析模块 data_analysis

本模块负责从Kafka获取各topic数据，解析、加工成各业务模块所需格式，并提供统一的数据分发接口。

## 目录结构

- data_analysis/
  - __init__.py
  - config.py           # 配置文件，Kafka、topic、模块依赖等
  - kafka_client.py     # Kafka消费封装
  - kafka_producer.py   # Kafka生产封装
  - parser_base.py      # 解析器基类
  - topic_parsers/      # 各topic原始数据解析器
      - __init__.py
      - station_param.py
      - station_realtime_data.py
      - environment_calendar.py
      - device_meter.py
      - device_gun.py
      - car_order.py
      - car_price.py
      - device_error.py
      - device_host.py
      - device_storage.py
  - parsers/            # 各业务模块数据整合器
      - __init__.py
      - electricity_price_parser.py
      - load_prediction_parser.py
      - pv_prediction_parser.py
      - thermal_management_parser.py
      - station_guidance_parser.py
      - evaluation_model_parser.py
      - SOH_model_parser.py
      - operation_optimization_parser.py
      - customer_mining_parser.py
  - dispatcher.py       # 数据分发与依赖处理
  - main.py             # 启动入口

## 主要功能
- Kafka消费与topic订阅，topic与模块多对多映射
- topic原始数据解析与整合
- 跨模块数据依赖处理
- 对外统一数据分发接口，支持上传到Kafka
- 易于扩展和云平台部署

## topic与模块映射

| topic                        | 字段（部分） | 推送频率 | 需求模块 |
|------------------------------|--------------|----------|----------|
| SCHEDULE-STATION-PARAM       | station_id, station_temp, ... | 新建/变更 | load_prediction, operation_optimization, electricity_price, SOH_model |
| SCHEDULE-STATION-REALTIME-DATA | station_id, gun_id, ... | 1分钟1次 | load_prediction, operation_optimization, electricity_price, SOH_model, thermal_management, evaluation_model |
| SCHEDULE-ENVIRONMENT-CALENDAR | workday_code, holiday_code | 1年1次 | load_prediction, operation_optimization, electricity_price, SOH_model |
| SCHEDULE-DEVICE-METER        | meter_id, current_power, ... | 5分钟1次 | operation_optimization, electricity_price |
| SCHEDULE-DEVICE-GUN          | host_id, gun_id, gun_status | 15秒1次 | operation_optimization |
| SCHEDULE-CAR-ORDER           | station_id, order_id, ... | 1秒1次 | load_prediction, operation_optimization, station_guidance, electricity_price, evaluation_model |
| SCHEDULE-CAR-PRICE           | station_id, period_no, ... | 1月1次 | operation_optimization, electricity_price, evaluation_model, thermal_management |
| SCHEDULE-DEVICE-ERROR        | station_id, host_error, ... | 触发推送 | operation_optimization, SOH_model |
| SCHEDULE-DEVICE-HOST         | host_id, acdc_status, ... | 1秒/15秒 | evaluation_model, thermal_management |
| SCHEDULE-DEVICE-STORAGE      | host_id, storage_id, ... | 15秒1次 | evaluation_model, thermal_management, electricity_price, operation_optimization |


## 数据流转说明

1. data_analysis模块从Kafka消费所有topic，按topic类型用topic_parsers解析原始数据。
2. dispatcher按场站缓存所有topic数据，自动整合为各业务模块所需结构（见parsers/）。
3. 各业务模块输入数据可上传到Kafka，供下游模型直接消费。
4. 支持多场站并发、内存自动清理、易于扩展。


## 嵌入式用法示例

### 1. 同步用法（适合传统多线程/阻塞模型）

```python
from data_analysis import DataAnalysisService

def my_module_callback(station_id, outputs):
  # outputs为该场站所有业务模块输入数据
  # 例如：outputs['load_prediction']、outputs['electricity_price']等
  do_something(outputs['load_prediction'])

service = DataAnalysisService()
service.start(callback=my_module_callback)
# ...业务主循环...
service.stop()
```

如需独立运行，直接执行main.py即可。

### 2. 异步用法（推荐，适合高并发/云原生场景）

```python
import asyncio
from data_analysis.async_service import AsyncDataAnalysisService

async def my_model_predict(module_input):
  # 这里写模型推理逻辑
  await asyncio.sleep(0.01)  # 模拟异步推理
  return {"result": 123}

async def my_callback(station_id, module_input):
  # module_input为本模型所需结构化输入（含窗口补全/插值、依赖聚合）
  result = await my_model_predict(module_input)
  return result  # 返回值会自动上传到Kafka

async def main():
  service = AsyncDataAnalysisService(module_name="load_prediction")
  await service.start(callback=my_callback)
  try:
    for _ in range(10):  # 模拟主循环10次
      print(service.get_station_status())
      await asyncio.sleep(1)
  finally:
    await service.stop()

if __name__ == "__main__":
  asyncio.run(main())
```

如需独立运行，直接执行 async_main.py。

---

## 同步/异步API兼容性与迁移说明

### 1. API兼容性
- 同步API（DataAnalysisService）适合传统阻塞/多线程场景，接口为阻塞式回调。
- 异步API（AsyncDataAnalysisService）基于asyncio，支持高并发、异步回调、异步Kafka、异步上传。
- 两者均支持多场站、窗口补全/插值、依赖聚合、自动上传Kafka、健康监控、配置热更新。

### 2. 迁移与混用建议
- 推荐新项目/云原生/高并发场景优先使用异步API。
- 迁移方式：
  - 回调函数由同步def改为async def，内部可await模型推理/IO。
  - 服务启动/停止由同步start/stop改为await service.start/stop。
  - Kafka、窗口补全、依赖聚合等用法保持一致。
- 同步/异步不可直接混用（如同一场站/同一Kafka group），如需混用请确保topic、group_id、端口等配置隔离。

### 3. 注意事项
- 异步API需在asyncio事件循环中运行。
- 异步回调支持await任意异步操作，适合模型推理、远程调用等高并发场景。
- 同步API适合遗留系统或对异步无需求场景。
- 日志、健康监控、配置热更新等功能同步/异步均支持。

如需更多用法示例、测试用例，请参考 data_analysis/async_main.py、tests/test_async_service.py。
