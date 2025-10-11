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
