#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:09:43
project: data_analysis
filename: config.py
version: 1.0
"""

# 配置文件：Kafka、topic、模块依赖


KAFKA_CONFIG = {
    'bootstrap_servers': ['10.8.4.40:35888'],
    'consumer': {
        'group_id': 'stack-charge-tcp-command-xfy',  # ⚠️ 如果从单消费者模式切换到多消费者模式，建议修改为新的 group_id（如：xxx-v2）避免读取旧的过期 offset
        'auto_offset_reset': 'latest', # 'earliest' 或 'latest'，默认从最新消息开始消费
        'key_deserializer': 'org.apache.kafka.common.serialization.StringDeserializer',
        'value_deserializer': 'org.apache.kafka.common.serialization.StringDeserializer',
        # 多消费者模式配置
        'multi_consumer_mode': True,  # 启用多消费者模式：为每个topic创建独立消费者，解决消息积压时的topic饥饿问题
        'max_poll_records': 10,      # 多消费者模式下：每个消费者的拉取上限；单消费者模式下：所有topic的总拉取上限
        'enable_auto_commit': False,  # 关闭自动提交，由服务统一管理
        # 字节数限制
        "max_partition_fetch_bytes": 100 * 1024 * 1024,   # 单分区100MB
        "fetch_max_bytes": 500 * 1024 * 1024,            # 单次请求500MB
        "fetch_max_wait_ms": 500,                        # 最多等待500ms
        "fetch_min_bytes": 1,                            # 至少1字节就返回
        # # 增加会话超时，避免 rebalance
        # "session_timeout_ms": 60000, 
        # "max_poll_interval_ms": 600000,
    },
    'listener': {
        'ack-mode': 'manual',
        'type': 'batch',
        'missing-topics-fatal': False,
    },
    'producer': {
        'key_deserializer': 'org.apache.kafka.common.serialization.StringDeserializer',
        'value_deserializer': 'org.apache.kafka.common.serialization.StringDeserializer',

    }
}

# Offset管理配置
OFFSET_COMMIT_CONFIG = {
    'commit_interval_seconds': 5.0,  # 定时提交间隔（秒）
    'commit_batch_size': 100,        # 累积消息数提交阈值
    'max_commit_retries': 3,         # 提交失败重试次数
    'commit_retry_delay': 1.0,       # 重试延迟（秒）
}

# topic详细配置
TOPIC_DETAIL = {
    'SCHEDULE-STATION-PARAM': {
        'fields': ['stationId', 'stationLng', 'stationLat', 'gunNum', 'gridCapacity', 'meterId','powerNum','normalClap', 'hostCode'],
        'frequency': '新建站或配置更改时',
        'modules': ['load_prediction', 'operation_optimization', 'electricity_price', 'SOH_model'],
        'window_size': 1
    },
    'SCHEDULE-STATION-REALTIME-DATA': {
        'fields': ['stationId', 'gunNo', 'outputPowerPerGunMax', 'outputPowerPerGunAvg', 'outputPowerPerStationMax', 'outputPowerPerStationAvg'],
        'frequency': '1小时1次，推送7天',
        'modules': ['load_prediction', 'electricity_price', 'SOH_model', 'thermal_management', 'evaluation_model'],
        'window_size': 24*7
    },
    'SCHEDULE-ENVIRONMENT-CALENDAR': {
        'fields': ['dayOfWeek', 'holiday'],
        'frequency': '1年1次',
        'modules': ['load_prediction', 'electricity_price', 'SOH_model'],
        'window_size': 1
    },
    'SCHEDULE-DEVICE-METER': {
        'fields': ['meterId', 'meterPower', 'rmeterLimitPower'],
        'frequency': '5分钟1次',
        'modules': ['electricity_price'],
        'window_size': 1
    },
    'SCHEDULE-DEVICE-GUN': {
        'fields': ['hostCode', 'gunNo', 'status'],
        'frequency': '15秒1次',
        'modules': [],
        'window_size': 1
    },
    'SCHEDULE-CAR-ORDER': {
        'fields': ['stationId', 'transactionSerialNo', 'hostCode', 'gunNo', 'terminalMaxOutElectric', 'startChargeTime', 'endChargeTime', 'beginSOC', 'soc', 'terminalRequireVoltage', 'terminalRequireElectric', 'outputPower', 'carProducerCode', 'batteryNominalTotalCapacity'],
        'frequency': '1秒1次',
        'modules': ['operation_optimization', 'station_guidance', 'electricity_price', 'evaluation_model'],
        'window_size': 2  # 需要保留2秒数据：当前秒（聚合中）+ 上一秒（已完成，待处理）
    },
    'SCHEDULE-CAR-PRICE': {
        'fields': ['stationId', 'FeeNo', 'startTime', 'endTime', 'periodType', 'gridPrice', 'serviceFee'],
        'frequency': '1月1次',
        'modules': ['operation_optimization', 'electricity_price', 'evaluation_model', 'thermal_management'],
        'window_size': 1
    },
    'SCHEDULE-DEVICE-ERROR': {
        'fields': ['stationId', 'hostError', 'acError', 'dcError', 'terminalError', 'storageError'],
        'frequency': '触发推送',
        'modules': ['SOH_model'],
        'window_size': 1
    },
    'SCHEDULE-DEVICE-HOST-DCDC': {
        'fields': ['hostCode', 'dcWorkStatus', 'dcPower'],
        'frequency': '充电时1秒1次，非充电15秒1次',
        'modules': ['evaluation_model', 'thermal_management', 'operation_optimization'],
        'window_size': 1
    },
    'SCHEDULE-DEVICE-HOST-ACDC': {
        'fields': ['hostCode', 'acPower'],
        'frequency': '充电时1秒1次，非充电15秒1次',
        'modules': ['evaluation_model', 'thermal_management'],
        'window_size': 1
    },
    'SCHEDULE-DEVICE-STORAGE': {
        'fields': ['hostId', 'storageId', 'storagePower', 'storageCurrent', 'storageTempMax', 'storageTempMin', 'storageSOC', 'storageSOH'],
        'frequency': '15秒1次',
        'modules': ['evaluation_model', 'thermal_management', 'electricity_price', 'operation_optimization'],
        'window_size': 1
    },
    'SCHEDULE-DEVICE-PV':{
        'fields': ['stationId', 'pvId', 'pvPreDcPower'],
        'frequency': '一天1次',
        'modules': ['pv_prediction'],
        'window_size': 1
    },
    'SCHEDULE-ENVIRONMENT-WEATHER': {
        'fields':['weatherSituationYesterday','seasonTomorrow','weatherSituationTomorrow'],
        'frequency': '一天1次',
        'modules': ['pv_prediction'],
        'window_size': 1
    }
}

# 各模块间依赖关系
MODULE_DEPENDENCIES = {
    'electricity_price': ['pv_prediction', 'evaluation_model', 'SOH_model'],
    'station_guidance': ['load_prediction', 'evaluation_model'],
    'thermal_management': ['load_prediction', 'operation_optimization'],
    'operation_optimization': ['pv_prediction'],
    # 其他模块依赖可扩展
}

# 各业务模块输出的Kafka topic映射（默认使用服务中的输出前缀）
MODULE_OUTPUT_TOPIC_PREFIX = "MODULE-OUTPUT-"
MODULE_OUTPUT_TOPICS = {
    'electricity_price': f"{MODULE_OUTPUT_TOPIC_PREFIX}ELECTRICITY_PRICE",
    'load_prediction': f"{MODULE_OUTPUT_TOPIC_PREFIX}LOAD_PREDICTION",
    'pv_prediction': f"{MODULE_OUTPUT_TOPIC_PREFIX}PV_PREDICTION",
    'thermal_management': f"{MODULE_OUTPUT_TOPIC_PREFIX}THERMAL_MANAGEMENT",
    'station_guidance': f"{MODULE_OUTPUT_TOPIC_PREFIX}STATION_GUIDANCE",
    'evaluation_model': f"{MODULE_OUTPUT_TOPIC_PREFIX}EVALUATION_MODEL",
    'SOH_model': f"{MODULE_OUTPUT_TOPIC_PREFIX}SOH_MODEL",
    'operation_optimization': f"{MODULE_OUTPUT_TOPIC_PREFIX}OPERATION_OPTIMIZATION",
    'customer_mining': f"{MODULE_OUTPUT_TOPIC_PREFIX}CUSTOMER_MINING",
}

# 默认缓存最近1条模型输出，供依赖窗口使用
MODULE_OUTPUT_WINDOW_SIZE = 1

for module_name, output_topic in MODULE_OUTPUT_TOPICS.items():
    consumers = [
        consumer
        for consumer, deps in MODULE_DEPENDENCIES.items()
        if module_name in deps
    ]
    TOPIC_DETAIL[output_topic] = {
        'fields': [],
        'frequency': '模型输出',
        'modules': consumers,
        'window_size': MODULE_OUTPUT_WINDOW_SIZE,
    }

# topic到模块的多对多映射
TOPIC_TO_MODULES = {topic: v['modules'] for topic, v in TOPIC_DETAIL.items()}
# 模块到topic的多对多映射
MODULE_TO_TOPICS = {}
for topic, v in TOPIC_DETAIL.items():
    for m in v['modules']:
        MODULE_TO_TOPICS.setdefault(m, []).append(topic)

# 确保所有模块在映射中至少有空列表，便于后续查找
for module_name in MODULE_OUTPUT_TOPICS.keys():
    MODULE_TO_TOPICS.setdefault(module_name, [])
