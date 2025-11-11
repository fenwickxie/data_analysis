#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:09:54
project: data_analysis
filename: dispatcher.py
version: 1.0
"""

# 数据分发与依赖处理
from .config import (
    MODULE_DEPENDENCIES,
    MODULE_OUTPUT_TOPICS,
    MODULE_TO_TOPICS,
    TOPIC_DETAIL,
    TOPIC_TO_MODULES,
)
from .parsers import *


import threading
import time
import logging
from collections import deque
from .errors import handle_error, DispatcherError

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s"
)
from .topic_parsers import *

TOPIC_PARSER_MAP = {
    "SCHEDULE-STATION-PARAM": StationParamParser(),
    "SCHEDULE-STATION-REALTIME-DATA": StationRealtimeDataParser(),
    "SCHEDULE-ENVIRONMENT-CALENDAR": EnvironmentCalendarParser(),
    "SCHEDULE-DEVICE-METER": DeviceMeterParser(),
    "SCHEDULE-DEVICE-GUN": DeviceGunParser(),
    "SCHEDULE-CAR-ORDER": CarOrderParser(),
    "SCHEDULE-CAR-PRICE": CarPriceParser(),
    "SCHEDULE-DEVICE-ERROR": DeviceErrorParser(),
    "SCHEDULE-DEVICE-HOST-DCDC": DeviceHostDCDCParser(),
    "SCHEDULE-DEVICE-HOST-ACDC": DeviceHostACDCParser(),
    "SCHEDULE-DEVICE-STORAGE": DeviceStorageParser(),
}

for module_name, topic_name in MODULE_OUTPUT_TOPICS.items():
    if topic_name not in TOPIC_PARSER_MAP:
        TOPIC_PARSER_MAP[topic_name] = ModelOutputParser(module_name)


class DataDispatcher:
    """
    数据分发与依赖处理核心类。
    
    该类负责管理多场站数据窗口缓存，处理模块间依赖关系，
    并将多topic数据整合为各业务模块所需格式。支持多种数据补全策略，
    并提供动态配置更新功能。
    
    主要功能：
    - 多场站数据窗口管理
    - 数据补全/插值处理
    - 模块间依赖处理
    - 动态配置热更新
    - 数据清理和过期处理
    
    Attributes:
        data_cache (dict): 多场站窗口缓存，格式为 {station_id: {topic: deque[(data, ts)]}}
        data_expire_seconds (int): 数据过期时间(秒)
        lock (threading.Lock): 线程锁，确保线程安全
        padding_strategy (str): 窗口补全策略，默认为"zero"
        parsers (dict): 业务模块解析器映射
    """

    def reload_config(self, config_mod):
        """
        热加载config模块，动态更新topic、窗口、依赖等。
        """
        global TOPIC_DETAIL, TOPIC_PARSER_MAP, MODULE_DEPENDENCIES, MODULE_TO_TOPICS
        TOPIC_DETAIL = config_mod.TOPIC_DETAIL
        MODULE_DEPENDENCIES = config_mod.MODULE_DEPENDENCIES
        MODULE_TO_TOPICS = config_mod.MODULE_TO_TOPICS
        # 解析器映射如有变动可重建
        # 这里只重建窗口长度等配置
        logging.info("DataDispatcher配置热更新完成")

    def set_padding_strategy(self, strategy):
        """
        设置窗口补全策略。
        strategy: 'zero'（补零）、'linear'（线性插值）、'forward'（前向填充）、'missing'（缺失标记None）
        """
        assert strategy in ("zero", "linear", "forward", "missing")
        self.padding_strategy = strategy

    def __init__(self, data_expire_seconds=600):
        """
        初始化数据分发器，设置解析器和缓存参数。
        
        创建分发器实例时会初始化所有业务模块解析器，
        设置数据缓存结构和默认补全策略。
        
        Args:
            data_expire_seconds (int, optional): 数据过期时间(秒)，默认为600。
                超过此时间未更新的场站数据将被自动清理。
        """
        self.parsers = {
            "electricity_price": ElectricityPriceParser(),
            "load_prediction": LoadPredictionParser(),
            "pv_prediction": PvPredictionParser(),
            "thermal_management": ThermalManagementParser(),
            "station_guidance": StationGuidanceParser(),
            "evaluation_model": EvaluationModelParser(),
            "SOH_model": SOHModelParser(),
            "operation_optimization": OperationOptimizationParser(),
            "customer_mining": CustomerMiningParser(),
        }
        # 多场站窗口缓存: {station_id: {topic: deque[(data, ts)]}}
        self.data_cache = {}
        self.data_expire_seconds = data_expire_seconds
        self.lock = threading.RLock()

    def update_topic_data(self, station_id, topic, raw_data):
        """
        更新指定场站、topic的数据窗口。
        
        将新数据添加到对应场站和topic的数据窗口中，如果窗口已满，
        会自动移除最旧的数据。此方法线程安全，可在多线程环境下使用。
        
        Args:
            station_id (str): 场站ID
            topic (str): topic名称
            raw_data (dict): 原始数据
            
        Raises:
            DispatcherError: 当数据更新失败时抛出
        """
        try:
            with self.lock:
                if station_id not in self.data_cache:
                    self.data_cache[station_id] = {}
                if topic not in self.data_cache[station_id]:
                    win_size = TOPIC_DETAIL.get(topic, {}).get("window_size", 1)
                    self.data_cache[station_id][topic] = deque(maxlen=win_size)
                self.data_cache[station_id][topic].append((raw_data, time.time()))
        except Exception as e:
            handle_error(
                DispatcherError(e),
                context=f"窗口数据更新 station_id={station_id}, topic={topic}",
            )

    def get_topic_window(self, station_id, topic):
        # 获取窗口数据（只返回data部分，按时间升序）
        if (
            station_id not in self.data_cache
            or topic not in self.data_cache[station_id]
        ):
            return []
        return [item[0] for item in self.data_cache[station_id][topic]]

    def get_module_input(self, station_id, module):
        """
        整合该场站所有topic窗口数据，组装为模块输入（在这里完成解析）。
        
        说明：
        - 对于事件驱动模式，当最快频率topic更新时触发处理
        - 慢速topic自动使用其最新缓存数据（不管数据新旧）
        - 只需确保topic有数据即可，不检查数据新鲜度
        
        Args:
            station_id (str): 场站ID
            module (str): 模块名称
            
        Returns:
            dict: 模块输入数据，包含：
                - 业务字段：由各topic parser解析生成
                - _data_quality: 数据可用性信息
                    - available_topics: 有数据的topic列表
                    - missing_topics: 无数据的topic列表
                    - availability_ratio: 数据可用率 (0.0-1.0)
        """
        try:
            # 使用线程锁确保线程安全
            with self.lock:
                # 检查场站ID是否存在于缓存中
                if station_id not in self.data_cache:
                    return None
                
                # 初始化输入数据字典，包含场站ID
                input_data = {'stationId': station_id}
                
                # 数据可用性元信息（简化版：只关注是否有数据）
                data_quality = {
                    'available_topics': [],     # 有数据的topic
                    'missing_topics': [],       # 无数据的topic
                    'total_topics': 0,
                    'availability_ratio': 0.0   # 数据可用率
                }
                
                # 处理模块所需的topic数据
                required_topics = MODULE_TO_TOPICS.get(module, [])
                data_quality['total_topics'] = len(required_topics)
                
                for topic in required_topics:
                    # 获取指定topic的窗口数据
                    window = self.get_topic_window(station_id, topic)
                    
                    if window:
                        # 有数据即可（不管是最新的还是缓存的）
                        data_quality['available_topics'].append(topic)
                        
                        # 调用topic parser的parse_window方法
                        # 各topic parser可以实现自己的窗口数据处理逻辑
                        parser = TOPIC_PARSER_MAP.get(topic)
                        if parser:
                            try:
                                # 解析窗口数据并更新到输入数据中
                                parsed_data = parser.parse_window(window) # 完成枪号对齐、插值、格式化等所有处理
                                if parsed_data:
                                    input_data.update(parsed_data)
                            except Exception as e:
                                # 处理解析错误
                                handle_error(
                                    DispatcherError(e),
                                    context=f"解析窗口数据 topic={topic}, station_id={station_id}",
                                )
                    else:
                        # 窗口为空，没有任何数据
                        data_quality['missing_topics'].append(topic)
                        
                        # 添加空列表
                        fields = TOPIC_DETAIL.get(topic, {}).get("fields", [])
                        for field in fields:
                            input_data[field] = []
                        input_data['sendTime'] = []
                
                # 依赖其他模块输出（通过Kafka topic获取）
                deps = MODULE_DEPENDENCIES.get(module, [])
                for dep in deps:
                    # 获取依赖模块的输出topic
                    output_topic = MODULE_OUTPUT_TOPICS.get(dep)
                    if not output_topic:
                        continue
                    dep_window = self.get_topic_window(station_id, output_topic)
                    if not dep_window:
                        continue
                    parser = TOPIC_PARSER_MAP.get(output_topic)
                    try:
                        if parser and hasattr(parser, 'parse_window'):
                            # 使用parse_window处理窗口数据
                            parsed_values = parser.parse_window(dep_window)
                        else:
                            # 回退到逐条解析
                            parsed_values = (
                                [parser.parse(d) for d in dep_window] if parser else list(dep_window)
                            )
                    except Exception as e:
                        handle_error(
                            DispatcherError(e),
                            context=f"依赖topic解析 module={module}, dep={dep}, topic={output_topic}, station_id={station_id}",
                        )
                        parsed_values = []
                    if not parsed_values:
                        continue
                    
                    input_data[f"{dep}_output"] = parsed_values
                
                # 计算数据可用率
                available_count = len(data_quality['available_topics'])
                total_count = data_quality['total_topics']
                data_quality['availability_ratio'] = (
                    available_count / total_count if total_count > 0 else 0.0
                )
                
                # 将数据可用性信息添加到返回结果（业务模块可根据此信息决定是否处理）
                input_data['_data_quality'] = data_quality
                
                # 调用模块解析器（如果需要进一步处理）
                try:
                    return self.parsers[module].parse(input_data)
                except Exception as e:
                    handle_error(
                        DispatcherError(e),
                        context=f"模块解析 module={module}, station_id={station_id}",
                    )
                    return input_data  # 解析失败时返回原始input_data
                    
        except Exception as e:
            handle_error(
                DispatcherError(e),
                context=f"模块输入组装 module={module}, station_id={station_id}",
            )
            return None

    def get_all_inputs(self, station_id):
        """获取场站的所有模块输入（触发解析）"""
        with self.lock:
            if station_id not in self.data_cache:
                return {}
            return {
                m: self.get_module_input(station_id, module=m) for m in self.parsers.keys()
            }

    def clean_expired(self):
        now = time.time()
        with self.lock:
            expired_stations = []
            for station_id, topic_map in self.data_cache.items():
                for t, dq in topic_map.items():
                    # 移除过期数据，保留最新窗口
                    while dq and now - dq[-1][1] > self.data_expire_seconds:
                        dq.clear()
                        break
                # 如果所有topic都无数据则移除场站
                if all(len(dq) == 0 for dq in topic_map.values()):
                    expired_stations.append(station_id)
            for sid in expired_stations:
                del self.data_cache[sid]
