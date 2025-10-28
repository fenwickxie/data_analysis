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
    "SCHEDULE-DEVICE-HOST": DeviceHostParser(),
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
        self.padding_strategy = "zero"  # 默认零填充

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
        # 整合该场站所有topic窗口数据，组装为模块输入，窗口不足自动补零或插值
        def pad_or_interp(seq, target_len, pad_value=0):
            """Pad or interpolate a sequence to target length according to current strategy."""
            n = len(seq)
            if n == 0:
                if self.padding_strategy == "zero":
                    return [0] * target_len
                elif self.padding_strategy == "linear":
                    return [0] * target_len
                elif self.padding_strategy == "forward":
                    return [pad_value] * target_len
                elif self.padding_strategy == "missing":
                    return [None] * target_len
            if n >= target_len:
                return seq[-target_len:]
            if self.padding_strategy == "zero":
                if isinstance(seq[0], (int, float)):
                    return [0] * (target_len - n) + seq
                else:
                    return [0] * (target_len - n) + seq
            elif self.padding_strategy == "linear":
                if isinstance(seq[0], (int, float)):
                    import numpy as np
                    x = np.arange(n)
                    xp = np.linspace(0, n-1, target_len)
                    y = np.array(seq)
                    return list(np.interp(xp, x, y))
                else:
                    return [seq[0]] * (target_len - n) + seq
            elif self.padding_strategy == "forward":
                return [seq[0]] * (target_len - n) + seq
            elif self.padding_strategy == "missing":
                return [None] * (target_len - n) + seq
            return seq
        
        try:
            with self.lock:
                if station_id not in self.data_cache:
                    return None
                input_data = {}
                for topic in MODULE_TO_TOPICS.get(module, []):
                    window = self.get_topic_window(station_id, topic)
                    win_size = TOPIC_DETAIL.get(topic, {}).get("window_size", 1)
                    if window:
                        try:
                            parsed_list = [
                                TOPIC_PARSER_MAP[topic].parse(d)
                                for d in window
                            ]
                        except Exception as e:
                            handle_error(
                                DispatcherError(e),
                                context=f"topic解析 topic={topic}, station_id={station_id}",
                            )
                            parsed_list = []
                        # 字段聚合
                        field_buf = {}
                        for parsed in parsed_list:
                            for k, v in parsed.items():
                                field_buf.setdefault(k, []).append(v)
                        for k, seq in field_buf.items():
                            padded = pad_or_interp(seq, win_size)
                            input_data[f"{k}_window"] = padded
                            input_data[k] = padded[-1] if padded else None
                    else:
                        # 全部补零
                        for f in TOPIC_DETAIL.get(topic, {}).get("fields", []):
                            input_data[f"{f}_window"] = [0] * win_size
                            input_data[f] = 0
                # 依赖其他模块输出（通过Kafka topic获取）
                deps = MODULE_DEPENDENCIES.get(module, [])
                for dep in deps:
                    output_topic = MODULE_OUTPUT_TOPICS.get(dep)
                    if not output_topic:
                        continue
                    dep_window = self.get_topic_window(station_id, output_topic)
                    if not dep_window:
                        continue
                    parser = TOPIC_PARSER_MAP.get(output_topic)
                    try:
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
                    window_size = TOPIC_DETAIL.get(output_topic, {}).get(
                        "window_size", len(parsed_values)
                    )
                    trimmed = parsed_values[-window_size:]
                    input_data[f"{dep}_output_window"] = trimmed
                    latest = trimmed[-1]
                    if isinstance(latest, dict):
                        input_data.update(latest)
                    else:
                        input_data[f"{dep}_output"] = latest
                try:
                    return self.parsers[module].parse(input_data)
                except Exception as e:
                    handle_error(
                        DispatcherError(e),
                        context=f"模块解析 module={module}, station_id={station_id}",
                    )
                    return None
        except Exception as e:
            handle_error(
                DispatcherError(e),
                context=f"模块输入组装 module={module}, station_id={station_id}",
            )
            return None

    def get_all_inputs(self, station_id):
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
                    # 移除过期数据
                    while dq and now - dq[0][1] > self.data_expire_seconds:
                        dq.popleft()
                # 如果所有topic都无数据则移除场站
                if all(len(dq) == 0 for dq in topic_map.values()):
                    expired_stations.append(station_id)
            for sid in expired_stations:
                del self.data_cache[sid]
