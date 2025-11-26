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
    "SCHEDULE-ENVIRONMENT-WEATHER": EnvironmentWeatherParser(),
    "SCHEDULE-DEVICE-PV": DevicePvParser(),
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

    def __init__(self, data_expire_seconds=600, enable_data_expiration=True):
        """
        初始化数据分发器，设置解析器和缓存参数。

        创建分发器实例时会初始化所有业务模块解析器，
        设置数据缓存结构和默认补全策略。

        Args:
            data_expire_seconds (int, optional): 数据过期时间(秒)，默认为600。
                超过此时间未更新的场站数据将被自动清理。
            enable_data_expiration (bool, optional): 是否启用数据过期清理，默认为True。
                设置为False时将不会自动清理过期数据。
        """
        # 初始化解析器映射,经各topic parser梳理后的数据输入模块后的特殊处理逻辑，一般不需调用
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
        self.enable_data_expiration = enable_data_expiration
        self.lock = threading.RLock()

        # Topic更新策略映射
        # 不再需要预定义集合，改为直接映射到处理方法
        self._topic_updaters = self._build_topic_updaters()

    def _build_topic_updaters(self):
        """
        构建 topic 到更新方法的映射

        每个 topic 独立更新方法，直观明确，易于扩展

        Returns:
            dict: {topic: updater_function}
        """
        updaters = {
            "SCHEDULE-STATION-PARAM": self._update_station_param,
            "SCHEDULE-STATION-REALTIME-DATA": self._update_station_realtime_data,
            "SCHEDULE-ENVIRONMENT-CALENDAR": self._update_environment_calendar,
            "SCHEDULE-DEVICE-METER": self._update_device_meter,
            "SCHEDULE-DEVICE-GUN": self._update_device_gun,
            "SCHEDULE-CAR-ORDER": self._update_car_order,
            "SCHEDULE-CAR-PRICE": self._update_car_price,
            "SCHEDULE-DEVICE-ERROR": self._update_device_error,
            "SCHEDULE-DEVICE-HOST-DCDC": self._update_device_host_dcdc,
            "SCHEDULE-DEVICE-HOST-ACDC": self._update_device_host_acdc,
            "SCHEDULE-DEVICE-STORAGE": self._update_device_storage,
        }

        # 自动添加模型输出topic更新器
        for module_name, topic_name in MODULE_OUTPUT_TOPICS.items():
            if topic_name not in updaters:
                updaters[topic_name] = self._update_model_output

        return updaters

    # ==================== Topic 更新器：每个 topic 独立方法 ====================

    def _update_station_param(self, station_id, topic, raw_data, timestamp):
        """
        更新 SCHEDULE-STATION-PARAM
        特点：window_size=1，单条数据，直接追加
        """
        self.data_cache[station_id][topic].append((raw_data, timestamp))
        return True

    def _update_station_realtime_data(self, station_id, topic, raw_data, timestamp):
        """
        更新 SCHEDULE-STATION-REALTIME-DATA
        特点：window_size > 1，批量窗口数据，直接替换整个窗口
        raw_data 是列表，包含多条记录（已按时间排序）
        """
        # 替换整个窗口（已按时间排序）
        self.data_cache[station_id][topic].clear()
        for item in raw_data:
            self.data_cache[station_id][topic].append((item, timestamp))
        return True

    def _update_environment_calendar(self, station_id, topic, raw_data, timestamp):
        """
        更新 SCHEDULE-ENVIRONMENT-CALENDAR
        特点：全局数据，window_size=1，直接追加
        """
        self.data_cache[station_id][topic].append((raw_data, timestamp))
        return True

    def _update_environment_weather(self, station_id, topic, raw_data, timestamp):
        """
        更新 SCHEDULE-ENVIRONMENT-WEATHER
        特点：天气数据分厂站，window_size=1，直接追加
        """
        self.data_cache[station_id][topic].append((raw_data, timestamp))
        return True

    def _update_device_meter(self, station_id, topic, raw_data, timestamp):
        """
        更新 SCHEDULE-DEVICE-METER
        特点：window_size=1，单条数据，直接追加
        """
        self.data_cache[station_id][topic].append((raw_data, timestamp))
        return True

    def _update_device_pv(self, station_id, topic, raw_data, timestamp):
        """
        更新 SCHEDULE-DEVICE-PV
        特点：window_size=1，单条数据，直接追加
        """
        self.data_cache[station_id][topic].append((raw_data, timestamp))
        return True

    def _update_device_gun(self, station_id, topic, raw_data, timestamp):
        """
        更新 SCHEDULE-DEVICE-GUN
        特点：window_size=1，单条数据，直接追加
        """
        self.data_cache[station_id][topic].append((raw_data, timestamp))
        return True

    def _update_car_order(self, station_id, topic, raw_data, timestamp):
        """
        更新 SCHEDULE-CAR-ORDER
        特点：window_size=2，订单聚合，同一秒聚合多个订单

        工作流程：
        1. 同一秒内：聚合订单到 deque[-1]，不触发事件
        2. 进入新的一秒：
           - 触发事件（通知业务模块处理 deque[-2]，即上一秒已完成的聚合数据）
           - append 新订单到 deque（成为 deque[-1]）
           - 旧数据自动保留在 deque[-2]，直到再次进入新的一秒

        Returns:
            bool: 是否应该触发数据就绪事件
        """
        should_trigger = False

        if self.data_cache[station_id][topic]:
            # 检查最后一条数据的时间戳（秒级）
            last_data, last_ts = self.data_cache[station_id][topic][-1]

            # 如果时间戳相同（秒级），聚合数据
            if int(last_ts) == int(timestamp):
                # 将 last_data 转换为列表（如果还不是）
                if not isinstance(last_data, list):
                    last_data = [last_data]

                # 添加新订单
                last_data.append(raw_data)

                # 更新最后一条记录
                self.data_cache[station_id][topic][-1] = (last_data, timestamp)

                # 同一秒内聚合，不触发事件
                should_trigger = False
            else:
                # 时间戳不同，说明进入新的一秒
                should_trigger = True

                # 添加新记录（包装为列表）
                self.data_cache[station_id][topic].append(([raw_data], timestamp))
        else:
            # 窗口为空，初始化（包装为列表）
            self.data_cache[station_id][topic].append(([raw_data], timestamp))

            # 第一条订单，触发事件（可能是唯一的订单）
            should_trigger = True

        return should_trigger

    def _update_car_price(self, station_id, topic, raw_data, timestamp):
        """
        更新 SCHEDULE-CAR-PRICE
        特点：window_size=1，单条数据，直接追加
        """
        self.data_cache[station_id][topic].append((raw_data, timestamp))
        return True

    def _update_device_error(self, station_id, topic, raw_data, timestamp):
        """
        更新 SCHEDULE-DEVICE-ERROR
        特点：window_size=1，单条数据，直接追加
        """
        self.data_cache[station_id][topic].append((raw_data, timestamp))
        return True

    def _update_device_host_dcdc(self, station_id, topic, raw_data, timestamp):
        """
        更新 SCHEDULE-DEVICE-HOST-DCDC
        特点：场站内多个 DCDC 主机设备，每个设备1秒间隔独立上传，需要聚合

        数据结构：
        deque([{
            'host_id_1': (data1, timestamp1),
            'host_id_2': (data2, timestamp2),
            ...
        }], maxlen=1)

        工作流程：
        1. 每个 DCDC 主机设备消息到达时，更新该设备在聚合字典中的数据
        2. 每次更新都触发事件（业务模块会获取所有主机设备的最新数据）
        3. 设备每1秒上传一次，自动覆盖旧数据

        Args:
            raw_data: {'stationId': '...', 'hostCode': '...', 'dcWorkStatus': [...], ...}

        Returns:
            bool: True（每次设备更新都触发事件）
        """
        try:
            host_id = raw_data.get("hostCode")
            if not host_id:
                logging.warning(f"DCDC 主机数据缺少 hostCode: {raw_data}")
                return False

            # 获取当前场站的 DCDC 主机聚合字典
            if not self.data_cache[station_id][topic]:
                # 首次初始化：创建空字典
                self.data_cache[station_id][topic].append({})

            # 获取当前聚合字典（引用）
            host_dict = self.data_cache[station_id][topic][0]

            # 更新该主机设备的数据（1秒间隔自动覆盖），引用数据类型修改，会直接修改缓存中的字典
            host_dict[host_id] = (raw_data, timestamp)

            # 每次有主机设备更新都触发事件
            return True

        except Exception as e:
            logging.error(
                f"更新 DCDC 主机数据失败 station_id={station_id}, hostCode={raw_data.get('hostCode')}: {e}"
            )
            return False

    def _update_device_host_acdc(self, station_id, topic, raw_data, timestamp):
        """
        更新 SCHEDULE-DEVICE-HOST-ACDC
        特点：场站内多个 ACDC 主机设备，每个设备1秒间隔独立上传，需要聚合

        数据结构：
        deque([{
            'host_id_1': (data1, timestamp1),
            'host_id_2': (data2, timestamp2),
            ...
        }], maxlen=1)

        工作流程：
        1. 每个 ACDC 主机设备消息到达时，更新该设备在聚合字典中的数据
        2. 每次更新都触发事件（业务模块会获取所有主机设备的最新数据）
        3. 设备每1秒上传一次，自动覆盖旧数据

        Args:
            raw_data: {'stationId': '...', 'hostCode': '...', 'acPower': ..., ...}

        Returns:
            bool: True（每次设备更新都触发事件）
        """
        try:
            host_id = raw_data.get("hostCode")
            if not host_id:
                logging.warning(f"ACDC 主机数据缺少 hostCode: {raw_data}")
                return False

            # 获取当前场站的 ACDC 主机聚合字典
            if not self.data_cache[station_id][topic]:
                # 首次初始化：创建空字典
                self.data_cache[station_id][topic].append({})

            # 获取当前聚合字典（引用）
            host_dict = self.data_cache[station_id][topic][0]

            # 更新该主机设备的数据（1秒间隔自动覆盖）
            host_dict[host_id] = (raw_data, timestamp)

            # 每次有主机设备更新都触发事件
            return True

        except Exception as e:
            logging.error(
                f"更新 ACDC 主机数据失败 station_id={station_id}, hostCode={raw_data.get('hostCode')}: {e}"
            )
            return False

    def _update_device_storage(self, station_id, topic, raw_data, timestamp):
        """
        更新 SCHEDULE-DEVICE-STORAGE
        特点：场站内多个储能设备，每个设备15秒间隔独立上传，需要聚合

        数据结构：
        deque([{
            'storage_id_1': (data1, timestamp1),
            'storage_id_2': (data2, timestamp2),
            ...
        }], maxlen=1)

        工作流程：
        1. 每个储能设备消息到达时，更新该设备在聚合字典中的数据
        2. 每次更新都触发事件（业务模块会获取所有储能设备的最新数据）
        3. 设备每15秒上传一次，自动覆盖旧数据

        Args:
            raw_data: {'stationId': '...', 'storageCode': '...', 'batteryGroupSoc': 12, ...}

        Returns:
            bool: True（每次设备更新都触发事件）
        """
        try:
            storage_id = raw_data.get("storageCode")
            if not storage_id:
                logging.warning(f"储能数据缺少 storageCode: {raw_data}")
                return False

            # 获取当前场站的储能聚合字典
            if not self.data_cache[station_id][topic]:
                # 首次初始化：创建空字典
                self.data_cache[station_id][topic].append({})

            # 获取当前聚合字典（引用）
            storage_dict = self.data_cache[station_id][topic][0]

            # 更新该储能设备的数据（15秒间隔自动覆盖）
            storage_dict[storage_id] = (raw_data, timestamp)

            # 每次有储能设备更新都触发事件
            # （业务模块会获取所有储能设备的最新数据）
            return True

        except Exception as e:
            logging.error(
                f"更新储能数据失败 station_id={station_id}, storageCode={raw_data.get('storageCode')}: {e}"
            )
            return False

    def _update_model_output(self, station_id, topic, raw_data, timestamp):
        """
        更新模型输出 topic (MODULE-OUTPUT-*)
        特点：window_size=1，模型输出数据，直接追加
        """
        self.data_cache[station_id][topic].append((raw_data, timestamp))
        return True

    def update_topic_data(self, station_id, topic, raw_data, timestamp=None):
        """
        更新指定场站、topic的数据窗口到缓存中。

        根据 topic 直接调用对应的更新方法。

        Args:
            station_id (str): 场站ID
            topic (str): topic名称
            raw_data (dict or list): 原始数据（单条dict或窗口数据list）
            timestamp (float, optional): Kafka消息时间戳（秒），默认使用当前时间

        Returns:
            bool: 是否应该触发数据就绪事件
                  - True: 数据已完整，可以触发事件通知业务模块
                  - False: 数据聚合中（如订单类同一秒内聚合），暂不触发
        Raises:
            DispatcherError: 当数据更新失败时抛出
        """
        try:
            with self.lock:
                # 初始化场站和topic缓存
                if station_id not in self.data_cache:
                    self.data_cache[station_id] = {}
                if topic not in self.data_cache[station_id]:
                    win_size = TOPIC_DETAIL.get(topic, {}).get("window_size", 1)
                    self.data_cache[station_id][topic] = deque(maxlen=win_size)

                # 使用Kafka时间戳或当前时间
                ts = timestamp if timestamp is not None else time.time()

                # 根据 topic 直接调用对应的更新器
                updater = self._topic_updaters.get(topic)
                if updater:
                    # 调用对应的更新方法
                    return updater(station_id, topic, raw_data, ts)
                else:
                    # 未配置的 topic，使用默认策略（单条追加）
                    logging.warning(f"未配置更新器的 topic: {topic}，使用默认策略")
                    self.data_cache[station_id][topic].append((raw_data, ts))
                    return True

        except Exception as e:
            handle_error(
                DispatcherError(e),
                context=f"窗口数据更新 station_id={station_id}, topic={topic}",
            )
            return False  # 更新失败，不触发

    def get_topic_window(self, station_id, topic):
        """
        获取窗口数据

        对于设备聚合类 topic（储能、DCDC、ACDC），直接返回聚合字典
        对于其他 topic，返回 data 部分的列表

        Args:
            station_id: 场站ID
            topic: topic名称

        Returns:
            list: 窗口数据列表
                - 设备聚合类: [{device_id: (data, ts), ...}]
                - 普通类: [data1, data2, ...]
        """
        if (
            station_id not in self.data_cache
            or topic not in self.data_cache[station_id]
        ):
            return []

        # 设备聚合类 topic：储能、DCDC、ACDC
        device_aggregation_topics = {
            "SCHEDULE-DEVICE-STORAGE",
            "SCHEDULE-DEVICE-HOST-DCDC",
            "SCHEDULE-DEVICE-HOST-ACDC",
        }

        window = self.data_cache[station_id][topic]

        if topic in device_aggregation_topics:
            # 设备聚合类：直接返回聚合字典（保持原格式给 parser）
            # window = deque([{device_id: (data, ts), ...}])
            # 返回 [{device_id: (data, ts), ...}]
            return [item for item in window]
        else:
            # 普通类：只返回 data 部分
            # window = deque[(data, ts)]
            # 返回 [data1, data2, ...]
            return [item[0] for item in window]

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
                input_data = {"stationId": station_id}

                # 数据可用性元信息（只关注是否有数据）
                data_quality = {
                    "available_topics": [],  # 有数据的topic
                    "missing_topics": [],  # 无数据的topic
                    "total_topics": 0,
                    "availability_ratio": 0.0,  # 数据可用率
                }

                # 处理模块所需的topic数据，包含依赖的其他模块输出（通过Kafka topic获取）
                required_topics = MODULE_TO_TOPICS.get(module, [])
                data_quality["total_topics"] = len(required_topics)

                for topic in required_topics:
                    # 获取指定topic的窗口数据
                    window = self.get_topic_window(station_id, topic)

                    if window:
                        # 有数据即可（不管是最新的还是缓存的）
                        data_quality["available_topics"].append(topic)

                        # 调用topic parser的parse_window方法
                        # 各topic parser可以实现自己的窗口数据处理逻辑
                        parser = TOPIC_PARSER_MAP.get(topic)
                        if parser:
                            try:
                                # 解析窗口数据并更新到输入数据中
                                parsed_data = parser.parse_window(
                                    window
                                )  # 完成枪号对齐、插值、格式化等所有处理
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
                        data_quality["missing_topics"].append(topic)

                # 计算数据可用率
                available_count = len(data_quality["available_topics"])
                total_count = data_quality["total_topics"]
                data_quality["availability_ratio"] = (
                    available_count / total_count if total_count > 0 else 0.0
                )

                # 将数据可用性信息添加到返回结果（业务模块可根据此信息决定是否处理）
                input_data["_data_quality"] = data_quality

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
                m: self.get_module_input(station_id, module=m)
                for m in self.parsers.keys()
            }

    def clean_expired(self):
        """
        清理过期数据

        对于设备聚合类 topic（储能、DCDC、ACDC），需要检查聚合字典内的设备时间戳
        对于普通 topic，检查 (data, ts) 元组的时间戳

        如果 enable_data_expiration=False，则不执行清理操作
        """
        if not self.enable_data_expiration:
            return

        now = time.time()

        # 设备聚合类 topic
        device_aggregation_topics = {
            "SCHEDULE-DEVICE-STORAGE",
            "SCHEDULE-DEVICE-HOST-DCDC",
            "SCHEDULE-DEVICE-HOST-ACDC",
        }

        with self.lock:
            expired_stations = []
            for station_id, topic_map in self.data_cache.items():
                for topic, dq in topic_map.items():
                    if not dq:
                        continue

                    if topic in device_aggregation_topics:
                        # 设备聚合类：检查聚合字典内所有设备的时间戳
                        # dq = deque([{device_id: (data, ts), ...}])
                        device_dict = dq[-1]
                        if isinstance(device_dict, dict):
                            # 检查是否所有设备都过期
                            all_expired = True
                            for device_id, (data, ts) in list(device_dict.items()):
                                if now - ts <= self.data_expire_seconds:
                                    all_expired = False
                                else:
                                    # 删除过期的设备
                                    del device_dict[device_id]

                            # 如果所有设备都过期，清空整个deque
                            if all_expired or not device_dict:
                                dq.clear()
                    else:
                        # 普通类：检查 (data, ts) 元组
                        # dq = deque[(data1, ts1), (data2, ts2), ...]
                        while dq and now - dq[-1][1] > self.data_expire_seconds:
                            dq.clear()
                            break

                # 如果所有topic都无数据则移除场站
                if all(len(dq) == 0 for dq in topic_map.values()):
                    expired_stations.append(station_id)

            for sid in expired_stations:
                del self.data_cache[sid]
                logging.info(f"场站 {sid} 数据已过期，已清理")
