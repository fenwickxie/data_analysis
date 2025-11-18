#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据解析服务基类,提取公共逻辑
"""

import logging
from .config import TOPIC_TO_MODULES, MODULE_TO_TOPICS
from .errors import handle_error
from .dispatcher import DataDispatcher


class ServiceBase:
    """服务基类，包含同步和异步服务的公共逻辑"""

    def __init__(self, module_name, topics, kafka_config, data_expire_seconds):
        """
        初始化服务基础配置

        Args:
            module_name: 模块名称
            topics: topic列表（如果为None则根据module_name自动获取）
            kafka_config: Kafka配置
            data_expire_seconds: 数据过期时间
        """
        self.module_name = module_name
        self.topics = self._resolve_topics(module_name, topics)
        self.kafka_config = kafka_config
        self.data_expire_seconds = data_expire_seconds

        # 创建 dispatcher（子类可以覆盖）
        self.dispatcher = DataDispatcher(data_expire_seconds=data_expire_seconds)

        # 日志输出订阅信息
        if module_name:
            logging.info(
                f"模块 '{module_name}' 订阅 {len(self.topics)} 个 topics: {', '.join(self.topics)}"
            )
        else:
            logging.info(f"订阅 {len(self.topics)} 个 topics")

        # Topic数据提取策略映射
        # 列表格式topic：直接列表 [{'stationId': ...}, ...]
        self._list_format_topics = {"SCHEDULE-STATION-PARAM"}

        # 窗口格式topic：包含窗口数据的字典 {'realTimeData': [...]} 或 {'fee': [...]}
        # 需要按场站分组并排序
        self._window_format_topics = {
            "SCHEDULE-STATION-REALTIME-DATA",  # realTimeData
            "SCHEDULE-CAR-PRICE",  # fee
        }

        # 全局数据topic：无stationId的全局配置数据
        self._global_data_topics = {"SCHEDULE-ENVIRONMENT-CALENDAR"}
        # 输出topic集合
        self._output_topics = {"SCHEDULE-LOAD-PREDICTION"}

        # 窗口数据key映射（用于从字典中提取数据列表）
        self._window_data_keys = {
            "SCHEDULE-STATION-REALTIME-DATA": "realTimeData",
            "SCHEDULE-CAR-PRICE": "fee",
        }

        # 全局数据key映射
        self._global_data_keys = {
            "SCHEDULE-ENVIRONMENT-CALENDAR": "calendar",
        }

    @staticmethod
    def _resolve_topics(module_name, topics):
        """
        解析需要订阅的topics

        Args:
            module_name: 模块名称
            topics: 显式指定的topics

        Returns:
            list: topic列表

        Raises:
            ValueError: 当module_name未配置topics时
        """
        if topics is not None:
            # 显式指定 topics
            return topics
        elif module_name:
            # 根据模块名获取所需 topics
            default_topics = MODULE_TO_TOPICS.get(module_name)
            if not default_topics:
                raise ValueError(
                    f"模块 '{module_name}' 未配置 topics，请在 config.py 的 MODULE_TO_TOPICS 中添加"
                )
            return list(dict.fromkeys(default_topics))
        else:
            # 未指定模块名和 topics，订阅所有（仅用于调试）
            logging.warning(
                "未指定 module_name 或 topics，将订阅所有 topics，可能影响性能"
            )
            return list(TOPIC_TO_MODULES.keys())

    @staticmethod
    def _extract_list_format(value):
        """
        提取列表格式数据：[{'stationId': '...', ...}, ...]

        示例：SCHEDULE-STATION-PARAM

        Args:
            value: 原始数据（list）

        Returns:
            list: [(station_id, data), ...]
        """
        station_data_list = []
        for item in value:
            if isinstance(item, dict):
                station_id = item.get("stationId")
                if station_id:
                    station_data_list.append((station_id, item))
        return station_data_list

    @staticmethod
    def _extract_window_format(value, data_key):
        """
        提取窗口格式数据：{'realTimeData': [...]} 或 {'fee': [...]}

        窗口数据需要：
        1. 按场站分组
        2. 按 sendTime 排序

        示例：SCHEDULE-STATION-REALTIME-DATA, SCHEDULE-CAR-PRICE

        Args:
            value: 原始数据（dict）
            data_key: 数据列表的key（'realTimeData' 或 'fee'）

        Returns:
            list: [(station_id, [sorted_data]), ...]
        """
        station_data_list = []
        data_list = value.get(data_key)

        if not data_list or not isinstance(data_list, list):
            return station_data_list

        # 按场站分组
        from collections import defaultdict

        station_groups = defaultdict(list)

        for item in data_list:
            if isinstance(item, dict):
                station_id = item.get("stationId")
                if station_id:
                    station_groups[station_id].append(item)

        # 对每个场站的数据按 sendTime 排序
        for station_id, items in station_groups.items():
            sorted_items = sorted(items, key=lambda x: x.get("sendTime", ""))
            station_data_list.append((station_id, sorted_items))

        return station_data_list

    @staticmethod
    def _extract_single_format(value):
        """
        提取单场站格式数据：{'stationId': '...', ...}

        示例：SCHEDULE-DEVICE-STORAGE, SCHEDULE-DEVICE-HOST-DCDC, SCHEDULE-CAR-ORDER

        Args:
            value: 原始数据（dict）

        Returns:
            list: [(station_id, data)] 单个元素的列表
        """
        station_id = value.get("stationId")
        if station_id:
            return [(station_id, value)]
        return []

    @staticmethod
    def _extract_output_format(value):
        """
        提取输出格式数据：{'batch_id': 'batch_1763112363623_2980700020096', 'module': 'load_prediction', 'results': [{'predicted_load': [25.015590419670175, 29.18676197267237, 26.610251686341627, 20.972859424021763, 18.64837065807092, 18.451426688502522, 18.028758154486244, 18.77851120096669, 20.31007351288126, 23.734521936301125, 24.750153727642743, 30.04224424756063, 30.50005881150041, 30.14793448980103, 29.571786846814057, 21.11390886248814, 29.999871314055085, 32.163665228815645, 14.210197621466953, 19.568502845489064, 24.86399270768177, 25.4425587506667, 29.82325265710602, 18.337728090106644], 'predicted_time': ['2025-11-12 21:00:00', '2025-11-12 22:00:00', '2025-11-12 23:00:00', '2025-11-13 00:00:00', '2025-11-13 01:00:00', '2025-11-13 02:00:00', '2025-11-13 03:00:00', '2025-11-13 04:00:00', '2025-11-13 05:00:00', '2025-11-13 06:00:00', '2025-11-13 07:00:00', '2025-11-13 08:00:00', '2025-11-13 09:00:00', '2025-11-13 10:00:00', '2025-11-13 11:00:00', '2025-11-13 12:00:00', '2025-11-13 13:00:00', '2025-11-13 14:00:00', '2025-11-13 15:00:00', '2025-11-13 16:00:00', '2025-11-13 17:00:00', '2025-11-13 18:00:00', '2025-11-13 19:00:00', '2025-11-13 20:00:00'], 'station_id': '1173980655843938304'},...], 'stations_count': 3, 'timestamp': 1763112692.5043597}

        示例：SCHEDULE-LOAD-PREDICTION

        Args:
            value: 原始数据（dict）

        Returns:
            list: [(station_id, data)] 单个元素的列表

        """
        station_data_list = []
        results = value.get("results", [])
        if isinstance(results, list):
            for item in results:
                if isinstance(item, dict):
                    station_id = item.get("station_id")
                    if station_id:
                        station_data_list.append((station_id, item))
        return station_data_list

    @staticmethod
    def _extract_global_format(value):
        """
        提取全局数据格式：{'calendar': [...]} 等无stationId的数据

        示例：SCHEDULE-ENVIRONMENT-CALENDAR

        Args:
            value: 原始数据（dict）

        Returns:
            list: [('__global__', data)]
        """
        return [("__global__", value)]

    def extract_station_data(self, topic, value):
        """
        从消息中提取场站数据列表

        职责：只负责识别消息格式并提取场站数据，不做任何解析

        根据 topic 类型选择对应的提取策略：
        1. 列表格式（SCHEDULE-STATION-PARAM）
        2. 窗口格式（SCHEDULE-STATION-REALTIME-DATA, SCHEDULE-CAR-PRICE）
        3. 全局格式（SCHEDULE-ENVIRONMENT-CALENDAR）
        4. 模型输出数据格式（MODULE-OUTPUT-LOAD-PREDICTION）
        5. 单条格式（其他 topic）

        Args:
            topic: topic名称
            value: 解析后的JSON数据（dict或list）

        Returns:
            list: [(station_id, raw_data), ...] 场站ID和对应原始数据的列表
                  - 对于窗口数据：raw_data 是按 sendTime 排序的列表
                  - 对于单条数据：raw_data 是单个字典
                  - 全局数据返回 [('__global__', raw_data)]
        """
        try:
            # 策略1：列表格式
            if topic in self._list_format_topics:
                if isinstance(value, list):
                    return self._extract_list_format(value)
                return []

            # 策略2：窗口格式
            if topic in self._window_format_topics:
                if isinstance(value, dict):
                    data_key = self._window_data_keys.get(topic)
                    if data_key:
                        return self._extract_window_format(value, data_key)
                return []

            # 策略3：全局格式
            if topic in self._global_data_topics:
                if isinstance(value, dict):
                    return self._extract_global_format(value)
                return []
            # 策略4： 输出topic 全局格式
            if topic in self._output_topics:
                if isinstance(value, dict):
                    return self._extract_output_format(value)
                return []
            # 策略4：单条格式（默认）
            # 兼容处理：自动识别未预先声明的 topic
            if isinstance(value, list):
                # 未声明但是列表格式，尝试作为列表格式处理
                return self._extract_list_format(value)
            elif isinstance(value, dict):
                # 检查是否是单场站数据
                if "stationId" in value:
                    return self._extract_single_format(value)

                # 尝试识别窗口格式（未声明的新 topic）
                for key in ["realTimeData", "fee", "data"]:
                    if key in value and isinstance(value[key], list):
                        logging.warning(
                            f"未声明的窗口格式 topic={topic}, key={key}，建议添加到配置中"
                        )
                        return self._extract_window_format(value, key)

                # 可能是全局数据
                logging.warning(
                    f"未识别的消息格式 topic={topic}, keys={list(value.keys())[:5]}"
                )

            return []

        except Exception as exc:
            handle_error(exc, context=f"提取场站数据 topic={topic}")
            return []
