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
    def extract_station_data(topic, value):
        """
        从消息中提取场站数据列表
        
        职责：只负责识别消息格式并提取场站数据，不做任何解析
        
        支持的数据格式：
        1. 直接列表: [{'stationId': '...', ...}, ...]
        2. 包含列表的字典: {'realTimeData': [...]} 或 {'fee': [...]}
        3. 单场站字典: {'stationId': '...', ...}
        4. 全局数据: {'calendar': [...]} 等无stationId的字典
        
        特殊处理：
        - 对于包含窗口数据的topic（realTimeData、fee），需要按场站分组
        - 返回格式：[(station_id, [data1, data2, ...]), ...]
        
        Args:
            topic: topic名称
            value: 解析后的JSON数据（dict或list）
            
        Returns:
            list: [(station_id, raw_data), ...] 场站ID和对应原始数据的列表
                  - 对于窗口数据：raw_data 是按 sendTime 排序的列表
                  - 对于单条数据：raw_data 是单个字典
                  - 全局数据返回 [('__global__', raw_data)]
        """
        station_data_list = []
        
        try:
            # 格式1：直接列表 [{'stationId': '...', ...}, ...]
            # 示例：SCHEDULE-STATION-PARAM
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        station_id = item.get('stationId')
                        if station_id:
                            station_data_list.append((station_id, item))
                return station_data_list
            
            # 格式2：字典格式
            if not isinstance(value, dict):
                return station_data_list
            
            # 格式2a：包含场站列表的字典 {'realTimeData': [...]} 或 {'fee': [...]}
            # 示例：SCHEDULE-STATION-REALTIME-DATA, SCHEDULE-CAR-PRICE
            # 这些数据需要按场站分组，因为一个消息包含多个场站的多个时间点数据
            list_keys = ['realTimeData', 'fee']
            for key in list_keys:
                data_list = value.get(key)
                if data_list and isinstance(data_list, list):
                    # 按场站分组
                    from collections import defaultdict
                    station_groups = defaultdict(list)
                    
                    for item in data_list:
                        if isinstance(item, dict):
                            station_id = item.get('stationId')
                            if station_id:
                                station_groups[station_id].append(item)
                    
                    # 对每个场站的数据按 sendTime 排序
                    for station_id, items in station_groups.items():
                        # 按 sendTime 排序（如果存在）
                        sorted_items = sorted(
                            items,
                            key=lambda x: x.get('sendTime', '')
                        )
                        # 返回整个窗口数据列表
                        station_data_list.append((station_id, sorted_items))
                    
                    return station_data_list
            
            # 格式2b：单场站数据 {'stationId': '...', ...}
            # 示例：SCHEDULE-DEVICE-STORAGE, SCHEDULE-DEVICE-HOST-DCDC
            station_id = value.get('stationId')
            if station_id:
                station_data_list.append((station_id, value))
                return station_data_list
            
            # 格式2c：全局数据（无stationId）
            # 示例：SCHEDULE-ENVIRONMENT-CALENDAR {'calendar': [...]}
            # 检查是否包含已知的全局数据字段
            global_keys = ['calendar']  # 可扩展其他全局数据的key
            for key in global_keys:
                if key in value:
                    # 确认是全局数据，返回特殊标识
                    station_data_list.append(('__global__', value))
                    return station_data_list
            
            # 其他未识别的字典格式，记录警告
            logging.warning(
                f"未识别的消息格式 topic={topic}, keys={list(value.keys())[:5]}"
            )
            
        except Exception as exc:
            handle_error(exc, context=f"提取场站数据 topic={topic}")
        
        return station_data_list
