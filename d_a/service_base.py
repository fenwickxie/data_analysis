#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据解析服务基类,提取公共逻辑
"""

import logging
from .config import MODULE_OUTPUT_TOPICS, TOPIC_TO_MODULES, MODULE_TO_TOPICS
from .errors import handle_error
from .dispatcher import DataDispatcher


class ServiceBase:
    """服务基类,包含同步和异步服务的公共逻辑"""

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
                    f"模块 '{module_name}' 未配置 topics,请在 config.py 的 MODULE_TO_TOPICS 中添加"
                )
            return list(dict.fromkeys(default_topics))
        else:
            # 未指定模块名和 topics,订阅所有（仅用于调试）
            logging.warning(
                "未指定 module_name 或 topics,将订阅所有 topics,可能影响性能"
            )
            return list(TOPIC_TO_MODULES.keys())

