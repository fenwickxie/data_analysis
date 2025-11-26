#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-11-24 10:38:04
project: data_analysis
filename: environment_weather.py
version: 1.0
"""
"""
天气数据解析器
处理格式 {'weatherSituationYesterday':str,'seasonTomorrow':str,'weatherSituationTomorrow':str} 的全局天气数据
"""
from ..parser_base import ConfigBasedParser


class EnvironmentWeatherParser(ConfigBasedParser):
    def __init__(
        self,
    ):
        super().__init__(topic_name="SCHEDULE-ENVIRONMENT-WEATHER")

    def parse(self, raw_data):
        """
        解析天气数据
        Args: raw_data: {'stationId':str,'weatherSituationYesterday':str,'seasonTomorrow':str,'weatherSituationTomorrow':str}

        Returns: dict: 提取配置字段的数据
        """
        if not raw_data:
            return None

        parsed_data = {}

        # 提取配置的顶层字段
        for field in self.fields:
            if field in raw_data:
                parsed_data[field] = raw_data[field]

        return parsed_data if parsed_data else None

    def parse_window(self, window_data):
        """
        天气参数通常只需要最新的数据

        Args:
            window_data: list of raw_data

        Returns:
            dict: 解析后的数据
        """
        if not window_data:
            return None

        # 使用窗口中最新的数据（最后一个元素）
        return self.parse(window_data[-1])
