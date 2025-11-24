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
光伏数据解析器
处理格式 {'pvPreDcPower': [...],'stationId':str} 的全局光伏数据
"""
from ..parser_base import ConfigBasedParser


class DevicePvParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name="SCHEDULE-DEVICE-PV")

    def parse(self, raw_data):
        if not raw_data:
            return None

        parsed_data = {}

        # 提取配置的顶层字段
        for field in self.fields:
            if field in raw_data:
                parsed_data[field] = raw_data[field]

        return parsed_data if parsed_data else None

    def parse_window(self, window_data):
        return self.parse(window_data[-1])
