#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:09:19
project: data_analysis
filename: station_realtime_data.py
version: 1.0
"""

from ..parser_base import ConfigBasedParser

# 使用通用的基于配置的解析器
# 字段自动从 config.TOPIC_DETAIL['SCHEDULE-STATION-REALTIME-DATA']['fields'] 读取
class StationRealtimeDataParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name='SCHEDULE-STATION-REALTIME-DATA')

