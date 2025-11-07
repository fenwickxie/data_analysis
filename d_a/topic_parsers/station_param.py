#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:09:13
project: data_analysis
filename: station_param.py
version: 1.0
"""

from ..parser_base import ConfigBasedParser

# 使用通用的基于配置的解析器
# 字段自动从 config.TOPIC_DETAIL['SCHEDULE-STATION-PARAM']['fields'] 读取
class StationParamParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name='SCHEDULE-STATION-PARAM')

# 如果需要自定义解析逻辑，可以覆盖 parse 方法：
# class StationParamParser(ConfigBasedParser):
#     def __init__(self):
#         super().__init__(topic_name='SCHEDULE-STATION-PARAM')
#     
#     def parse(self, raw_data):
#         # 先调用父类方法获取基础字段
#         parsed_data = super().parse(raw_data)
#         
#         # 添加自定义处理逻辑
#         if parsed_data:
#             # 例如：字段转换、计算衍生字段等
#             parsed_data['custom_field'] = parsed_data.get('stationId', '') + '_processed'
#         
#         return parsed_data

