#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
自动更新：使用基于配置的解析器
字段自动从 config.TOPIC_DETAIL['SCHEDULE-STATION-PARAM']['fields'] 读取
"""

from ..parser_base import ConfigBasedParser

class StationParamParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name='SCHEDULE-STATION-PARAM')


# 如果需要自定义解析逻辑，可以覆盖 parse 方法：
# 
# class StationParamParser(ConfigBasedParser):
#     def __init__(self):
#         super().__init__(topic_name='SCHEDULE-STATION-PARAM')
#     
#     def parse(self, raw_data):
#         # 先调用父类方法获取基础字段
#         parsed_data = super().parse(raw_data)
#         
#         if parsed_data:
#             # 添加自定义处理逻辑
#             # 例如：类型转换、计算衍生字段等
#             pass
#         
#         return parsed_data
