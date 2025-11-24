#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
站点参数解析器
处理直接列表格式的数据
"""

from ..parser_base import ConfigBasedParser


class StationParamParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name="SCHEDULE-STATION-PARAM")

    def parse(self, raw_data):
        """
        解析站点参数数据，处理嵌套的 power 对象

        Args:
            raw_data: {
                'gridCapacity': 0.0,
                'gunNum': 2,
                'hostCode': ['223311', 'SN_2025_2_20'],
                'normalCap': 186.8,
                'power': {'powerIds': ['sntt55']},
                'powerNum': 1,
                'stationId': '1053405539402387456',
                ...
            }

        Returns:
            dict: 提取配置字段的数据
        """
        if not raw_data:
            return None

        parsed_data = {}

        # 提取配置的顶层字段
        for field in self.fields:
            if field in raw_data:
                parsed_data[field] = raw_data[field]

        # # 特殊处理：如果有 power 对象但配置中包含 meterId，尝试提取
        # if 'meterId' in self.fields and 'power' in raw_data:
        #     power_obj = raw_data['power']
        #     if isinstance(power_obj, dict) and 'powerIds' in power_obj:
        #         # 将 powerIds 映射为 meterId（假设第一个为主ID）
        #         power_ids = power_obj['powerIds']
        #         if power_ids:
        #             parsed_data['meterId'] = power_ids[0] if len(power_ids) == 1 else power_ids

        return parsed_data if parsed_data else None

    def parse_window(self, window_data):
        """
        站点参数通常只需要最新的数据
        
        Args:
            window_data: list of raw_data
            
        Returns:
            dict: 解析后的数据
        """
        if not window_data:
            return None
        
        # 使用窗口中最新的数据（最后一个元素）
        return self.parse(window_data[-1])
