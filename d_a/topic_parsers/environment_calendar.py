#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
日历数据解析器
处理 {'calendar': [...]} 格式的全局日历数据
"""

from ..parser_base import ParserBase

class EnvironmentCalendarParser(ParserBase):
    def __init__(self):
        pass
    
    def parse(self, raw_data):
        """
        解析日历数据
        
        Args:
            raw_data: {'calendar': [{'date': '2025-11-11', 'isHoliday': 0, 'isWeekend': 0}, ...]}
            
        Returns:
            dict: {
                'date': ['2025-11-11', '2025-11-12', ...],
                'isHoliday': [0, 0, ...],
                'isWeekend': [0, 0, ...]
            }
        """
        if not raw_data:
            return None
        
        # 提取calendar数组
        calendar_list = raw_data.get('calendar', [])
        if not calendar_list:
            return None
        
        # 转换为列表格式
        parsed_data = {
            'date': [],
            'isHoliday': [],
            'isWeekend': []
        }
        
        for item in calendar_list:
            parsed_data['date'].append(item.get('date', ''))
            parsed_data['isHoliday'].append(item.get('isHoliday', 0))
            parsed_data['isWeekend'].append(item.get('isWeekend', 0))
        
        return parsed_data
    
    def parse_window(self, window_data):
        """
        日历数据通常不需要窗口处理，直接返回最新的数据
        """
        if not window_data:
            return {}
        
        # 使用最新的日历数据
        return self.parse(window_data[-1])
