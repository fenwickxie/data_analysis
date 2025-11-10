#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
主机设备解析器
包含 DCDC 和 ACDC 两个解析器
DCDC 需要处理模块数组对齐
"""

from ..parser_base import ConfigBasedParser

class DeviceHostDCDCParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name='SCHEDULE-DEVICE-HOST-DCDC')
    
    def parse(self, raw_data):
        """
        解析 DCDC 数据
        
        Args:
            raw_data: {
                'stationId': '1881540927845654529',
                'hostCode': '42000000001900',
                'dcWorkStatus': ['00', '01', '00', ...],  # 20个元素
                'dcdcPower': [3.0, 15.0, 30.0, ...]      # 20个元素
            }
            
        Returns:
            dict: {
                'moduleIndex': [0, 1, 2, ...],  # 模块索引
                'dcWorkStatus': ['00', '01', '00', ...],
                'dcPower': [3.0, 15.0, 30.0, ...]
            }
        """
        if not raw_data:
            return None
        
        parsed_data = {}
        
        # 提取数组字段
        dc_work_status = raw_data.get('dcWorkStatus', [])
        dcdc_power = raw_data.get('dcdcPower', [])
        
        if dc_work_status:
            parsed_data['dcWorkStatus'] = dc_work_status
            # 生成模块索引
            parsed_data['moduleIndex'] = list(range(len(dc_work_status)))
        
        if dcdc_power:
            parsed_data['dcPower'] = dcdc_power
        
        # 提取其他字段
        if 'hostCode' in raw_data:
            parsed_data['hostCode'] = raw_data['hostCode']
        
        return parsed_data if parsed_data else None
    
    def parse_window(self, window_data):
        """
        重写窗口解析方法，实现模块对齐
        
        Args:
            window_data: 窗口内的原始数据列表
            
        Returns:
            dict: 对齐后的数据，格式为：
            {
                'moduleIndex': [0, 1, 2, ...],  # 统一的模块索引
                'dcWorkStatus': [['00', '01', ...], ['00', '00', ...], ...],
                'dcPower': [[3.0, 15.0, ...], [2.0, 10.0, ...], ...],
                'sendTime': ['2025-11-04 09:00:00', ...],
            }
        """
        if not window_data:
            return {}
        
        # 第一步：解析所有数据
        parsed_list = []
        for raw_data in window_data:
            parsed = self.parse(raw_data)
            parsed_list.append({
                'sendTime': raw_data.get('sendTime', ''),
                'parsed': parsed
            })
        
        # 第二步：找出最大模块数
        max_modules = 0
        for item in parsed_list:
            if item['parsed'] and 'moduleIndex' in item['parsed']:
                max_modules = max(max_modules, len(item['parsed']['moduleIndex']))
        
        if max_modules == 0:
            return super().parse_window(window_data)
        
        # 第三步：对齐数据
        result = {
            'sendTime': [item['sendTime'] for item in parsed_list],
            'moduleIndex': list(range(max_modules))
        }
        
        # 对齐各字段
        for field in ['dcWorkStatus', 'dcPower']:
            result[field] = []
            for item in parsed_list:
                if item['parsed'] and field in item['parsed']:
                    values = item['parsed'][field]
                    # 补齐到最大模块数
                    padded_values = values + [None] * (max_modules - len(values))
                    result[field].append(padded_values[:max_modules])
                else:
                    # 没有数据，填充 None
                    result[field].append([None] * max_modules)
        
        # hostCode 通常是单值
        if parsed_list and parsed_list[0]['parsed']:
            result['hostCode'] = parsed_list[0]['parsed'].get('hostCode')
        
        return result


class DeviceHostACDCParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name='SCHEDULE-DEVICE-HOST-ACDC')
    
    def parse(self, raw_data):
        """
        解析 ACDC 数据
        
        Args:
            raw_data: {
                'hostCode': '...',
                'acPower': ...
            }
        """
        if not raw_data:
            return None
        
        parsed_data = {}
        
        # 提取字段
        if 'hostCode' in raw_data:
            parsed_data['hostCode'] = raw_data['hostCode']
        if 'acPower' in raw_data:
            parsed_data['acPower'] = raw_data['acPower']
        
        return parsed_data if parsed_data else None
