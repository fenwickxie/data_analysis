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
        解析 DCDC 主机设备窗口数据（从聚合字典读取）
        
        Args:
            window_data: [host_dict]，其中 host_dict = {
                'host_id_1': (raw_data1, timestamp1),
                'host_id_2': (raw_data2, timestamp2),
                ...
            }
            
        Returns:
            list: 所有 DCDC 主机设备的数据列表，格式为：
            [
                {
                    'hostCode': 'H001',
                    'moduleIndex': [0, 1, 2, ...],
                    'dcWorkStatus': ['00', '01', '00', ...],
                    'dcPower': [3.0, 15.0, 30.0, ...]
                },
                {
                    'hostCode': 'H002',
                    'moduleIndex': [0, 1, 2, ...],
                    'dcWorkStatus': ['01', '01', '00', ...],
                    'dcPower': [5.0, 20.0, 35.0, ...]
                },
                ...
            ]
        """
        if not window_data or not isinstance(window_data, list) or not window_data[0]:
            return None
        
        # 获取 DCDC 主机聚合字典
        host_dict = window_data[0]
        
        if not isinstance(host_dict, dict):
            return None
        
        # 按 hostCode 排序（保证顺序稳定）
        sorted_hosts = sorted(host_dict.items(), key=lambda x: x[0])
        
        # 组装结果列表
        result = []
        
        for host_id, (raw_data, timestamp) in sorted_hosts:
            # 解析单个主机的数据
            parsed = self.parse(raw_data)
            if parsed:
                # 确保包含 hostCode
                if 'hostCode' not in parsed:
                    parsed['hostCode'] = host_id
                result.append(parsed)
        
        return {"device_host_dcdc": result}


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
    
    def parse_window(self, window_data):
        """
        解析 ACDC 主机设备窗口数据（从聚合字典读取）
        
        Args:
            window_data: [host_dict]，其中 host_dict = {
                'host_id_1': (raw_data1, timestamp1),
                'host_id_2': (raw_data2, timestamp2),
                ...
            }
            
        Returns:
            list: 所有 ACDC 主机设备的数据列表，格式为：
            [
                {'hostCode': 'H001', 'acPower': 100.5},
                {'hostCode': 'H002', 'acPower': 200.3},
                ...
            ]
        """
        if not window_data or not isinstance(window_data, list) or not window_data[0]:
            return None
        
        # 获取 ACDC 主机聚合字典
        host_dict = window_data[0]
        
        if not isinstance(host_dict, dict):
            return None
        
        # 按 hostCode 排序（保证顺序稳定）
        sorted_hosts = sorted(host_dict.items(), key=lambda x: x[0])
        
        # 组装结果列表
        result = []
        
        for host_id, (raw_data, timestamp) in sorted_hosts:
            # 解析单个主机的数据
            parsed = self.parse(raw_data)
            if parsed:
                # 确保包含 hostCode
                if 'hostCode' not in parsed:
                    parsed['hostCode'] = host_id
                result.append(parsed)
        
        return {"device_host_acdc": result}
