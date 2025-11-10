#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
充电价格解析器
处理 {'fee': [...]} 格式的价格数据
"""

from ..parser_base import ParserBase

class CarPriceParser(ParserBase):
    def __init__(self):
        pass
    
    def parse(self, raw_data):
        """
        解析价格数据
        
        Args:
            raw_data: {
                'fee': [
                    {
                        'stationId': '1077316458003959808',
                        'hostCode': '52000000000088',
                        'peakElectricFee': 0.8,
                        'peakServerFee': 0.6,
                        'flatElectricFee': 0.7,
                        'flatServerFee': 0.4,
                        'sharpElectricFee': 0.5,
                        'sharpServerFee': 0.2,
                        'ebbElectricFee': 0.6,
                        'ebbServerFee': 0.3,
                        'sendTime': '2025-10-21 18:58:13',
                        ...
                    }
                ]
            }
            
        Returns:
            dict: 转换为列表格式
        """
        if not raw_data:
            return None
        
        # 提取 fee 数组
        fee_list = raw_data.get('fee', [])
        if not fee_list:
            return None
        
        # 转换为列表格式（时序数据）
        parsed_data = {}
        
        # 关键字段列表
        key_fields = [
            'peakElectricFee', 'peakServerFee',
            'flatElectricFee', 'flatServerFee', 
            'sharpElectricFee', 'sharpServerFee',
            'ebbElectricFee', 'ebbServerFee',
            'otherElectricFee', 'otherServerFee',
            'sendTime', 'hostCode'
        ]
        
        for field in key_fields:
            parsed_data[field] = []
            for item in fee_list:
                parsed_data[field].append(item.get(field))
        
        # 时段费率编号（feeNo1-feeNo48，对应48个时段）
        # 提取第一个费率方案作为代表（通常同一批次的费率方案相同）
        if fee_list:
            first_fee = fee_list[0]
            fee_numbers = []
            for i in range(1, 49):  # feeNo1 到 feeNo48
                fee_no_key = f'feeNo{i}'
                if fee_no_key in first_fee:
                    fee_numbers.append(first_fee[fee_no_key])
            if fee_numbers:
                parsed_data['feeNumbers'] = fee_numbers
        
        return parsed_data if parsed_data else None
    
    def parse_window(self, window_data):
        """
        解析窗口数据，通常价格数据使用最新的一条
        """
        if not window_data:
            return {}
        
        # 使用最新的价格数据
        return self.parse(window_data[-1])
