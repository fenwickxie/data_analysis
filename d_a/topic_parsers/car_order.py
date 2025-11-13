#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
充电订单解析器
包含枪号对齐和数据插值逻辑
"""

from ..parser_base import ConfigBasedParser

class CarOrderParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name='SCHEDULE-CAR-ORDER')
    
    def parse_window(self, window_data):
        """
        重写窗口解析方法，实现枪号对齐
        
        订单数据的特殊处理：
        - window_size=1，window_data 只包含最新一秒的订单列表
        - 如果是订单聚合：window_data = [[{order1}, {order2}, ...]]
        - 如果未聚合：window_data = [{order1}] 或 [{order1}, {order2}, ...]
        - 需要按枪号对齐后返回
        
        Args:
            window_data: 窗口内的原始数据列表
                - 订单聚合：[[{order1}, {order2}]]（外层列表只有1个元素）
                - 未聚合：[{order1}] 或 [{order1}, {order2}]
            
        Returns:
            dict: 对齐后的数据，格式为：
            {
                'gunNo': ['01', '02', '05', ...],  # 统一的枪号顺序
                'transactionSerialNo': ['tx1', 'tx2', 'tx3', ...],  # 当前时刻各枪号的订单
                'outputPower': [229.7, 156.3, 88.5, ...],
                'soc': [79.93, 85.20, 62.50, ...],
                'startChargeTime': ['2025-11-12 08:47:26', ...],
                ...
            }
        """
        if not window_data:
            return {}
        
        # 获取最新一条数据（window_size=1）
        latest_data = window_data[-1] if window_data else None
        return latest_data
        # if not latest_data:
        #     return {}
        
        # # 规范化：确保是订单列表
        # if isinstance(latest_data, list):
        #     # 已经是订单列表（聚合后）
        #     orders = latest_data
        # else:
        #     # 单个订单，包装为列表
        #     orders = [latest_data]
        
        # # 解析所有订单并按枪号组织
        # gun_data = {}  # {gunNo: parsed_data}
        # all_gun_nos = []
        
        # for order in orders:
        #     parsed = self.parse(order)
        #     if not parsed:
        #         continue
            
        #     gun_no = parsed.get('gunNo')
        #     if not gun_no:
        #         continue
            
        #     # 将 gunNo 从列表提取为字符串（ConfigBasedParser 返回的是列表）
        #     if isinstance(gun_no, list) and gun_no:
        #         gun_no = gun_no[0]
            
        #     # 记录枪号顺序
        #     if gun_no not in all_gun_nos:
        #         all_gun_nos.append(gun_no)
            
        #     # 存储该枪号的数据（提取标量值）
        #     gun_data[gun_no] = {}
        #     for key, value in parsed.items():
        #         if key == 'gunNo':
        #             continue
        #         # 提取列表中的第一个值（ConfigBasedParser 返回列表）
        #         if isinstance(value, list) and value:
        #             gun_data[gun_no][key] = value[0]
        #         else:
        #             gun_data[gun_no][key] = value
        
        # if not all_gun_nos:
        #     return {}
        
        # # 按枪号顺序组装返回结果
        # result = {'gunNo': all_gun_nos}
        
        # # 获取所有字段名
        # field_names = set()
        # for data in gun_data.values():
        #     field_names.update(data.keys())
        
        # # 按枪号顺序对齐所有字段
        # for field in field_names:
        #     result[field] = []
        #     for gun_no in all_gun_nos:
        #         if gun_no in gun_data and field in gun_data[gun_no]:
        #             result[field].append(gun_data[gun_no][field])
        #         else:
        #             # 该枪号没有此字段，填充默认值
        #             if field in ['stationId', 'transactionSerialNo', 'hostCode', 
        #                         'startChargeTime', 'endChargeTime', 'carProducerCode']:
        #                 result[field].append('')
        #             else:
        #                 result[field].append(0.0)
        
        # return result
