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
        - window_size=2，保留2秒数据：上一秒（已完成）+ 当前秒（聚合中）
        - 事件触发时，业务模块应处理上一秒已完成的聚合订单
        - window_data = [[{order1}, {order2}], [{order3}]] 
          - window_data[-2]: 上一秒已完成的聚合订单（应处理）
          - window_data[-1]: 当前秒聚合中的订单（不应处理）
        
        Args:
            window_data: 窗口内的原始数据列表
                - 正常情况：[[order1, order2], [order3]] （2条记录）
                - 初始情况：[[order1]] （只有1条记录，第一次触发）
            
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
        
        # 获取应处理的订单数据
        # - 如果有2条：取倒数第二条（上一秒已完成的聚合订单）
        # - 如果只有1条：取最后一条（初始情况或服务刚启动）
        if len(window_data) >= 2:
            latest_data = window_data[-2]  # 上一秒已完成的聚合订单
        else:
            latest_data = window_data[-1]  # 初始情况
            
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
