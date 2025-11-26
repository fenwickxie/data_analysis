#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
充电订单解析器
包含枪号对齐和数据插值逻辑
"""

from ..parser_base import ConfigBasedParser


class CarOrderParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name="SCHEDULE-CAR-ORDER")

    def parse_window(self, window_data):
        """
        重写窗口解析方法,实现枪号对齐

        订单数据的特殊处理：
        - window_size=2,保留2秒数据：上一秒（已完成）+ 当前秒（聚合中）
        - 事件触发时,业务模块应处理上一秒已完成的聚合订单
        - window_data = [[{order1}, {order2}], [{order3}]]
          - window_data[-2]: 上一秒已完成的聚合订单（应处理）
          - window_data[-1]: 当前秒聚合中的订单（不应处理）

        Args:
            window_data: 窗口内的原始数据列表
                - 正常情况：[[order1, order2], [order3]] （2条记录）
                - 初始情况：[[order1]] （只有1条记录,第一次触发）

        Returns:
            list[dict]: 聚合后的订单数据,格式为：
            {"car_order":
                [
                    {
                        'gunNo': '01',  # 统一的枪号顺序
                        'transactionSerialNo': 'tx1',  # 当前时刻各枪号的订单
                        'outputPower': 229.7,
                        'soc': 79.93,
                        'startChargeTime': '2025-11-12 08:47:26',
                        ...
                    },
                    {
                        'gunNo': '02',  # 统一的枪号顺序
                        'transactionSerialNo':  'tx2',  # 当前时刻各枪号的订单
                        'outputPower': 156.3,
                        'soc': 85.20,
                        'startChargeTime': '2025-11-12 08:47:26',
                        ...
                    }, ...
                ]
            }
        """
        if not window_data:
            return None

        # 获取应处理的订单数据
        # - 如果有2条：取倒数第二条（上一秒已完成的聚合订单）
        # - 如果只有1条：取最后一条（初始情况或服务刚启动）
        if len(window_data) >= 2:
            latest_data = window_data[-2]  # 上一秒已完成的聚合订单
        else:
            latest_data = window_data[-1]  # 初始情况

        return {"car_order": latest_data}