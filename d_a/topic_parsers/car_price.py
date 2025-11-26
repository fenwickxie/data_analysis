#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
充电价格解析器
处理 {'fee': [...]} 格式的价格数据
"""

from ..parser_base import ConfigBasedParser


class CarPriceParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name="SCHEDULE-CAR-PRICE")

    def parse(self, raw_data):
        """
        解析价格数据,将 feeNo1-feeNo48 合并为 feeNo 列表

        Args:
            raw_data: 单条价格数据或包含 fee 列表的字典
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
                'otherElectricFee': 0.0,
                'otherServerFee': 0.0,
                'feeNo1': '00', 'feeNo2': '00', ..., 'feeNo48': '02',
                'sendTime': '2025-10-21 18:58:13',
                'dialNo': '2100',
                'lossRate': 1,
                ...
            }

            feeNo 值含义：'00'=尖, '01'=峰, '02'=平, '03'=谷, '04'=其它

        Returns:
            dict: 合并 feeNo 后的数据
            {"car_price":
                {
                    'stationId': '...',
                    'feeNo': ['00', '00', ..., '02'],  # 48个时段的费率类型
                    'peakElectricFee': 0.8,
                    ...
                }
            }
        """
        if not raw_data:
            return None

        # 复制数据（避免修改原始数据）
        parsed_data = {}

        # 提取 feeNo1-feeNo48 并合并为 feeNo 列表
        fee_numbers = []
        for i in range(1, 49):  # feeNo1 到 feeNo48（48个半小时时段）
            fee_no_key = f"feeNo{i}"
            if fee_no_key in raw_data:
                fee_numbers.append(raw_data[fee_no_key])

        if fee_numbers:
            parsed_data["feeNo"] = fee_numbers

        # 复制其他字段,排除 feeNo1-feeNo48 及不在 self.fields 的字段
        excluded_keys = {f"feeNo{i}" for i in range(1, 49)}
        for key, value in raw_data.items():
            if key not in excluded_keys and key in self.fields:
                parsed_data[key] = value

        return {"car_price": parsed_data} if parsed_data else None

    def parse_window(self, window_data):
        """
        解析窗口数据

        由于 SCHEDULE-CAR-PRICE 的 window_size=1,且一个消息包含完整的价格信息,
        所以直接解析最新的一条数据即可

        Args:
            window_data: 窗口数据列表,只有一个元素,是多日的价格数据组成的列表。
            [
                [
                    {},
                    {},
                    ... # 多日的价格数据
                ],
            ]

        Returns:
            dict: 最新一天的解析后的价格数据（feeNo1-feeNo48 已合并为 feeNo 列表）
            {"car_price":
                {
                    'stationId': '...',
                    'feeNo': ['00', '00', ..., '02'],  # 48个时段的费率类型
                    'peakElectricFee': 0.8,
                    ...
                }
            }
        """
        if not window_data:
            return None

        # 使用最新的价格数据（已经是单条数据,不是包含 'fee' 键的字典）
        return self.parse(window_data[-1][-1])  # 取最后一条数据
