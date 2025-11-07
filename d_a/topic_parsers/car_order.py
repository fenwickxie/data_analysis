#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:08:24
project: data_analysis
filename: car_order.py
version: 1.0
"""

from ..parser_base import ConfigBasedParser

# 使用通用的基于配置的解析器
# 字段自动从 config.TOPIC_DETAIL['SCHEDULE-CAR-ORDER']['fields'] 读取
class CarOrderParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name='SCHEDULE-CAR-ORDER')

