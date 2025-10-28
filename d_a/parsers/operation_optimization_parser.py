#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:07:30
project: data_analysis
filename: operation_optimization_parser.py
version: 1.0
"""

from ..parser_base import ParserBase

class OperationOptimizationParser(ParserBase):
    def parse(self, raw_data):
        # 解析运行优化及配置模块数据
        return {
            'storage_power': raw_data.get('storage_power'),
            'charge_power': raw_data.get('charge_power'),
            'timestamp': raw_data.get('timestamp'),
        }
