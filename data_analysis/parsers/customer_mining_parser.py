#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:04:19
project: data_analysis
filename: customer_mining_parser.py
version: 1.0
"""

from ..parser_base import ParserBase

class CustomerMiningParser(ParserBase):
    def parse(self, raw_data):
        # 解析客户挖掘模块数据
        return {
            'customer_info': raw_data.get('customer_info'),
            'timestamp': raw_data.get('timestamp'),
        }
