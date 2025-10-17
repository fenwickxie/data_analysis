#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:06:21
project: data_analysis
filename: load_prediction_parser.py
version: 1.0
"""

from ..parser_base import ParserBase

class LoadPredictionParser(ParserBase):
    def parse(self, raw_data):
        # 解析负载预测数据
        return {
            'load_forecast': raw_data.get('load_forecast'),
            'timestamp': raw_data.get('timestamp'),
        }
