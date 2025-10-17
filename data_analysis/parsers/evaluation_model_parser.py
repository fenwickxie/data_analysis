#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:04:38
project: data_analysis
filename: evaluation_model_parser.py
version: 1.0
"""

from ..parser_base import ParserBase

class EvaluationModelParser(ParserBase):
    def parse(self, raw_data):
        # 解析评价模块数据
        return {
            'power_gap': raw_data.get('power_gap'),
            'station_cost': raw_data.get('station_cost'),
            'timestamp': raw_data.get('timestamp'),
        }
