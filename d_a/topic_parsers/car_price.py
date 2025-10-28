#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:08:30
project: data_analysis
filename: car_price.py
version: 1.0
"""

from ..parser_base import ParserBase

class CarPriceParser(ParserBase):
    def parse(self, raw_data):
        # 解析SCHEDULE-CAR-PRICE
        return {
            'station_id': raw_data.get('station_id'),
            'period_no': raw_data.get('period_no'),
            'start_time': raw_data.get('start_time'),
            'end_time': raw_data.get('end_time'),
            'period_type': raw_data.get('period_type'),
            'grid_price': raw_data.get('grid_price'),
            'service_fee': raw_data.get('service_fee'),
        }
