#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:09:13
project: data_analysis
filename: station_param.py
version: 1.0
"""

from ..parser_base import ParserBase

class StationParamParser(ParserBase):
    def parse(self, raw_data):
        # 解析SCHEDULE-STATION-PARAM
        return {
            'station_id': raw_data.get('station_id'),
            'station_temp': raw_data.get('station_temp'),
            'lat': raw_data.get('lat'),
            'lng': raw_data.get('lng'),
            'gun_count': raw_data.get('gun_count'),
            'grid_capacity': raw_data.get('grid_capacity'),
            'storage_count': raw_data.get('storage_count'),
            'storage_capacity': raw_data.get('storage_capacity'),
            'host_id': raw_data.get('host_id'),
        }
