#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:08:47
project: data_analysis
filename: device_host.py
version: 1.0
"""

from ..parser_base import ParserBase

class DeviceHostParser(ParserBase):
    def parse(self, raw_data):
        # 解析SCHEDULE-DEVICE-HOST
        return {
            'host_id': raw_data.get('host_id'),
            'acdc_status': raw_data.get('acdc_status'),
            'dcdc_input_power': raw_data.get('dcdc_input_power'),
            'acdc_input_power': raw_data.get('acdc_input_power'),
        }
