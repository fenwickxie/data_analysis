#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:08:41
project: data_analysis
filename: device_gun.py
version: 1.0
"""

from ..parser_base import ParserBase

class DeviceGunParser(ParserBase):
    def parse(self, raw_data):
        # 解析SCHEDULE-DEVICE-GUN
        return {
            'host_id': raw_data.get('host_id'),
            'gun_id': raw_data.get('gun_id'),
            'gun_status': raw_data.get('gun_status'),
        }
