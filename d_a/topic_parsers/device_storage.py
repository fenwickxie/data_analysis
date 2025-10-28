#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:08:56
project: data_analysis
filename: device_storage.py
version: 1.0
"""

from ..parser_base import ParserBase

class DeviceStorageParser(ParserBase):
    def parse(self, raw_data):
        # 解析SCHEDULE-DEVICE-STORAGE
        return {
            'host_id': raw_data.get('host_id'),
            'storage_id': raw_data.get('storage_id'),
            'storage_power': raw_data.get('storage_power'),
            'storage_current': raw_data.get('storage_current'),
            'storage_temp_max': raw_data.get('storage_temp_max'),
            'storage_temp_min': raw_data.get('storage_temp_min'),
            'storage_SOC': raw_data.get('storage_SOC'),
            'storage_SOH': raw_data.get('storage_SOH'),
        }
