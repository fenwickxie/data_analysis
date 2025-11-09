#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
自动更新：使用基于配置的解析器
包含 DCDC 和 ACDC 两个解析器
"""

from ..parser_base import ConfigBasedParser

class DeviceHostDCDCParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name='SCHEDULE-DEVICE-HOST-DCDC')


class DeviceHostACDCParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name='SCHEDULE-DEVICE-HOST-ACDC')
