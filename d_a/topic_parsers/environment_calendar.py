#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:09:05
project: data_analysis
filename: environment_calendar.py
version: 1.0
"""

from ..parser_base import ParserBase

class EnvironmentCalendarParser(ParserBase):
    def parse(self, raw_data):
        # 解析SCHEDULE-ENVIRONMENT-CALENDAR
        return {
            'workday_code': raw_data.get('workday_code'),
            'holiday_code': raw_data.get('holiday_code'),
        }
