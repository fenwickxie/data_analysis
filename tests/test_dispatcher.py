#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:10:52
project: data_analysis
filename: test_dispatcher.py
version: 1.0
"""

import pytest
from d_a.dispatcher import DataDispatcher
from d_a.config import TOPIC_DETAIL

def test_window_padding():
    dispatcher = DataDispatcher(data_expire_seconds=60)
    station_id = 'test_station'
    topic = 'SCHEDULE-DEVICE-METER'
    win_size = TOPIC_DETAIL[topic]['window_size']
    # 插入不足窗口长度的数据
    for i in range(3):
        dispatcher.update_topic_data(station_id, topic, {'meter_id': 1, 'current_power': i, 'rated_power_limit': 10})
    window = dispatcher.get_topic_window(station_id, topic)
    assert len(window) == 3
    # get_module_input应补全到win_size
    result = dispatcher.get_module_input(station_id, 'operation_optimization')
    assert result is not None
    # 检查窗口补零/插值字段
    assert 'current_power_window' in result
    assert len(result['current_power_window']) == win_size

def test_expired_clean():
    dispatcher = DataDispatcher(data_expire_seconds=0.01)
    station_id = 'test_station2'
    topic = 'SCHEDULE-DEVICE-METER'
    dispatcher.update_topic_data(station_id, topic, {'meter_id': 1, 'current_power': 1, 'rated_power_limit': 10})
    import time
    time.sleep(0.02)
    dispatcher.clean_expired()
    assert station_id not in dispatcher.data_cache
