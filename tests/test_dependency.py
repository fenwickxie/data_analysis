#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:10:37
project: data_analysis
filename: test_dependency.py
version: 1.0
"""

from d_a.dispatcher import DataDispatcher
from d_a.config import TOPIC_DETAIL

def test_dependency_aggregation():
    dispatcher = DataDispatcher(data_expire_seconds=60)
    station_id = 'test_station_dep'
    # operation_optimization依赖load_prediction
    topic = 'SCHEDULE-STATION-PARAM'
    dispatcher.update_topic_data(station_id, topic, {'station_id': 1, 'station_temp': 25, 'lat': 0, 'lng': 0, 'gun_count': 1, 'grid_capacity': 1, 'storage_count': 1, 'storage_capacity': 1, 'host_id': 1})
    result = dispatcher.get_module_input(station_id, 'operation_optimization')
    assert result is not None
    # 检查依赖字段是否聚合
    assert 'station_temp' in result
    assert 'station_temp_window' in result
