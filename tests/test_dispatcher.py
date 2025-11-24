#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Dispatcher统一测试模块
整合了窗口补全、数据过期、补零策略、依赖聚合等所有Dispatcher功能测试

合并自:
- test_dispatcher.py
- test_dispatcher_padding.py  
- test_dependency.py
"""

import pytest
import time
from d_a.dispatcher import DataDispatcher
from d_a.config import TOPIC_DETAIL


# ========== 基础功能测试 ==========

def test_window_padding():
    """测试窗口数据补全"""
    dispatcher = DataDispatcher(data_expire_seconds=60)
    station_id = 'test_station'
    topic = 'SCHEDULE-DEVICE-METER'
    win_size = TOPIC_DETAIL[topic]['window_size']
    
    # 插入不足窗口长度的数据
    for i in range(3):
        dispatcher.update_topic_data(
            station_id, topic, 
            {'meter_id': 1, 'current_power': i, 'rated_power_limit': 10}
        )
    
    window = dispatcher.get_topic_window(station_id, topic)
    assert len(window) == 3
    
    # get_module_input应补全到win_size
    result = dispatcher.get_module_input(station_id, 'operation_optimization')
    assert result is not None
    assert 'current_power_window' in result
    assert len(result['current_power_window']) == win_size


def test_expired_clean():
    """测试过期数据清理"""
    dispatcher = DataDispatcher(data_expire_seconds=0.01)
    station_id = 'test_station2'
    topic = 'SCHEDULE-DEVICE-METER'
    
    dispatcher.update_topic_data(
        station_id, topic, 
        {'meter_id': 1, 'current_power': 1, 'rated_power_limit': 10}
    )
    
    time.sleep(0.02)
    dispatcher.clean_expired()
    
    assert station_id not in dispatcher.data_cache


def test_dependency_aggregation():
    """测试模块依赖字段聚合"""
    dispatcher = DataDispatcher(data_expire_seconds=60)
    station_id = 'test_station_dep'
    topic = 'SCHEDULE-STATION-PARAM'
    
    # operation_optimization依赖load_prediction
    dispatcher.update_topic_data(
        station_id, topic, 
        {
            'station_id': 1, 'station_temp': 25, 'lat': 0, 'lng': 0, 
            'gun_count': 1, 'grid_capacity': 1, 'storage_count': 1, 
            'storage_capacity': 1, 'host_id': 1
        }
    )
    
    result = dispatcher.get_module_input(station_id, 'operation_optimization')
    assert result is not None
    
    # 检查依赖字段是否聚合
    assert 'station_temp' in result
    assert 'station_temp_window' in result


# ========== 补零策略测试 ==========

def test_zero_padding():
    """测试零值补全策略"""
    dispatcher = DataDispatcher()
    dispatcher.set_padding_strategy('zero')
    seq = [1, 2]
    
    padded = dispatcher.get_module_input.__func__.__closure__[0].cell_contents['pad_or_interp'](
        dispatcher, seq, 5
    )
    
    assert padded == [0, 0, 0, 1, 2]


def test_linear_padding():
    """测试线性插值补全策略"""
    dispatcher = DataDispatcher()
    dispatcher.set_padding_strategy('linear')
    seq = [1, 3]
    
    padded = dispatcher.get_module_input.__func__.__closure__[0].cell_contents['pad_or_interp'](
        dispatcher, seq, 4
    )
    
    assert all(isinstance(x, float) for x in padded)
    assert len(padded) == 4


def test_forward_padding():
    """测试前向填充补全策略"""
    dispatcher = DataDispatcher()
    dispatcher.set_padding_strategy('forward')
    seq = [5]
    
    padded = dispatcher.get_module_input.__func__.__closure__[0].cell_contents['pad_or_interp'](
        dispatcher, seq, 3
    )
    
    assert padded == [5, 5, 5]


def test_missing_padding():
    """测试缺失值补全策略"""
    dispatcher = DataDispatcher()
    dispatcher.set_padding_strategy('missing')
    seq = []
    
    padded = dispatcher.get_module_input.__func__.__closure__[0].cell_contents['pad_or_interp'](
        dispatcher, seq, 2
    )
    
    assert padded == [None, None]


# ========== 极端情况测试 ==========

@pytest.mark.parametrize("padding_strategy,expected_type", [
    ('zero', list),
    ('linear', list),
    ('forward', list),
    ('missing', list),
])
def test_empty_sequence_padding(padding_strategy, expected_type):
    """测试空序列补全（参数化）"""
    dispatcher = DataDispatcher()
    dispatcher.set_padding_strategy(padding_strategy)
    
    topic = 'SCHEDULE-STATION-REALTIME-DATA'
    station_id = 'test_empty'
    
    # 不添加任何数据
    result = dispatcher.get_module_input(station_id, 'load_prediction')
    
    # 应该返回补全后的空窗口
    if result is not None:
        for key in result:
            if key.endswith('_window'):
                assert isinstance(result[key], expected_type)


def test_dispatcher_with_invalid_topic():
    """测试无效topic处理"""
    dispatcher = DataDispatcher()
    station_id = 'test_invalid'
    
    # 使用未配置的topic
    dispatcher.update_topic_data(station_id, 'UNKNOWN_TOPIC', {'data': 123})
    
    result = dispatcher.get_all_inputs(station_id)
    assert result == {}


def test_dispatcher_empty_cache():
    """测试空缓存情况"""
    dispatcher = DataDispatcher()
    station_id = 'test_empty_cache'
    
    result = dispatcher.get_module_input(station_id, 'load_prediction')
    
    # 空缓存应返回None或空字典
    assert result is None or result == {}


def test_multiple_stations_isolation():
    """测试多场站数据隔离"""
    dispatcher = DataDispatcher(data_expire_seconds=60)
    topic = 'SCHEDULE-DEVICE-METER'
    
    # 为不同场站添加数据
    dispatcher.update_topic_data('station_A', topic, {'meter_id': 1, 'current_power': 100, 'rated_power_limit': 10})
    dispatcher.update_topic_data('station_B', topic, {'meter_id': 2, 'current_power': 200, 'rated_power_limit': 10})
    
    # 验证数据隔离
    result_a = dispatcher.get_module_input('station_A', 'operation_optimization')
    result_b = dispatcher.get_module_input('station_B', 'operation_optimization')
    
    assert result_a is not None
    assert result_b is not None
    assert result_a != result_b


# ========== 性能测试 ==========

def test_large_window_performance():
    """测试大窗口性能"""
    dispatcher = DataDispatcher(data_expire_seconds=600)
    station_id = 'test_perf'
    topic = 'SCHEDULE-STATION-REALTIME-DATA'
    
    # 添加100条数据
    for i in range(100):
        dispatcher.update_topic_data(
            station_id, topic, 
            {'stationId': station_id, 'value': i}
        )
    
    import time
    start = time.time()
    result = dispatcher.get_module_input(station_id, 'load_prediction')
    elapsed = time.time() - start
    
    assert result is not None
    assert elapsed < 1.0  # 应在1秒内完成


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
