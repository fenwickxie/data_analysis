#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-11-05
project: data_analysis
filename: test_mock_data_generator.py
version: 1.0
"""

import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import pytest
from tests.test_mock_producer import MockDataGenerator


def test_station_param_generation():
    """测试场站参数数据生成"""
    generator = MockDataGenerator()
    data = generator.generate_station_param("test_station_001")
    
    assert data["station_id"] == "test_station_001"
    assert "station_temp" in data
    assert "lat" in data
    assert "lng" in data
    assert "gun_count" in data
    assert "grid_capacity" in data
    assert "storage_count" in data
    assert "storage_capacity" in data
    assert "host_id" in data
    assert "timestamp" in data
    
    # 验证数据范围
    assert 20.0 <= data["station_temp"] <= 35.0
    assert 30.0 <= data["lat"] <= 40.0
    assert 110.0 <= data["lng"] <= 120.0
    assert 4 <= data["gun_count"] <= 12


def test_station_realtime_data_generation():
    """测试场站实时数据窗口生成"""
    generator = MockDataGenerator()
    window_size = 50
    data = generator.generate_station_realtime_data("test_station_001", window_size=window_size)
    
    assert data["station_id"] == "test_station_001"
    assert "gun_id" in data
    
    # 验证窗口数据
    assert len(data["history_curve_gun_avg"]) == window_size
    assert len(data["history_curve_gun_max"]) == window_size
    assert len(data["history_curve_station_avg"]) == window_size
    assert len(data["history_curve_station_max"]) == window_size
    assert len(data["timestamps"]) == window_size
    
    # 验证时间戳递增
    timestamps = data["timestamps"]
    for i in range(1, len(timestamps)):
        assert timestamps[i] > timestamps[i-1]


def test_device_meter_generation():
    """测试电表数据窗口生成"""
    generator = MockDataGenerator()
    window_size = 40
    data = generator.generate_device_meter("meter_001", window_size=window_size)
    
    assert data["meter_id"] == "meter_001"
    assert len(data["current_power"]) == window_size
    assert len(data["rated_power_limit"]) == window_size
    assert len(data["timestamps"]) == window_size
    
    # 验证所有额定功率相同
    assert all(p == 500.0 for p in data["rated_power_limit"])
    
    # 验证功率范围
    for power in data["current_power"]:
        assert 50.0 <= power <= 300.0


def test_car_order_generation():
    """测试订单数据窗口生成"""
    generator = MockDataGenerator()
    window_size = 30
    data = generator.generate_car_order("test_station_001", window_size=window_size)
    
    assert data["station_id"] == "test_station_001"
    assert "order_id" in data
    assert "charger_id" in data
    assert "gun_id" in data
    
    # 验证窗口数据
    assert len(data["current_SOC"]) == window_size
    assert len(data["demand_voltage"]) == window_size
    assert len(data["demand_current"]) == window_size
    assert len(data["timestamps"]) == window_size
    
    # 验证SOC递增趋势
    soc_values = data["current_SOC"]
    assert soc_values[0] < soc_values[-1]  # SOC应该增加
    
    # 验证时间范围
    assert data["start_time"] < data["end_time"]


def test_device_storage_generation():
    """测试储能数据窗口生成"""
    generator = MockDataGenerator()
    window_size = 50
    data = generator.generate_device_storage("host_001", window_size=window_size)
    
    assert data["host_id"] == "host_001"
    assert "storage_id" in data
    
    # 验证所有窗口字段长度
    assert len(data["storage_power"]) == window_size
    assert len(data["storage_current"]) == window_size
    assert len(data["storage_temp_max"]) == window_size
    assert len(data["storage_temp_min"]) == window_size
    assert len(data["storage_SOC"]) == window_size
    assert len(data["storage_SOH"]) == window_size
    assert len(data["timestamps"]) == window_size
    
    # 验证温度关系
    for i in range(window_size):
        assert data["storage_temp_max"][i] >= data["storage_temp_min"][i]


def test_environment_calendar_generation():
    """测试环境日历数据生成"""
    generator = MockDataGenerator()
    data = generator.generate_environment_calendar()
    
    assert "workday_code" in data
    assert "holiday_code" in data
    assert "date" in data
    assert "timestamp" in data
    
    assert data["workday_code"] in [0, 1]
    assert data["holiday_code"] in [0, 1, 2]


def test_car_price_generation():
    """测试电价数据生成"""
    generator = MockDataGenerator()
    data = generator.generate_car_price("test_station_001")
    
    assert data["station_id"] == "test_station_001"
    assert "periods" in data
    assert "timestamp" in data
    
    periods = data["periods"]
    assert len(periods) == 4  # 4个时段
    
    for period in periods:
        assert "period_no" in period
        assert "start_time" in period
        assert "end_time" in period
        assert "period_type" in period
        assert "grid_price" in period
        assert "service_fee" in period
        assert period["period_type"] in [1, 2, 3]


def test_device_error_generation():
    """测试设备错误数据窗口生成"""
    generator = MockDataGenerator()
    window_size = 10
    data = generator.generate_device_error("test_station_001", window_size=window_size)
    
    assert data["station_id"] == "test_station_001"
    assert len(data["host_error"]) == window_size
    assert len(data["ac_error"]) == window_size
    assert len(data["dc_error"]) == window_size
    assert len(data["terminal_error"]) == window_size
    assert len(data["storage_error"]) == window_size
    assert len(data["timestamps"]) == window_size
    
    # 验证错误值为0或1
    for error_list in [data["host_error"], data["ac_error"], data["dc_error"], 
                       data["terminal_error"], data["storage_error"]]:
        for error_val in error_list:
            assert error_val in [0, 1]


def test_multiple_stations():
    """测试生成多个场站的数据"""
    generator = MockDataGenerator()
    
    # 确保生成的数据有多个场站
    assert len(generator.station_ids) >= 3
    assert len(generator.host_ids) >= 3
    assert len(generator.meter_ids) >= 2
    assert len(generator.gun_ids) >= 4
    
    # 为每个场站生成数据
    for station_id in generator.station_ids:
        param_data = generator.generate_station_param(station_id)
        assert param_data["station_id"] == station_id
        
        realtime_data = generator.generate_station_realtime_data(station_id, window_size=10)
        assert realtime_data["station_id"] == station_id


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
