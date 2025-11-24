#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
验证重构后的代码功能
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import pytest
from tests.fixtures import (
    WindowConfig,
    WindowDataBuilder,
    DataGeneratorFactory,
)


class TestWindowDataBuilder:
    """测试窗口数据建造者"""
    
    def test_random_field(self):
        """测试随机字段生成"""
        data = (WindowDataBuilder(10, 60)
                .add_random_field("temp", 20.0, 30.0)
                .build())
        
        assert "temp" in data
        assert len(data["temp"]) == 10
        assert all(20.0 <= v <= 30.0 for v in data["temp"])
    
    def test_constant_field(self):
        """测试常量字段生成"""
        data = (WindowDataBuilder(10, 60)
                .add_constant_field("status", 1)
                .build())
        
        assert "status" in data
        assert len(data["status"]) == 10
        assert all(v == 1 for v in data["status"])
    
    def test_linear_field(self):
        """测试线性字段生成"""
        data = (WindowDataBuilder(10, 60)
                .add_linear_field("soc", 20.0, 80.0)
                .build())
        
        assert "soc" in data
        assert len(data["soc"]) == 10
        assert data["soc"][0] == 20.0
        assert data["soc"][-1] == 80.0
    
    def test_metadata(self):
        """测试元数据添加"""
        data = (WindowDataBuilder(10, 60)
                .add_metadata("device_id", "dev_001")
                .add_metadata("station_id", "station_001")
                .build())
        
        assert data["device_id"] == "dev_001"
        assert data["station_id"] == "station_001"
    
    def test_timestamps(self):
        """测试时间戳生成"""
        data = WindowDataBuilder(10, 60).build()
        
        assert "timestamps" in data
        assert len(data["timestamps"]) == 10
        
        # 验证时间戳递增
        for i in range(1, len(data["timestamps"])):
            assert data["timestamps"][i] > data["timestamps"][i-1]
    
    def test_chain_calls(self):
        """测试链式调用"""
        data = (WindowDataBuilder(5, 30)
                .add_metadata("id", "test")
                .add_random_field("field1", 0.0, 10.0)
                .add_constant_field("field2", 100)
                .add_linear_field("field3", 0.0, 100.0)
                .build())
        
        assert data["id"] == "test"
        assert "field1" in data
        assert "field2" in data
        assert "field3" in data
        assert "timestamps" in data


class TestDataGeneratorFactory:
    """测试数据生成工厂"""
    
    def test_factory_initialization(self):
        """测试工厂初始化"""
        factory = DataGeneratorFactory()
        
        assert len(factory.station_ids) > 0
        assert len(factory.host_ids) > 0
        assert len(factory.meter_ids) > 0
        assert len(factory.gun_ids) > 0
    
    def test_available_types(self):
        """测试获取可用数据类型"""
        factory = DataGeneratorFactory()
        types = factory.get_available_types()
        
        assert "station_param" in types
        assert "station_realtime" in types
        assert "device_meter" in types
        assert "device_gun" in types
        assert "car_order" in types
        assert len(types) >= 10
    
    def test_generate_station_param(self):
        """测试生成场站参数"""
        factory = DataGeneratorFactory()
        data = factory.generate("station_param", "station_001")
        
        assert data["station_id"] == "station_001"
        assert "station_temp" in data
        assert "lat" in data
        assert "lng" in data
        assert "gun_count" in data
        assert "grid_capacity" in data
    
    def test_generate_device_meter(self):
        """测试生成电表数据"""
        factory = DataGeneratorFactory()
        data = factory.generate("device_meter", "meter_001", window_size=50)
        
        assert data["meter_id"] == "meter_001"
        assert len(data["current_power"]) == 50
        assert len(data["rated_power_limit"]) == 50
        assert len(data["timestamps"]) == 50
        
        # 验证常量字段
        assert all(v == 500.0 for v in data["rated_power_limit"])
    
    def test_generate_car_order(self):
        """测试生成订单数据"""
        factory = DataGeneratorFactory()
        data = factory.generate("car_order", "station_001", window_size=30)
        
        assert data["station_id"] == "station_001"
        assert "order_id" in data
        assert len(data["current_SOC"]) == 30
        assert len(data["demand_current"]) == 30
        
        # 验证SOC线性增长
        soc_values = data["current_SOC"]
        assert soc_values[0] < soc_values[-1]
    
    def test_generate_station_realtime(self):
        """测试生成场站实时数据"""
        factory = DataGeneratorFactory()
        data = factory.generate("station_realtime", "station_001", window_size=20)
        
        assert data["station_id"] == "station_001"
        assert "gun_id" in data
        assert len(data["history_curve_station_avg"]) == 20
        assert len(data["history_curve_station_max"]) == 20
        assert len(data["timestamps"]) == 20
    
    def test_unknown_data_type(self):
        """测试未知数据类型"""
        factory = DataGeneratorFactory()
        
        with pytest.raises(ValueError, match="Unknown data type"):
            factory.generate("unknown_type", "entity_001")
    
    def test_window_size_parameter(self):
        """测试窗口大小参数"""
        factory = DataGeneratorFactory()
        
        # 测试不同窗口大小
        for size in [10, 50, 100]:
            data = factory.generate("device_meter", "meter_001", window_size=size)
            assert len(data["current_power"]) == size
            assert len(data["timestamps"]) == size


class TestWindowConfig:
    """测试窗口配置"""
    
    def test_timestamp_generation(self):
        """测试时间戳生成"""
        config = WindowConfig(size=10, interval=60)
        timestamps = config.generate_timestamps()
        
        assert len(timestamps) == 10
        
        # 验证间隔
        for i in range(1, len(timestamps)):
            diff = timestamps[i] - timestamps[i-1]
            assert abs(diff - 60) < 0.1  # 允许微小误差
    
    def test_custom_base_time(self):
        """测试自定义基准时间"""
        config = WindowConfig(size=5, interval=30)
        base_time = 1700000000.0
        timestamps = config.generate_timestamps(base_time)
        
        assert len(timestamps) == 5
        assert timestamps[-1] == base_time


if __name__ == "__main__":
    # 运行测试
    pytest.main([__file__, "-v", "-s"])
