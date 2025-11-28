#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-11-24
project: data_analysis
filename: data_generator_base.py
version: 2.0
description: 测试数据生成基础类，应用建造者模式和策略模式
"""

import random
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from datetime import datetime


@dataclass
class WindowConfig:
    """窗口数据配置"""
    size: int
    interval: int  # 秒
    
    def generate_timestamps(self, base_time: Optional[float] = None) -> List[float]:
        """生成时间戳序列"""
        if base_time is None:
            base_time = time.time()
        return [base_time - (self.size - i) * self.interval for i in range(self.size)]


class WindowDataBuilder:
    """窗口数据建造者（Builder Pattern）"""
    
    def __init__(self, window_size: int, interval: int):
        self.config = WindowConfig(window_size, interval)
        self.fields: Dict[str, List] = {}
        self.metadata: Dict[str, Any] = {}
        
    def add_random_field(self, field_name: str, min_val: float, max_val: float, 
                        precision: int = 2) -> 'WindowDataBuilder':
        """添加随机数值字段"""
        values = [round(random.uniform(min_val, max_val), precision) 
                 for _ in range(self.config.size)]
        self.fields[field_name] = values
        return self
    
    def add_constant_field(self, field_name: str, value: Any) -> 'WindowDataBuilder':
        """添加常量字段"""
        self.fields[field_name] = [value] * self.config.size
        return self
    
    def add_choice_field(self, field_name: str, choices: List[Any]) -> 'WindowDataBuilder':
        """添加选择字段"""
        values = [random.choice(choices) for _ in range(self.config.size)]
        self.fields[field_name] = values
        return self
    
    def add_linear_field(self, field_name: str, start: float, end: float, 
                        precision: int = 2) -> 'WindowDataBuilder':
        """添加线性变化字段"""
        step = (end - start) / (self.config.size - 1) if self.config.size > 1 else 0
        values = [round(start + i * step, precision) for i in range(self.config.size)]
        self.fields[field_name] = values
        return self
    
    def add_metadata(self, key: str, value: Any) -> 'WindowDataBuilder':
        """添加元数据"""
        self.metadata[key] = value
        return self
    
    def build(self) -> Dict[str, Any]:
        """构建最终数据"""
        result = self.metadata.copy()
        result.update(self.fields)
        result['timestamps'] = self.config.generate_timestamps()
        return result


class DataGenerationStrategy(ABC):
    """数据生成策略接口（Strategy Pattern）"""
    
    @abstractmethod
    def generate(self, entity_id: str, window_size: int, **kwargs) -> Dict[str, Any]:
        """生成数据"""
        pass


class StationParamStrategy(DataGenerationStrategy):
    """场站参数生成策略"""
    
    def __init__(self, host_ids: List[str]):
        self.host_ids = host_ids
    
    def generate(self, entity_id: str, window_size: int = 1, **kwargs) -> Dict[str, Any]:
        return {
            "station_id": entity_id,
            "station_temp": round(random.uniform(20.0, 35.0), 2),
            "lat": round(random.uniform(30.0, 40.0), 6),
            "lng": round(random.uniform(110.0, 120.0), 6),
            "gun_count": random.randint(4, 12),
            "grid_capacity": random.randint(500, 2000),
            "storage_count": random.randint(1, 4),
            "storage_capacity": random.randint(100, 500),
            "host_id": random.choice(self.host_ids),
            "timestamp": time.time(),
        }


class StationRealtimeStrategy(DataGenerationStrategy):
    """场站实时数据生成策略"""
    
    def __init__(self, gun_ids: List[str]):
        self.gun_ids = gun_ids
    
    def generate(self, entity_id: str, window_size: int = 100, **kwargs) -> Dict[str, Any]:
        builder = WindowDataBuilder(window_size, interval=60)
        
        # 先生成基础数据
        station_avg = [round(random.uniform(50.0, 200.0), 2) for _ in range(window_size)]
        station_max = [round(random.uniform(200.0, 400.0), 2) for _ in range(window_size)]
        
        # 使用建造者模式构建
        return (builder
                .add_metadata("station_id", entity_id)
                .add_metadata("gun_id", random.choice(self.gun_ids))
                .add_metadata("history_curve_gun_avg", 
                             [v * random.uniform(0.5, 0.8) for v in station_avg])
                .add_metadata("history_curve_gun_max",
                             [v * random.uniform(0.6, 0.9) for v in station_max])
                .add_metadata("history_curve_station_avg", station_avg)
                .add_metadata("history_curve_station_max", station_max)
                .build())


class DeviceMeterStrategy(DataGenerationStrategy):
    """电表数据生成策略"""
    
    def generate(self, entity_id: str, window_size: int = 100, **kwargs) -> Dict[str, Any]:
        return (WindowDataBuilder(window_size, interval=300)
                .add_metadata("meter_id", entity_id)
                .add_random_field("current_power", 50.0, 300.0)
                .add_constant_field("rated_power_limit", 500.0)
                .build())


class DeviceGunStrategy(DataGenerationStrategy):
    """充电枪数据生成策略"""
    
    def __init__(self, gun_ids: List[str]):
        self.gun_ids = gun_ids
    
    def generate(self, entity_id: str, window_size: int = 100, **kwargs) -> Dict[str, Any]:
        return (WindowDataBuilder(window_size, interval=15)
                .add_metadata("host_id", entity_id)
                .add_metadata("gun_id", random.choice(self.gun_ids))
                .add_choice_field("gun_status", [0, 1, 2, 3])
                .build())


class CarOrderStrategy(DataGenerationStrategy):
    """订单数据生成策略"""
    
    def __init__(self, host_ids: List[str], gun_ids: List[str]):
        self.host_ids = host_ids
        self.gun_ids = gun_ids
    
    def generate(self, entity_id: str, window_size: int = 100, **kwargs) -> Dict[str, Any]:
        base_time = time.time()
        order_id = f"order_{int(base_time)}"
        
        builder = WindowDataBuilder(window_size, interval=1)
        result = (builder
                 .add_metadata("station_id", entity_id)
                 .add_metadata("order_id", order_id)
                 .add_metadata("charger_id", random.choice(self.host_ids))
                 .add_metadata("gun_id", random.choice(self.gun_ids))
                 .add_metadata("charger_rated_current", 250.0)
                 .add_metadata("start_time", base_time - window_size)
                 .add_metadata("end_time", base_time)
                 .add_metadata("start_SOC", 20.0)
                 .add_metadata("mileage", random.randint(1000, 100000))
                 .add_metadata("car_model", random.choice(["Tesla Model 3", "NIO ES6", "BYD Han"]))
                 .add_metadata("battery_capacity", random.choice([60.0, 75.0, 90.0, 100.0]))
                 .add_linear_field("current_SOC", 20.0, min(95.0, 20.0 + window_size * 0.5))
                 .add_random_field("demand_voltage", 350.0, 450.0)
                 .add_random_field("demand_current", 50.0, 200.0)
                 .build())
        
        return result


class CarPriceStrategy(DataGenerationStrategy):
    """电价数据生成策略"""
    
    def generate(self, entity_id: str, window_size: int = 1, **kwargs) -> Dict[str, Any]:
        periods = []
        for i in range(4):
            periods.append({
                "period_no": i + 1,
                "start_time": f"{i*6:02d}:00",
                "end_time": f"{(i+1)*6:02d}:00",
                "period_type": random.choice([1, 2, 3]),
                "grid_price": round(random.uniform(0.3, 1.2), 4),
                "service_fee": round(random.uniform(0.1, 0.5), 4),
            })
        
        return {
            "station_id": entity_id,
            "periods": periods,
            "timestamp": time.time(),
        }


class DeviceErrorStrategy(DataGenerationStrategy):
    """设备错误数据生成策略"""
    
    def generate(self, entity_id: str, window_size: int = 10, **kwargs) -> Dict[str, Any]:
        # 错误数据间隔不规则
        base_time = time.time()
        timestamps = [base_time - (window_size - i) * random.randint(60, 600) 
                     for i in range(window_size)]
        
        builder = WindowDataBuilder(window_size, interval=1)  # 间隔无效，手动设置
        result = (builder
                 .add_metadata("station_id", entity_id)
                 .add_choice_field("host_error", [0, 1])
                 .add_choice_field("ac_error", [0, 1])
                 .add_choice_field("dc_error", [0, 1])
                 .add_choice_field("terminal_error", [0, 1])
                 .add_choice_field("storage_error", [0, 1])
                 .build())
        
        result['timestamps'] = sorted(timestamps)  # 覆盖为不规则时间戳
        return result


class DeviceHostStrategy(DataGenerationStrategy):
    """主机数据生成策略"""
    
    def generate(self, entity_id: str, window_size: int = 100, **kwargs) -> Dict[str, Any]:
        # 随机选择1秒或15秒间隔
        interval = random.choice([1, 15])
        
        return (WindowDataBuilder(window_size, interval=interval)
                .add_metadata("host_id", entity_id)
                .add_choice_field("acdc_status", [0, 1, 2])
                .add_random_field("dcdc_input_power", 10.0, 200.0)
                .add_random_field("acdc_input_power", 10.0, 200.0)
                .build())


class DeviceStorageStrategy(DataGenerationStrategy):
    """储能数据生成策略"""
    
    def generate(self, entity_id: str, window_size: int = 100, **kwargs) -> Dict[str, Any]:
        storage_id = f"storage_{random.randint(1, 4):03d}"
        
        return (WindowDataBuilder(window_size, interval=15)
                .add_metadata("host_id", entity_id)
                .add_metadata("storage_id", storage_id)
                .add_random_field("storage_power", -100.0, 100.0)
                .add_random_field("storage_current", -50.0, 50.0)
                .add_random_field("storage_temp_max", 25.0, 45.0)
                .add_random_field("storage_temp_min", 20.0, 35.0)
                .add_random_field("storage_SOC", 20.0, 100.0)
                .add_random_field("storage_SOH", 85.0, 100.0)
                .build())


class EnvironmentCalendarStrategy(DataGenerationStrategy):
    """环境日历生成策略"""
    
    def generate(self, entity_id: str = "global", window_size: int = 1, **kwargs) -> Dict[str, Any]:
        return {
            "workday_code": random.choice([0, 1]),
            "holiday_code": random.choice([0, 1, 2]),
            "date": datetime.now().strftime("%Y-%m-%d"),
            "timestamp": time.time(),
        }


class DataGeneratorFactory:
    """数据生成器工厂（Factory Pattern）"""
    
    def __init__(self):
        self.station_ids = ["station_001", "station_002", "station_003"]
        self.host_ids = ["host_001", "host_002", "host_003"]
        self.meter_ids = ["meter_001", "meter_002"]
        self.gun_ids = ["gun_001", "gun_002", "gun_003", "gun_004"]
        
        # 注册策略
        self._strategies: Dict[str, DataGenerationStrategy] = {
            "station_param": StationParamStrategy(self.host_ids),
            "station_realtime": StationRealtimeStrategy(self.gun_ids),
            "device_meter": DeviceMeterStrategy(),
            "device_gun": DeviceGunStrategy(self.gun_ids),
            "car_order": CarOrderStrategy(self.host_ids, self.gun_ids),
            "car_price": CarPriceStrategy(),
            "device_error": DeviceErrorStrategy(),
            "device_host": DeviceHostStrategy(),
            "device_storage": DeviceStorageStrategy(),
            "environment_calendar": EnvironmentCalendarStrategy(),
        }
    
    def generate(self, data_type: str, entity_id: str, window_size: int = 100, 
                **kwargs) -> Dict[str, Any]:
        """使用工厂方法生成数据"""
        strategy = self._strategies.get(data_type)
        if not strategy:
            raise ValueError(f"Unknown data type: {data_type}")
        
        return strategy.generate(entity_id, window_size, **kwargs)
    
    def get_available_types(self) -> List[str]:
        """获取支持的数据类型"""
        return list(self._strategies.keys())
