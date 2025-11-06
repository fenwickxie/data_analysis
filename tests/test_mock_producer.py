#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-11-05
project: data_analysis
filename: test_mock_producer.py
version: 1.0
"""

import asyncio
import json
import logging
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

# 添加项目根目录到Python路径
if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from d_a.config import TOPIC_DETAIL, KAFKA_CONFIG
from d_a.kafka_client import AsyncKafkaProducerClient

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s"
)


class MockDataGenerator:
    """模拟数据生成器，根据config中的topic配置生成窗口数据"""
    
    def __init__(self):
        self.station_ids = ["station_001", "station_002", "station_003"]
        self.host_ids = ["host_001", "host_002", "host_003"]
        self.meter_ids = ["meter_001", "meter_002"]
        self.gun_ids = ["gun_001", "gun_002", "gun_003", "gun_004"]
        
    def generate_station_param(self, station_id: str) -> Dict[str, Any]:
        """生成场站参数数据（单条，无窗口）"""
        return {
            "station_id": station_id,
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
    
    def generate_station_realtime_data(self, station_id: str, window_size: int = 100) -> Dict[str, Any]:
        """生成场站实时数据窗口（7天历史曲线）"""
        base_time = time.time()
        window_data = []
        
        for i in range(window_size):
            timestamp = base_time - (window_size - i) * 60  # 每分钟一个点
            window_data.append({
                "timestamp": timestamp,
                "station_avg": round(random.uniform(50.0, 200.0), 2),
                "station_max": round(random.uniform(200.0, 400.0), 2),
            })
        
        return {
            "station_id": station_id,
            "gun_id": random.choice(self.gun_ids),
            "history_curve_gun_avg": [d["station_avg"] * random.uniform(0.5, 0.8) for d in window_data],
            "history_curve_gun_max": [d["station_max"] * random.uniform(0.6, 0.9) for d in window_data],
            "history_curve_station_avg": [d["station_avg"] for d in window_data],
            "history_curve_station_max": [d["station_max"] for d in window_data],
            "timestamps": [d["timestamp"] for d in window_data],
        }
    
    def generate_environment_calendar(self) -> Dict[str, Any]:
        """生成环境日历数据（单条）"""
        return {
            "workday_code": random.choice([0, 1]),  # 0工作日，1周末
            "holiday_code": random.choice([0, 1, 2]),  # 0正常，1节假日，2调休
            "date": datetime.now().strftime("%Y-%m-%d"),
            "timestamp": time.time(),
        }
    
    def generate_device_meter(self, meter_id: str, window_size: int = 100) -> Dict[str, Any]:
        """生成电表数据窗口（5分钟间隔）"""
        base_time = time.time()
        window_data = []
        
        for i in range(window_size):
            timestamp = base_time - (window_size - i) * 300  # 5分钟间隔
            window_data.append({
                "timestamp": timestamp,
                "current_power": round(random.uniform(50.0, 300.0), 2),
                "rated_power_limit": 500.0,
            })
        
        return {
            "meter_id": meter_id,
            "current_power": [d["current_power"] for d in window_data],
            "rated_power_limit": [d["rated_power_limit"] for d in window_data],
            "timestamps": [d["timestamp"] for d in window_data],
        }
    
    def generate_device_gun(self, host_id: str, window_size: int = 100) -> Dict[str, Any]:
        """生成充电枪数据窗口（15秒间隔）"""
        base_time = time.time()
        window_data = []
        
        for i in range(window_size):
            timestamp = base_time - (window_size - i) * 15  # 15秒间隔
            window_data.append({
                "timestamp": timestamp,
                "gun_status": random.choice([0, 1, 2, 3]),  # 0空闲，1充电中，2故障，3预约
            })
        
        return {
            "host_id": host_id,
            "gun_id": random.choice(self.gun_ids),
            "gun_status": [d["gun_status"] for d in window_data],
            "timestamps": [d["timestamp"] for d in window_data],
        }
    
    def generate_car_order(self, station_id: str, window_size: int = 100) -> Dict[str, Any]:
        """生成订单数据窗口（1秒间隔）"""
        base_time = time.time()
        window_data = []
        order_id = f"order_{int(base_time)}"
        
        for i in range(window_size):
            timestamp = base_time - (window_size - i)  # 1秒间隔
            soc = min(20 + i * 0.5, 100)  # SOC逐渐增加
            window_data.append({
                "timestamp": timestamp,
                "current_SOC": round(soc, 2),
                "demand_current": round(random.uniform(50.0, 200.0), 2),
            })
        
        return {
            "station_id": station_id,
            "order_id": order_id,
            "charger_id": random.choice(self.host_ids),
            "gun_id": random.choice(self.gun_ids),
            "charger_rated_current": 250.0,
            "start_time": base_time - window_size,
            "end_time": base_time,
            "start_SOC": 20.0,
            "current_SOC": [d["current_SOC"] for d in window_data],
            "demand_voltage": [random.uniform(350, 450) for _ in window_data],
            "demand_current": [d["demand_current"] for d in window_data],
            "mileage": random.randint(1000, 100000),
            "car_model": random.choice(["Tesla Model 3", "NIO ES6", "BYD Han"]),
            "battery_capacity": random.choice([60.0, 75.0, 90.0, 100.0]),
            "timestamps": [d["timestamp"] for d in window_data],
        }
    
    def generate_car_price(self, station_id: str) -> Dict[str, Any]:
        """生成电价数据（单条，包含多个时段）"""
        periods = []
        for i in range(4):  # 4个时段
            periods.append({
                "period_no": i + 1,
                "start_time": f"{i*6:02d}:00",
                "end_time": f"{(i+1)*6:02d}:00",
                "period_type": random.choice([1, 2, 3]),  # 1峰，2平，3谷
                "grid_price": round(random.uniform(0.3, 1.2), 4),
                "service_fee": round(random.uniform(0.1, 0.5), 4),
            })
        
        return {
            "station_id": station_id,
            "periods": periods,
            "timestamp": time.time(),
        }
    
    def generate_device_error(self, station_id: str, window_size: int = 10) -> Dict[str, Any]:
        """生成设备错误数据窗口"""
        base_time = time.time()
        window_data = []
        
        for i in range(window_size):
            timestamp = base_time - (window_size - i) * random.randint(60, 600)
            window_data.append({
                "timestamp": timestamp,
                "host_error": random.choice([0, 1]),
                "ac_error": random.choice([0, 1]),
                "dc_error": random.choice([0, 1]),
                "terminal_error": random.choice([0, 1]),
                "storage_error": random.choice([0, 1]),
            })
        
        return {
            "station_id": station_id,
            "host_error": [d["host_error"] for d in window_data],
            "ac_error": [d["ac_error"] for d in window_data],
            "dc_error": [d["dc_error"] for d in window_data],
            "terminal_error": [d["terminal_error"] for d in window_data],
            "storage_error": [d["storage_error"] for d in window_data],
            "timestamps": [d["timestamp"] for d in window_data],
        }
    
    def generate_device_host(self, host_id: str, window_size: int = 100) -> Dict[str, Any]:
        """生成主机数据窗口（1秒或15秒间隔）"""
        base_time = time.time()
        window_data = []
        
        for i in range(window_size):
            timestamp = base_time - (window_size - i) * random.choice([1, 15])
            window_data.append({
                "timestamp": timestamp,
                "acdc_status": random.choice([0, 1, 2]),  # 0停机，1运行，2故障
                "dcdc_input_power": round(random.uniform(10.0, 200.0), 2),
                "acdc_input_power": round(random.uniform(10.0, 200.0), 2),
            })
        
        return {
            "host_id": host_id,
            "acdc_status": [d["acdc_status"] for d in window_data],
            "dcdc_input_power": [d["dcdc_input_power"] for d in window_data],
            "acdc_input_power": [d["acdc_input_power"] for d in window_data],
            "timestamps": [d["timestamp"] for d in window_data],
        }
    
    def generate_device_storage(self, host_id: str, window_size: int = 100) -> Dict[str, Any]:
        """生成储能数据窗口（15秒间隔）"""
        base_time = time.time()
        window_data = []
        
        for i in range(window_size):
            timestamp = base_time - (window_size - i) * 15
            window_data.append({
                "timestamp": timestamp,
                "storage_power": round(random.uniform(-100.0, 100.0), 2),
                "storage_current": round(random.uniform(-50.0, 50.0), 2),
                "storage_temp_max": round(random.uniform(25.0, 45.0), 2),
                "storage_temp_min": round(random.uniform(20.0, 35.0), 2),
                "storage_SOC": round(random.uniform(20.0, 100.0), 2),
                "storage_SOH": round(random.uniform(85.0, 100.0), 2),
            })
        
        return {
            "host_id": host_id,
            "storage_id": f"storage_{random.randint(1, 4):03d}",
            "storage_power": [d["storage_power"] for d in window_data],
            "storage_current": [d["storage_current"] for d in window_data],
            "storage_temp_max": [d["storage_temp_max"] for d in window_data],
            "storage_temp_min": [d["storage_temp_min"] for d in window_data],
            "storage_SOC": [d["storage_SOC"] for d in window_data],
            "storage_SOH": [d["storage_SOH"] for d in window_data],
            "timestamps": [d["timestamp"] for d in window_data],
        }


class MockProducer:
    """模拟Kafka生产者，持续发送测试数据"""
    
    def __init__(self, kafka_config: Optional[Dict] = None, topic_detail: Optional[Dict] = None):
        self.kafka_config = kafka_config or KAFKA_CONFIG
        self.topic_detail = topic_detail or TOPIC_DETAIL
        self.generator = MockDataGenerator()
        self.producer: Optional[AsyncKafkaProducerClient] = None
        self.running = False
        
    async def start(self):
        """启动生产者"""
        self.producer = AsyncKafkaProducerClient(self.kafka_config)
        await self.producer.start()
        logging.info("Mock producer started")
        
    async def stop(self):
        """停止生产者"""
        self.running = False
        if self.producer:
            await self.producer.stop()
        logging.info("Mock producer stopped")
        
    async def produce_topic_data(self, topic: str, data: Dict):
        """发送单条topic数据"""
        if not self.producer:
            raise RuntimeError("Producer not started")
        try:
            await self.producer.send(topic, data)
            logging.info(f"Sent data to topic {topic}: {list(data.keys())}")
        except Exception as e:
            logging.error(f"Failed to send to {topic}: {e}")
            
    async def run_continuous(self, duration_seconds: int = 60, interval_seconds: int = 5):
        """持续运行指定时长，定期发送各topic数据"""
        self.running = True
        start_time = time.time()
        
        logging.info(f"Starting continuous mock production for {duration_seconds}s, interval={interval_seconds}s")
        
        iteration = 0
        while self.running and (time.time() - start_time) < duration_seconds:
            iteration += 1
            logging.info(f"\n=== Iteration {iteration} ===")
            
            # 为每个场站生成数据
            for station_id in self.generator.station_ids:
                # 场站参数（低频）
                if iteration % 10 == 1:  # 每10次发送一次
                    data = self.generator.generate_station_param(station_id)
                    await self.produce_topic_data("SCHEDULE-STATION-PARAM", data)
                
                # 场站实时数据
                data = self.generator.generate_station_realtime_data(station_id, window_size=50)
                await self.produce_topic_data("SCHEDULE-STATION-REALTIME-DATA", data)
                
                # 订单数据
                data = self.generator.generate_car_order(station_id, window_size=30)
                await self.produce_topic_data("SCHEDULE-CAR-ORDER", data)
                
                # 电价数据（低频）
                if iteration % 20 == 1:
                    data = self.generator.generate_car_price(station_id)
                    await self.produce_topic_data("SCHEDULE-CAR-PRICE", data)
                
                # 设备错误（随机触发）
                if random.random() < 0.2:
                    data = self.generator.generate_device_error(station_id, window_size=5)
                    await self.produce_topic_data("SCHEDULE-DEVICE-ERROR", data)
            
            # 环境日历（低频）
            if iteration % 30 == 1:
                data = self.generator.generate_environment_calendar()
                await self.produce_topic_data("SCHEDULE-ENVIRONMENT-CALENDAR", data)
            
            # 设备数据
            for host_id in self.generator.host_ids:
                # 主机数据
                data = self.generator.generate_device_host(host_id, window_size=40)
                await self.produce_topic_data("SCHEDULE-DEVICE-HOST", data)
                
                # 充电枪数据
                data = self.generator.generate_device_gun(host_id, window_size=40)
                await self.produce_topic_data("SCHEDULE-DEVICE-GUN", data)
                
                # 储能数据
                data = self.generator.generate_device_storage(host_id, window_size=40)
                await self.produce_topic_data("SCHEDULE-DEVICE-STORAGE", data)
            
            # 电表数据
            for meter_id in self.generator.meter_ids:
                data = self.generator.generate_device_meter(meter_id, window_size=40)
                await self.produce_topic_data("SCHEDULE-DEVICE-METER", data)
            
            # 等待下一次迭代
            await asyncio.sleep(interval_seconds)
        
        logging.info(f"Completed {iteration} iterations")


async def test_mock_producer_basic():
    """基础测试：验证数据生成器"""
    generator = MockDataGenerator()
    
    # 测试各类数据生成
    station_param = generator.generate_station_param("test_station")
    assert "station_id" in station_param
    assert station_param["station_id"] == "test_station"
    
    realtime = generator.generate_station_realtime_data("test_station", window_size=10)
    assert "station_id" in realtime
    assert len(realtime["history_curve_station_avg"]) == 10
    
    meter = generator.generate_device_meter("meter_001", window_size=20)
    assert len(meter["current_power"]) == 20
    
    logging.info("All data generation tests passed")


async def test_mock_producer_kafka():
    """集成测试：实际发送到Kafka（需要Kafka可用）"""
    producer = MockProducer()
    
    try:
        await producer.start()
        # 运行30秒，每5秒一次
        await producer.run_continuous(duration_seconds=30, interval_seconds=5)
    except Exception as e:
        logging.error(f"Mock producer test failed: {e}")
        raise
    finally:
        await producer.stop()


async def main():
    """主函数：运行模拟生产者"""
    logging.info("Starting mock Kafka producer...")
    
    # 运行基础测试
    await test_mock_producer_basic()
    
    # 运行实际生产者（注释掉以避免需要真实Kafka）
    # await test_mock_producer_kafka()
    
    # 或者直接运行持续生产
    producer = MockProducer()
    try:
        await producer.start()
        # 运行5分钟，每10秒发送一批数据
        await producer.run_continuous(duration_seconds=300, interval_seconds=10)
    except KeyboardInterrupt:
        logging.info("Interrupted by user")
    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
