#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-11-24
project: data_analysis
filename: test_mock_producer.py
version: 2.0
description: 重构后的模拟数据生产者（应用工厂模式和策略模式）
"""

import asyncio
import logging
import random
import sys
import time
from pathlib import Path
from typing import Dict, List, Any, Optional

# 添加项目根目录到Python路径
if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from d_a.config import TOPIC_DETAIL, KAFKA_CONFIG
from d_a.kafka_client import AsyncKafkaProducerClient
from tests.fixtures import DataGeneratorFactory

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s"
)


class MockDataGenerator:
    """模拟数据生成器（使用工厂模式重构）"""
    
    def __init__(self):
        self.factory = DataGeneratorFactory()
        self.station_ids = self.factory.station_ids
        self.host_ids = self.factory.host_ids
        self.meter_ids = self.factory.meter_ids
        self.gun_ids = self.factory.gun_ids
    
    def generate_station_param(self, station_id: str) -> Dict[str, Any]:
        """生成场站参数数据"""
        return self.factory.generate("station_param", station_id)
    
    def generate_station_realtime_data(self, station_id: str, window_size: int = 100) -> Dict[str, Any]:
        """生成场站实时数据窗口"""
        return self.factory.generate("station_realtime", station_id, window_size)
    
    def generate_environment_calendar(self) -> Dict[str, Any]:
        """生成环境日历数据"""
        return self.factory.generate("environment_calendar", "global")
    
    def generate_device_meter(self, meter_id: str, window_size: int = 100) -> Dict[str, Any]:
        """生成电表数据窗口"""
        return self.factory.generate("device_meter", meter_id, window_size)
    
    def generate_device_gun(self, host_id: str, window_size: int = 100) -> Dict[str, Any]:
        """生成充电枪数据窗口"""
        return self.factory.generate("device_gun", host_id, window_size)
    
    def generate_car_order(self, station_id: str, window_size: int = 100) -> Dict[str, Any]:
        """生成订单数据窗口"""
        return self.factory.generate("car_order", station_id, window_size)
    
    def generate_car_price(self, station_id: str) -> Dict[str, Any]:
        """生成电价数据"""
        return self.factory.generate("car_price", station_id)
    
    def generate_device_error(self, station_id: str, window_size: int = 10) -> Dict[str, Any]:
        """生成设备错误数据窗口"""
        return self.factory.generate("device_error", station_id, window_size)
    
    def generate_device_host(self, host_id: str, window_size: int = 100) -> Dict[str, Any]:
        """生成主机数据窗口"""
        return self.factory.generate("device_host", host_id, window_size)
    
    def generate_device_storage(self, host_id: str, window_size: int = 100) -> Dict[str, Any]:
        """生成储能数据窗口"""
        return self.factory.generate("device_storage", host_id, window_size)


class MockProducer:
    """模拟Kafka生产者"""
    
    def __init__(self, kafka_config: Optional[Dict] = None, topic_detail: Optional[Dict] = None):
        self.kafka_config = kafka_config or KAFKA_CONFIG
        self.topic_detail = topic_detail or TOPIC_DETAIL
        self.generator = MockDataGenerator()
        self.producer: Optional[AsyncKafkaProducerClient] = None
        self.running = False
        
        # 发送策略配置（低频、中频、高频）
        self.send_strategies = {
            "low_freq": 10,      # 每10次迭代发送1次
            "medium_freq": 5,    # 每5次迭代发送1次
            "high_freq": 1,      # 每次迭代都发送
            "random": -1,        # 随机触发
        }
        
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
            logging.debug(f"Sent data to topic {topic}: {list(data.keys())}")
        except Exception as e:
            logging.error(f"Failed to send to {topic}: {e}")
    
    def should_send(self, iteration: int, strategy: str, probability: float = 0.2) -> bool:
        """判断是否应该发送数据"""
        if strategy == "random":
            return random.random() < probability
        
        freq = self.send_strategies.get(strategy, 1)
        return iteration % freq == 1
    
    async def run_continuous(self, duration_seconds: int = 60, interval_seconds: int = 5):
        """持续运行指定时长，定期发送各topic数据"""
        self.running = True
        start_time = time.time()
        
        logging.info(f"Starting continuous mock production for {duration_seconds}s, interval={interval_seconds}s")
        
        iteration = 0
        while self.running and (time.time() - start_time) < duration_seconds:
            iteration += 1
            logging.info(f"\n=== Iteration {iteration} ===")
            
            await self._produce_station_data(iteration)
            await self._produce_device_data(iteration)
            await self._produce_environment_data(iteration)
            
            await asyncio.sleep(interval_seconds)
        
        logging.info(f"Completed {iteration} iterations")
    
    async def _produce_station_data(self, iteration: int):
        """生成场站相关数据"""
        for station_id in self.generator.station_ids:
            # 场站参数（低频）
            if self.should_send(iteration, "low_freq"):
                data = self.generator.generate_station_param(station_id)
                await self.produce_topic_data("SCHEDULE-STATION-PARAM", data)
            
            # 场站实时数据（高频）
            if self.should_send(iteration, "high_freq"):
                data = self.generator.generate_station_realtime_data(station_id, window_size=50)
                await self.produce_topic_data("SCHEDULE-STATION-REALTIME-DATA", data)
            
            # 订单数据（高频）
            if self.should_send(iteration, "high_freq"):
                data = self.generator.generate_car_order(station_id, window_size=30)
                await self.produce_topic_data("SCHEDULE-CAR-ORDER", data)
            
            # 电价数据（低频）
            if self.should_send(iteration, "low_freq", probability=0.05):
                data = self.generator.generate_car_price(station_id)
                await self.produce_topic_data("SCHEDULE-CAR-PRICE", data)
            
            # 设备错误（随机触发）
            if self.should_send(iteration, "random", probability=0.2):
                data = self.generator.generate_device_error(station_id, window_size=5)
                await self.produce_topic_data("SCHEDULE-DEVICE-ERROR", data)
    
    async def _produce_device_data(self, iteration: int):
        """生成设备相关数据"""
        # 主机、充电枪、储能数据（高频）
        for host_id in self.generator.host_ids:
            if self.should_send(iteration, "high_freq"):
                data = self.generator.generate_device_host(host_id, window_size=40)
                await self.produce_topic_data("SCHEDULE-DEVICE-HOST", data)
                
                data = self.generator.generate_device_gun(host_id, window_size=40)
                await self.produce_topic_data("SCHEDULE-DEVICE-GUN", data)
                
                data = self.generator.generate_device_storage(host_id, window_size=40)
                await self.produce_topic_data("SCHEDULE-DEVICE-STORAGE", data)
        
        # 电表数据（高频）
        for meter_id in self.generator.meter_ids:
            if self.should_send(iteration, "high_freq"):
                data = self.generator.generate_device_meter(meter_id, window_size=40)
                await self.produce_topic_data("SCHEDULE-DEVICE-METER", data)
    
    async def _produce_environment_data(self, iteration: int):
        """生成环境数据"""
        # 环境日历（极低频）
        if self.should_send(iteration, "low_freq", probability=0.03):
            data = self.generator.generate_environment_calendar()
            await self.produce_topic_data("SCHEDULE-ENVIRONMENT-CALENDAR", data)


async def test_mock_producer_basic():
    """基础测试：验证数据生成器"""
    generator = MockDataGenerator()
    
    logging.info("Testing data generation...")
    
    # 测试各类数据生成
    tests = [
        ("station_param", lambda: generator.generate_station_param("test_station")),
        ("station_realtime", lambda: generator.generate_station_realtime_data("test_station", 10)),
        ("device_meter", lambda: generator.generate_device_meter("meter_001", 20)),
        ("device_gun", lambda: generator.generate_device_gun("host_001", 20)),
        ("car_order", lambda: generator.generate_car_order("test_station", 30)),
        ("car_price", lambda: generator.generate_car_price("test_station")),
        ("device_error", lambda: generator.generate_device_error("test_station", 5)),
        ("device_host", lambda: generator.generate_device_host("host_001", 20)),
        ("device_storage", lambda: generator.generate_device_storage("host_001", 20)),
        ("environment_calendar", lambda: generator.generate_environment_calendar()),
    ]
    
    for name, func in tests:
        data = func()
        assert data is not None, f"{name} generation failed"
        logging.info(f"✓ {name}: generated {len(data)} fields")
    
    logging.info("All data generation tests passed")


async def test_mock_producer_kafka():
    """集成测试：实际发送到Kafka"""
    producer = MockProducer()
    
    try:
        await producer.start()
        await producer.run_continuous(duration_seconds=30, interval_seconds=5)
    except Exception as e:
        logging.error(f"Mock producer test failed: {e}")
        raise
    finally:
        await producer.stop()


async def main():
    """主函数"""
    logging.info("Starting mock Kafka producer...")
    
    # 运行基础测试
    await test_mock_producer_basic()
    
    # 运行持续生产
    producer = MockProducer()
    try:
        await producer.start()
        await producer.run_continuous(duration_seconds=300, interval_seconds=10)
    except KeyboardInterrupt:
        logging.info("Interrupted by user")
    except Exception as e:
        logging.error(f"Error: {e}", exc_info=True)
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
