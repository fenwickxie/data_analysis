#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-11-06
project: data_analysis
filename: test_kafka_consume.py
version: 1.0
"""

import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Dict, Set, List, Optional
from collections import defaultdict

# 添加项目根目录到Python路径
if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from d_a.config import TOPIC_DETAIL, KAFKA_CONFIG, MODULE_TO_TOPICS
from d_a.kafka_client import AsyncKafkaConsumerClient, KafkaConsumerClient
from d_a.analysis_service import AsyncDataAnalysisService

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s"
)


class TopicConsumeTester:
    """测试各个topic的消费功能"""
    
    def __init__(self, kafka_config: Optional[Dict] = None):
        self.kafka_config = kafka_config or KAFKA_CONFIG
        self.consumed_topics: Set[str] = set()
        self.topic_message_counts: Dict[str, int] = defaultdict(int)
        self.topic_sample_data: Dict[str, List] = defaultdict(list)
        self.errors: List[str] = []
        
    async def test_single_topic_async(self, topic: str, timeout_seconds: int = 30) -> bool:
        """异步测试单个topic的消费"""
        logging.info(f"Testing async consumption for topic: {topic}")
        
        try:
            consumer = AsyncKafkaConsumerClient([topic], self.kafka_config)
            await consumer.start()
            
            start_time = time.time()
            messages_received = 0
            
            try:
                while time.time() - start_time < timeout_seconds:
                    try:
                        batch = await consumer.getmany(timeout_ms=2000)
                        
                        if batch:
                            for msg in batch:
                                messages_received += 1
                                self.topic_message_counts[topic] += 1
                                
                                # 保存前3条消息作为样本
                                if len(self.topic_sample_data[topic]) < 3:
                                    self.topic_sample_data[topic].append({
                                        "topic": msg.topic,
                                        "partition": msg.partition,
                                        "offset": msg.offset,
                                        "timestamp": msg.timestamp,
                                        "value_keys": list(msg.value.keys()) if isinstance(msg.value, dict) else None,
                                        "value_sample": str(msg.value)[:200]  # 截取前200字符
                                    })
                                
                                logging.debug(f"Received message from {topic}: offset={msg.offset}")
                            
                            self.consumed_topics.add(topic)
                            logging.info(f"✓ Topic {topic}: received {messages_received} messages")
                            return True
                        
                        await asyncio.sleep(0.5)
                        
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        error_msg = f"Error consuming from {topic}: {e}"
                        logging.error(error_msg)
                        self.errors.append(error_msg)
                        return False
                
                if messages_received == 0:
                    warning_msg = f"⚠ Topic {topic}: no messages received within {timeout_seconds}s"
                    logging.warning(warning_msg)
                    self.errors.append(warning_msg)
                    return False
                
                return True
                    
            finally:
                await consumer.stop()
                
        except Exception as e:
            error_msg = f"Failed to connect to topic {topic}: {e}"
            logging.error(error_msg)
            self.errors.append(error_msg)
            return False
    
    def test_single_topic_sync(self, topic: str, timeout_seconds: int = 30) -> bool:
        """同步测试单个topic的消费"""
        logging.info(f"Testing sync consumption for topic: {topic}")
        
        try:
            consumer = KafkaConsumerClient([topic], self.kafka_config)
            
            start_time = time.time()
            messages_received = 0
            
            try:
                while time.time() - start_time < timeout_seconds:
                    try:
                        msg_pack = consumer.poll(timeout_ms=2000)
                        
                        if msg_pack:
                            for tp, msgs in msg_pack.items():
                                for msg in msgs:
                                    messages_received += 1
                                    self.topic_message_counts[topic] += 1
                                    
                                    # 保存前3条消息作为样本
                                    if len(self.topic_sample_data[topic]) < 3:
                                        self.topic_sample_data[topic].append({
                                            "topic": tp.topic,
                                            "partition": tp.partition,
                                            "offset": msg.offset,
                                            "timestamp": msg.timestamp,
                                            "value_keys": list(msg.value.keys()) if isinstance(msg.value, dict) else None,
                                            "value_sample": str(msg.value)[:200]
                                        })
                                    
                                    logging.debug(f"Received message from {topic}: offset={msg.offset}")
                            
                            self.consumed_topics.add(topic)
                            logging.info(f"✓ Topic {topic}: received {messages_received} messages")
                            return True
                        
                        time.sleep(0.5)
                        
                    except Exception as e:
                        error_msg = f"Error consuming from {topic}: {e}"
                        logging.error(error_msg)
                        self.errors.append(error_msg)
                        return False
                
                if messages_received == 0:
                    warning_msg = f"⚠ Topic {topic}: no messages received within {timeout_seconds}s"
                    logging.warning(warning_msg)
                    self.errors.append(warning_msg)
                    return False
                
                return True
                    
            finally:
                consumer.close()
                
        except Exception as e:
            error_msg = f"Failed to connect to topic {topic}: {e}"
            logging.error(error_msg)
            self.errors.append(error_msg)
            return False
    
    async def test_all_topics_async(self, topics: List[str], timeout_per_topic: int = 30):
        """异步测试所有topic"""
        logging.info(f"Starting async test for {len(topics)} topics")
        
        results = {}
        for topic in topics:
            result = await self.test_single_topic_async(topic, timeout_per_topic)
            results[topic] = result
            await asyncio.sleep(1)  # 间隔1秒避免过载
        
        return results
    
    def test_all_topics_sync(self, topics: List[str], timeout_per_topic: int = 30):
        """同步测试所有topic"""
        logging.info(f"Starting sync test for {len(topics)} topics")
        
        results = {}
        for topic in topics:
            result = self.test_single_topic_sync(topic, timeout_per_topic)
            results[topic] = result
            time.sleep(1)  # 间隔1秒避免过载
        
        return results
    
    async def test_service_integration(self, module_name: str, duration_seconds: int = 30):
        """测试完整的服务集成（消费 -> dispatcher -> callback）"""
        logging.info(f"Testing service integration for module: {module_name}")
        
        callback_invocations = []
        
        async def test_callback(station_id, module_input):
            callback_invocations.append({
                "station_id": station_id,
                "timestamp": time.time(),
                "input_keys": list(module_input.keys()) if module_input else [],
                "has_data": module_input is not None
            })
            logging.info(f"Callback invoked for station {station_id}, input keys: {list(module_input.keys()) if module_input else 'None'}")
            return {"test_result": "success"}
        
        service = AsyncDataAnalysisService(module_name=module_name)
        
        try:
            await service.start(callback=test_callback)
            
            # 运行指定时长
            start_time = time.time()
            while time.time() - start_time < duration_seconds:
                await asyncio.sleep(2)
                status = service.get_station_status()
                if status:
                    logging.info(f"Active stations: {len(status)}")
            
            # 检查结果
            success = len(callback_invocations) > 0
            
            if success:
                logging.info(f"✓ Service integration test passed: {len(callback_invocations)} callbacks invoked")
                logging.info(f"  Stations processed: {set(inv['station_id'] for inv in callback_invocations)}")
            else:
                logging.warning(f"⚠ Service integration test: no callbacks invoked")
            
            return {
                "success": success,
                "callback_count": len(callback_invocations),
                "stations": list(set(inv['station_id'] for inv in callback_invocations)),
                "invocations": callback_invocations
            }
            
        finally:
            await service.stop()
    
    def print_summary(self):
        """打印测试摘要"""
        logging.info("\n" + "="*80)
        logging.info("TOPIC CONSUMPTION TEST SUMMARY")
        logging.info("="*80)
        
        logging.info(f"\nTotal topics tested: {len(self.topic_message_counts)}")
        logging.info(f"Topics with data: {len(self.consumed_topics)}")
        logging.info(f"Topics without data: {len(self.topic_message_counts) - len(self.consumed_topics)}")
        
        if self.consumed_topics:
            logging.info("\n✓ Topics successfully consumed:")
            for topic in sorted(self.consumed_topics):
                count = self.topic_message_counts[topic]
                logging.info(f"  - {topic}: {count} messages")
        
        missing_topics = set(self.topic_message_counts.keys()) - self.consumed_topics
        if missing_topics:
            logging.info("\n⚠ Topics with no data:")
            for topic in sorted(missing_topics):
                logging.info(f"  - {topic}")
        
        if self.errors:
            logging.info(f"\n✗ Errors encountered: {len(self.errors)}")
            for error in self.errors[:10]:  # 只显示前10个错误
                logging.info(f"  - {error}")
        
        if self.topic_sample_data:
            logging.info("\n📋 Sample messages:")
            for topic, samples in list(self.topic_sample_data.items())[:5]:  # 只显示5个topic的样本
                logging.info(f"\n  Topic: {topic}")
                for i, sample in enumerate(samples[:2], 1):  # 每个topic显示2条
                    logging.info(f"    Message {i}:")
                    if sample.get('value_keys'):
                        logging.info(f"      Keys: {sample['value_keys']}")
                    logging.info(f"      Sample: {sample['value_sample']}")
        
        logging.info("\n" + "="*80)


async def test_async_all_topics():
    """测试所有配置的topic（异步）"""
    tester = TopicConsumeTester()
    topics = list(TOPIC_DETAIL.keys())
    
    logging.info(f"Testing {len(topics)} topics from TOPIC_DETAIL")
    
    results = await tester.test_all_topics_async(topics, timeout_per_topic=20)
    
    tester.print_summary()
    
    return results, tester


def test_sync_all_topics():
    """测试所有配置的topic（同步）"""
    tester = TopicConsumeTester()
    topics = list(TOPIC_DETAIL.keys())
    
    logging.info(f"Testing {len(topics)} topics from TOPIC_DETAIL")
    
    results = tester.test_all_topics_sync(topics, timeout_per_topic=20)
    
    tester.print_summary()
    
    return results, tester


async def test_module_topics(module_name: str):
    """测试特定模块需要的所有topic"""
    tester = TopicConsumeTester()
    topics = MODULE_TO_TOPICS.get(module_name, [])
    
    if not topics:
        logging.warning(f"No topics configured for module: {module_name}")
        return None, tester
    
    logging.info(f"Testing {len(topics)} topics for module: {module_name}")
    logging.info(f"Topics: {topics}")
    
    results = await tester.test_all_topics_async(topics, timeout_per_topic=20)
    
    tester.print_summary()
    
    return results, tester


async def test_specific_topics(topics: List[str], timeout_per_topic: int = 30):
    """测试指定的topic列表"""
    tester = TopicConsumeTester()
    
    logging.info(f"Testing {len(topics)} specified topics")
    
    results = await tester.test_all_topics_async(topics, timeout_per_topic)
    
    tester.print_summary()
    
    return results, tester


async def test_kafka_connectivity():
    """快速测试Kafka连接性"""
    logging.info("Testing Kafka connectivity...")
    
    try:
        # 尝试连接第一个topic
        first_topic = list(TOPIC_DETAIL.keys())[0]
        consumer = AsyncKafkaConsumerClient([first_topic], KAFKA_CONFIG)
        await consumer.start()
        
        logging.info(f"✓ Successfully connected to Kafka")
        logging.info(f"  Bootstrap servers: {KAFKA_CONFIG['bootstrap_servers']}")
        logging.info(f"  Test topic: {first_topic}")
        
        await consumer.stop()
        return True
        
    except Exception as e:
        logging.error(f"✗ Failed to connect to Kafka: {e}")
        return False


async def main():
    """主测试函数"""
    logging.info("Starting Kafka Topic Consumption Tests")
    logging.info(f"Kafka servers: {KAFKA_CONFIG['bootstrap_servers']}")
    
    # 1. 测试连接性
    logging.info("\n" + "="*80)
    logging.info("STEP 1: Testing Kafka Connectivity")
    logging.info("="*80)
    
    if not await test_kafka_connectivity():
        logging.error("Cannot proceed without Kafka connection")
        return
    
    # 2. 测试所有topic（异步）
    logging.info("\n" + "="*80)
    logging.info("STEP 2: Testing All Topics (Async)")
    logging.info("="*80)
    
    results, tester = await test_async_all_topics()
    
    # 3. 测试特定模块
    logging.info("\n" + "="*80)
    logging.info("STEP 3: Testing Module-Specific Topics")
    logging.info("="*80)
    
    test_module = "load_prediction"
    await test_module_topics(test_module)
    
    # 4. 测试服务集成
    logging.info("\n" + "="*80)
    logging.info("STEP 4: Testing Service Integration")
    logging.info("="*80)
    
    integration_result = await tester.test_service_integration(
        module_name=test_module,
        duration_seconds=30
    )
    
    logging.info("\nIntegration test result:")
    logging.info(f"  Success: {integration_result['success']}")
    logging.info(f"  Callbacks: {integration_result['callback_count']}")
    logging.info(f"  Stations: {integration_result['stations']}")
    
    # 最终总结
    logging.info("\n" + "="*80)
    logging.info("FINAL SUMMARY")
    logging.info("="*80)
    
    total_topics = len(TOPIC_DETAIL)
    consumed_topics = len(tester.consumed_topics)
    success_rate = (consumed_topics / total_topics * 100) if total_topics > 0 else 0
    
    logging.info(f"Topics tested: {total_topics}")
    logging.info(f"Topics with data: {consumed_topics}")
    logging.info(f"Success rate: {success_rate:.1f}%")
    logging.info(f"Total messages received: {sum(tester.topic_message_counts.values())}")
    
    if success_rate < 50:
        logging.warning("⚠ Less than 50% of topics have data!")
    elif success_rate < 100:
        logging.info("⚠ Some topics missing data")
    else:
        logging.info("✓ All topics have data!")


# 便捷测试函数
async def quick_test(timeout: int = 10):
    """快速测试（较短超时时间）"""
    tester = TopicConsumeTester()
    topics = list(TOPIC_DETAIL.keys())[:5]  # 只测试前5个topic
    
    logging.info(f"Quick test: checking first {len(topics)} topics")
    results = await tester.test_all_topics_async(topics, timeout_per_topic=timeout)
    tester.print_summary()
    
    return results, tester


if __name__ == "__main__":
    # 运行完整测试
    asyncio.run(main())
    
    # 或者运行快速测试
    # asyncio.run(quick_test(timeout=10))
