#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-11-24
project: data_analysis
filename: test_kafka_consume.py
version: 2.0
description: 重构后的Kafka消费测试（应用模板方法模式）
"""

import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Dict, List, Any

# 添加项目根目录到Python路径
if __name__ == "__main__":
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from d_a.config import TOPIC_DETAIL, KAFKA_CONFIG, MODULE_TO_TOPICS
from d_a.analysis_service import AsyncDataAnalysisService
from tests.fixtures import AsyncTopicTester, TestReportFormatter

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s"
)


class TopicConsumeTester(AsyncTopicTester):
    """扩展的Topic消费测试器"""
    
    async def test_service_integration(self, module_name: str, 
                                      duration_seconds: int = 30) -> Dict[str, Any]:
        """测试完整的服务集成"""
        logging.info(f"Testing service integration for module: {module_name}")
        
        callback_invocations = []
        
        async def test_callback(station_id, module_input):
            callback_invocations.append({
                "station_id": station_id,
                "timestamp": time.time(),
                "input_keys": list(module_input.keys()) if module_input else [],
                "has_data": module_input is not None
            })
            logging.info(f"Callback invoked for station {station_id}, "
                        f"input keys: {list(module_input.keys()) if module_input else 'None'}")
            return {"test_result": "success"}
        
        service = AsyncDataAnalysisService(module_name=module_name)
        
        try:
            await service.start(callback=test_callback)
            
            start_time = time.time()
            while time.time() - start_time < duration_seconds:
                await asyncio.sleep(2)
                status = service.get_station_status()
                if status:
                    logging.info(f"Active stations: {len(status)}")
            
            success = len(callback_invocations) > 0
            
            if success:
                logging.info(f"✓ Service integration test passed: "
                           f"{len(callback_invocations)} callbacks invoked")
                logging.info(f"  Stations processed: "
                           f"{set(inv['station_id'] for inv in callback_invocations)}")
            else:
                logging.warning(f"⚠ Service integration test: no callbacks invoked")
            
            return {
                "success": success,
                "callback_count": len(callback_invocations),
                "stations": list(set(inv['station_id'] for inv in callback_invocations)),
                "invocations": callback_invocations,
                "duration": duration_seconds,
            }
            
        finally:
            await service.stop()


async def test_kafka_connectivity() -> bool:
    """测试Kafka连接性"""
    logging.info("Testing Kafka connectivity...")
    
    try:
        from d_a.kafka_client import AsyncKafkaConsumerClient
        
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


async def test_all_topics():
    """测试所有配置的topic"""
    tester = TopicConsumeTester(KAFKA_CONFIG)
    topics = list(TOPIC_DETAIL.keys())
    
    logging.info(f"Testing {len(topics)} topics from TOPIC_DETAIL")
    
    results = await tester.test_multiple_topics(topics, timeout_per_topic=20)
    
    tester.print_summary()
    
    return results, tester


async def test_module_topics(module_name: str):
    """测试特定模块需要的所有topic"""
    tester = TopicConsumeTester(KAFKA_CONFIG)
    topics = MODULE_TO_TOPICS.get(module_name, [])
    
    if not topics:
        logging.warning(f"No topics configured for module: {module_name}")
        return None, tester
    
    logging.info(f"Testing {len(topics)} topics for module: {module_name}")
    logging.info(f"Topics: {topics}")
    
    results = await tester.test_multiple_topics(topics, timeout_per_topic=20)
    
    tester.print_summary()
    
    return results, tester


async def quick_test(timeout: int = 10, num_topics: int = 5):
    """快速测试前N个topic"""
    tester = TopicConsumeTester(KAFKA_CONFIG)
    topics = list(TOPIC_DETAIL.keys())[:num_topics]
    
    logging.info(f"Quick test: checking first {len(topics)} topics")
    results = await tester.test_multiple_topics(topics, timeout_per_topic=timeout)
    
    tester.print_summary()
    
    return results, tester


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
    
    # 2. 测试所有topic
    logging.info("\n" + "="*80)
    logging.info("STEP 2: Testing All Topics")
    logging.info("="*80)
    
    results, tester = await test_all_topics()
    
    # 3. 测试特定模块
    logging.info("\n" + "="*80)
    logging.info("STEP 3: Testing Module-Specific Topics")
    logging.info("="*80)
    
    test_module = "load_prediction"
    module_results, module_tester = await test_module_topics(test_module)
    
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
    
    # 使用格式化器生成报告
    report = TestReportFormatter.format_summary(tester, {
        "Integration test": "Passed" if integration_result['success'] else "Failed",
        "Callback count": integration_result['callback_count'],
    })
    logging.info(f"\n{report}")


if __name__ == "__main__":
    asyncio.run(main())
