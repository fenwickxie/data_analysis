#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-11-24
project: data_analysis
filename: test_utils.py
version: 2.0
description: 测试工具基础类，应用模板方法模式
"""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Set
from collections import defaultdict


class TopicTesterTemplate(ABC):
    """Topic测试模板（Template Method Pattern）"""
    
    def __init__(self, kafka_config: Dict):
        self.kafka_config = kafka_config
        self.consumed_topics: Set[str] = set()
        self.topic_message_counts: Dict[str, int] = defaultdict(int)
        self.topic_sample_data: Dict[str, List] = defaultdict(list)
        self.errors: List[str] = []
    
    @abstractmethod
    def create_consumer(self, topics: List[str]):
        """创建消费者（子类实现）"""
        pass
    
    @abstractmethod
    async def consume_messages(self, consumer, topic: str, timeout_seconds: int) -> int:
        """消费消息（子类实现）"""
        pass
    
    @abstractmethod
    async def close_consumer(self, consumer):
        """关闭消费者（子类实现）"""
        pass
    
    async def test_single_topic(self, topic: str, timeout_seconds: int = 30) -> bool:
        """测试单个topic（模板方法）"""
        logging.info(f"Testing topic: {topic}")
        
        try:
            consumer = self.create_consumer([topic])
            
            try:
                messages_received = await self.consume_messages(consumer, topic, timeout_seconds)
                
                if messages_received > 0:
                    self.consumed_topics.add(topic)
                    logging.info(f"✓ Topic {topic}: received {messages_received} messages")
                    return True
                else:
                    warning_msg = f"⚠ Topic {topic}: no messages within {timeout_seconds}s"
                    logging.warning(warning_msg)
                    self.errors.append(warning_msg)
                    return False
                    
            finally:
                await self.close_consumer(consumer)
                
        except Exception as e:
            error_msg = f"Failed to test topic {topic}: {e}"
            logging.error(error_msg)
            self.errors.append(error_msg)
            return False
    
    async def test_multiple_topics(self, topics: List[str], 
                                   timeout_per_topic: int = 30) -> Dict[str, bool]:
        """测试多个topic"""
        logging.info(f"Testing {len(topics)} topics")
        
        results = {}
        for topic in topics:
            result = await self.test_single_topic(topic, timeout_per_topic)
            results[topic] = result
            await asyncio.sleep(1)  # 避免过载
        
        return results
    
    def record_message(self, topic: str, msg_data: Dict[str, Any], max_samples: int = 3):
        """记录消息"""
        self.topic_message_counts[topic] += 1
        
        if len(self.topic_sample_data[topic]) < max_samples:
            self.topic_sample_data[topic].append(msg_data)
    
    def print_summary(self):
        """打印测试摘要"""
        logging.info("\n" + "="*80)
        logging.info("TOPIC CONSUMPTION TEST SUMMARY")
        logging.info("="*80)
        
        total_tested = len(self.topic_message_counts)
        with_data = len(self.consumed_topics)
        without_data = total_tested - with_data
        
        logging.info(f"\nTotal topics tested: {total_tested}")
        logging.info(f"Topics with data: {with_data}")
        logging.info(f"Topics without data: {without_data}")
        
        if total_tested > 0:
            success_rate = (with_data / total_tested) * 100
            logging.info(f"Success rate: {success_rate:.1f}%")
        
        total_messages = sum(self.topic_message_counts.values())
        logging.info(f"Total messages received: {total_messages}")
        
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
            for error in self.errors[:10]:
                logging.info(f"  - {error}")
        
        if self.topic_sample_data:
            logging.info("\n📋 Sample messages:")
            for topic, samples in list(self.topic_sample_data.items())[:5]:
                logging.info(f"\n  Topic: {topic}")
                for i, sample in enumerate(samples[:2], 1):
                    logging.info(f"    Message {i}:")
                    if sample.get('value_keys'):
                        logging.info(f"      Keys: {sample['value_keys']}")
                    sample_str = sample.get('value_sample', '')
                    if len(sample_str) > 100:
                        sample_str = sample_str[:100] + "..."
                    logging.info(f"      Sample: {sample_str}")
        
        logging.info("\n" + "="*80)


class AsyncTopicTester(TopicTesterTemplate):
    """异步Topic测试器"""
    
    def __init__(self, kafka_config: Dict):
        super().__init__(kafka_config)
        from d_a.kafka_client import AsyncKafkaConsumerClient
        self.consumer_class = AsyncKafkaConsumerClient
    
    def create_consumer(self, topics: List[str]):
        """创建异步消费者"""
        return self.consumer_class(topics, self.kafka_config)
    
    async def consume_messages(self, consumer, topic: str, timeout_seconds: int) -> int:
        """异步消费消息"""
        await consumer.start()
        
        start_time = time.time()
        messages_received = 0
        
        while time.time() - start_time < timeout_seconds:
            try:
                batch = await consumer.getmany(timeout_ms=2000)
                
                if batch:
                    for msg in batch:
                        messages_received += 1
                        
                        msg_data = {
                            "topic": msg.topic,
                            "partition": msg.partition,
                            "offset": msg.offset,
                            "timestamp": msg.timestamp,
                            "value_keys": list(msg.value.keys()) if isinstance(msg.value, dict) else None,
                            "value_sample": str(msg.value)[:200]
                        }
                        
                        self.record_message(topic, msg_data)
                        logging.debug(f"Received message from {topic}: offset={msg.offset}")
                
                await asyncio.sleep(0.5)
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logging.error(f"Error consuming: {e}")
                break
        
        return messages_received
    
    async def close_consumer(self, consumer):
        """关闭异步消费者"""
        await consumer.stop()


class SyncTopicTester(TopicTesterTemplate):
    """同步Topic测试器"""
    
    def __init__(self, kafka_config: Dict):
        super().__init__(kafka_config)
        from d_a.kafka_client import KafkaConsumerClient
        self.consumer_class = KafkaConsumerClient
    
    def create_consumer(self, topics: List[str]):
        """创建同步消费者"""
        return self.consumer_class(topics, self.kafka_config)
    
    async def consume_messages(self, consumer, topic: str, timeout_seconds: int) -> int:
        """同步消费消息（包装为async）"""
        start_time = time.time()
        messages_received = 0
        
        while time.time() - start_time < timeout_seconds:
            try:
                msg_pack = consumer.poll(timeout_ms=2000)
                
                if msg_pack:
                    for tp, msgs in msg_pack.items():
                        for msg in msgs:
                            messages_received += 1
                            
                            msg_data = {
                                "topic": tp.topic,
                                "partition": tp.partition,
                                "offset": msg.offset,
                                "timestamp": msg.timestamp,
                                "value_keys": list(msg.value.keys()) if isinstance(msg.value, dict) else None,
                                "value_sample": str(msg.value)[:200]
                            }
                            
                            self.record_message(topic, msg_data)
                            logging.debug(f"Received message from {topic}: offset={msg.offset}")
                
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logging.error(f"Error consuming: {e}")
                break
        
        return messages_received
    
    async def close_consumer(self, consumer):
        """关闭同步消费者"""
        consumer.close()


class TestReportFormatter:
    """测试报告格式化器"""
    
    @staticmethod
    def format_summary(tester: TopicTesterTemplate, additional_info: Optional[Dict] = None) -> str:
        """格式化测试摘要"""
        lines = []
        lines.append("="*80)
        lines.append("TEST SUMMARY REPORT")
        lines.append("="*80)
        
        total = len(tester.topic_message_counts)
        success = len(tester.consumed_topics)
        
        lines.append(f"\n📊 Statistics:")
        lines.append(f"  Total topics: {total}")
        lines.append(f"  Successful: {success}")
        lines.append(f"  Failed: {total - success}")
        lines.append(f"  Success rate: {(success/total*100):.1f}%" if total > 0 else "  Success rate: N/A")
        lines.append(f"  Total messages: {sum(tester.topic_message_counts.values())}")
        
        if additional_info:
            lines.append(f"\n📝 Additional Info:")
            for key, value in additional_info.items():
                lines.append(f"  {key}: {value}")
        
        if tester.errors:
            lines.append(f"\n❌ Errors ({len(tester.errors)}):")
            for error in tester.errors[:5]:
                lines.append(f"  - {error}")
        
        lines.append("="*80)
        
        return "\n".join(lines)
    
    @staticmethod
    def format_topic_details(tester: TopicTesterTemplate, topics: Optional[List[str]] = None) -> str:
        """格式化topic详情"""
        if topics is None:
            topics = sorted(tester.consumed_topics)
        
        lines = []
        lines.append("\n📋 Topic Details:")
        
        for topic in topics:
            count = tester.topic_message_counts.get(topic, 0)
            status = "✓" if topic in tester.consumed_topics else "✗"
            lines.append(f"  {status} {topic}: {count} messages")
            
            samples = tester.topic_sample_data.get(topic, [])
            if samples:
                first_sample = samples[0]
                if first_sample.get('value_keys'):
                    keys_str = ", ".join(first_sample['value_keys'][:5])
                    if len(first_sample['value_keys']) > 5:
                        keys_str += "..."
                    lines.append(f"      Keys: {keys_str}")
        
        return "\n".join(lines)
