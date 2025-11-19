#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:10:00
project: data_analysis
filename: kafka_client.py
version: 1.0
"""

# Kafka消费客户端封装
from kafka import KafkaConsumer
from kafka import KafkaProducer
import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import json
import logging
from collections import deque

from .errors import handle_error, KafkaConnectionError

def _extract_bootstrap_servers(config: dict, section: str):
    """Get bootstrap_servers from nested section or top-level for compatibility."""
    sec = config.get(section, {}) if isinstance(config, dict) else {}
    bs = None
    if isinstance(sec, dict):
        bs = sec.get("bootstrap_servers")
    return bs if bs is not None else config.get("bootstrap_servers")


def _allowed_subset(d: dict, allowed_keys: set):
    return {k: d[k] for k in allowed_keys if k in d}


class KafkaConsumerClient:
    def __init__(self, topics, config, max_retries=5, retry_interval=5):
        self.topics = topics
        self.config = config
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        self.consumer = None
        self._connect()

    def _connect(self):
        import time
        for i in range(self.max_retries):
            try:
                # Build compatible consumer kwargs (nested/flat)
                consumer_cfg = self.config.get('consumer', {}) if isinstance(self.config, dict) else {}
                bootstrap = _extract_bootstrap_servers(self.config, 'consumer')
                if not bootstrap:
                    raise KeyError("bootstrap_servers 未配置：请在顶层或consumer子配置中提供")

                # Allowed keys for kafka-python KafkaConsumer
                allowed = {
                    'group_id', 'auto_offset_reset', 'enable_auto_commit',
                    'max_poll_records', 'session_timeout_ms', 'request_timeout_ms',
                    'heartbeat_interval_ms', 'max_poll_interval_ms',
                    # security
                    'security_protocol', 'sasl_mechanism', 'sasl_plain_username', 'sasl_plain_password',
                    'ssl_cafile', 'ssl_certfile', 'ssl_keyfile',
                }
                base_kwargs = _allowed_subset(consumer_cfg, allowed)
                # fallback from top-level if not present
                for k in list(allowed):
                    if k not in base_kwargs and k in self.config:
                        base_kwargs[k] = self.config[k]

                self.consumer = KafkaConsumer(
                    *self.topics,
                    bootstrap_servers=bootstrap,
                    **base_kwargs,
                )
                return
            except Exception as e:
                if i == self.max_retries - 1:
                    handle_error(KafkaConnectionError(e), context="KafkaConsumer连接最终失败")
                    raise
                handle_error(KafkaConnectionError(e), context=f"KafkaConsumer连接失败，重试{i+1}/{self.max_retries}")
                time.sleep(self.retry_interval)

    def poll(self, timeout_ms=1000):
        try:
            if self.consumer is None:
                self._connect()
            if self.consumer is not None:
                return self.consumer.poll(timeout_ms=timeout_ms)
            else:
                handle_error(KafkaConnectionError("KafkaConsumer为None，无法poll"), context="poll失败")
                return {}
        except Exception as e:
            handle_error(e, context="KafkaConsumer poll异常，尝试重连")
            self._connect()
            if self.consumer is not None:
                return self.consumer.poll(timeout_ms=timeout_ms)
            else:
                handle_error(KafkaConnectionError("KafkaConsumer重连后仍为None"), context="poll失败")
                return {}

    def close(self):
        if self.consumer:
            try:
                self.consumer.close()
            except Exception as e:
                handle_error(e, context="KafkaConsumer关闭异常")
class KafkaProducerClient:
    def __init__(self, config, max_retries=5, retry_interval=5):
        self.config = config
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        self.producer = None
        self._connect()

    def _connect(self):
        import time
        for i in range(self.max_retries):
            try:
                producer_cfg = self.config.get('producer', {}) if isinstance(self.config, dict) else {}
                bootstrap = _extract_bootstrap_servers(self.config, 'producer')
                if not bootstrap:
                    raise KeyError("bootstrap_servers 未配置：请在顶层或producer子配置中提供")

                # Allowed keys for kafka-python KafkaProducer
                allowed = {
                    'acks', 'retries', 'compression_type', 'linger_ms', 'batch_size',
                    'max_in_flight_requests_per_connection', 'buffer_memory',
                    # security
                    'security_protocol', 'sasl_mechanism', 'sasl_plain_username', 'sasl_plain_password',
                    'ssl_cafile', 'ssl_certfile', 'ssl_keyfile',
                }
                base_kwargs = _allowed_subset(producer_cfg, allowed)
                for k in list(allowed):
                    if k not in base_kwargs and k in self.config:
                        base_kwargs[k] = self.config[k]

                self.producer = KafkaProducer(
                    bootstrap_servers=bootstrap,
                    value_serializer=lambda m: json.dumps(m, ensure_ascii=False).encode('utf-8'),
                    **base_kwargs,
                )
                return
            except Exception as e:
                if i == self.max_retries - 1:
                    handle_error(KafkaConnectionError(e), context="KafkaProducer连接最终失败")
                    raise
                handle_error(KafkaConnectionError(e), context=f"KafkaProducer连接失败，重试{i+1}/{self.max_retries}")
                time.sleep(self.retry_interval)

    def send(self, topic, value):
        for i in range(self.max_retries):
            try:
                if self.producer is None:
                    self._connect()
                if self.producer is not None:
                    self.producer.send(topic, value=value)
                    self.producer.flush()
                    return
                else:
                    handle_error(KafkaConnectionError("KafkaProducer为None，无法send"), context="send失败")
            except Exception as e:
                handle_error(e, context=f"KafkaProducer发送异常，重试{i+1}/{self.max_retries}")
                self._connect()
        handle_error(KafkaConnectionError("KafkaProducer发送失败，重试多次后仍失败"), context="send失败")
        raise Exception("KafkaProducer发送失败，重试多次后仍失败")

    def close(self):
        if self.producer:
            try:
                self.producer.close()
            except Exception as e:
                handle_error(e, context="KafkaProducer关闭异常")


class AsyncKafkaConsumerClient:
    def __init__(self, topics, config, loop=None):
        self.topics = topics
        self.config = config
        self.loop = loop or asyncio.get_event_loop()
        self.consumer = None
        self._pending_messages = deque()
        self._multi_consumer_mode = False
        self._topic_consumers = {}  # {topic: consumer} 多消费者模式

    async def start(self):
        consumer_cfg = self.config.get('consumer', {}) if isinstance(self.config, dict) else {}
        bootstrap = _extract_bootstrap_servers(self.config, 'consumer')
        if not bootstrap:
            raise KeyError("bootstrap_servers 未配置：请在顶层或consumer子配置中提供")

        # Allowed keys for aiokafka AIOKafkaConsumer
        allowed = {
            'group_id', 'auto_offset_reset', 'enable_auto_commit',
            'session_timeout_ms', 'request_timeout_ms', 'heartbeat_interval_ms',
            'max_poll_records', 'max_poll_interval_ms',
            # security (AIOKafka常用)
            'security_protocol', 'sasl_mechanism', 'sasl_plain_username', 'sasl_plain_password',
        }
        base_kwargs = _allowed_subset(consumer_cfg, allowed)
        for k in list(allowed):
            if k not in base_kwargs and k in self.config:
                base_kwargs[k] = self.config[k]

        # 检查是否启用多消费者模式
        self._multi_consumer_mode = consumer_cfg.get('multi_consumer_mode', False)
        
        if self._multi_consumer_mode and len(self.topics) > 1:
            # 多消费者模式：为每个topic创建独立的消费者
            logging.info(f"启用多消费者模式，为 {len(self.topics)} 个topic创建独立消费者")
            
            # 为每个topic单独限制max_poll_records
            per_topic_max_records = base_kwargs.get('max_poll_records', 500)
            
            for topic in self.topics:
                topic_kwargs = base_kwargs.copy()
                # 为每个topic的消费者设置合理的拉取上限
                topic_kwargs['max_poll_records'] = per_topic_max_records
                
                consumer = AIOKafkaConsumer(
                    topic,
                    loop=self.loop,
                    bootstrap_servers=bootstrap,
                    **topic_kwargs,
                )
                await consumer.start()
                self._topic_consumers[topic] = consumer
                logging.info(f"Topic {topic} 消费者已启动，max_poll_records={per_topic_max_records}")
        else:
            # 单消费者模式（原有逻辑）
            if self._multi_consumer_mode:
                logging.warning("多消费者模式需要订阅多个topic，当前只有1个topic，使用单消费者模式")
            
            self.consumer = AIOKafkaConsumer(
                *self.topics,
                loop=self.loop,
                bootstrap_servers=bootstrap,
                **base_kwargs,
            )
            await self.consumer.start()

    async def getmany(self, timeout_ms=1000, max_records=None):
        """
        拉取多条消息
        
        Args:
            timeout_ms: 超时时间（毫秒）
            max_records: 最大记录数（仅在单消费者模式下生效）
            
        Returns:
            list: 消息列表
        """
        if self._multi_consumer_mode:
            # 多消费者模式：并发从所有topic消费者拉取消息
            tasks = []
            for topic, consumer in self._topic_consumers.items():
                tasks.append(self._fetch_from_consumer(consumer, timeout_ms, max_records))
            
            # 并发拉取所有topic的消息
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 合并所有消息
            batch = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    topic = list(self._topic_consumers.keys())[i]
                    logging.error(f"从topic {topic} 拉取消息失败: {result}")
                elif isinstance(result, list):
                    batch.extend(result)
            
            return batch
        else:
            # 单消费者模式（原有逻辑）
            if self.consumer is None:
                raise RuntimeError("AsyncKafkaConsumer 未启动，consumer为None")
            records = await self.consumer.getmany(
                timeout_ms=timeout_ms, max_records=max_records
            )
            batch = []
            for msgs in records.values():
                batch.extend(msgs)
            return batch
    
    async def _fetch_from_consumer(self, consumer, timeout_ms, max_records):
        """从单个消费者拉取消息"""
        try:
            records = await consumer.getmany(
                timeout_ms=timeout_ms, max_records=max_records
            )
            batch = []
            for msgs in records.values():
                batch.extend(msgs)
            return batch
        except Exception as e:
            logging.error(f"消费者拉取消息异常: {e}")
            return []

    async def getone(self, timeout_ms=1000):
        try:
            if self._pending_messages:
                return self._pending_messages.popleft()
            batch = await self.getmany(timeout_ms=timeout_ms)
            for msg in batch:
                self._pending_messages.append(msg)
            if self._pending_messages:
                return self._pending_messages.popleft()
            return None
        except Exception as e:
            logging.error(f"AsyncKafkaConsumer getone异常: {e}")
            raise
    
    async def commit_offsets(self, offsets=None):
        """
        手动提交offset
        
        Args:
            offsets: 可选的TopicPartition到OffsetAndMetadata的字典
                    如果为None,则提交当前消费位置
        
        Returns:
            bool: 提交是否成功
        """
        if self._multi_consumer_mode:
            # 多消费者模式：提交所有消费者的offset
            success = True
            for topic, consumer in self._topic_consumers.items():
                try:
                    if offsets:
                        # 过滤出该topic的offsets
                        topic_offsets = {
                            tp: om for tp, om in offsets.items() 
                            if tp.topic == topic
                        }
                        if topic_offsets:
                            await consumer.commit(topic_offsets)
                    else:
                        await consumer.commit()
                except Exception as e:
                    logging.error(f"提交topic {topic} 的offset失败: {e}")
                    success = False
            return success
        else:
            # 单消费者模式（原有逻辑）
            if self.consumer is None:
                raise RuntimeError("AsyncKafkaConsumer 未启动，consumer为None")
            
            try:
                if offsets:
                    # 提交指定的offsets
                    await self.consumer.commit(offsets)
                else:
                    # 提交当前消费位置
                    await self.consumer.commit()
                return True
            except Exception as e:
                logging.error(f"提交offset失败: {e}")
                return False
    
    def get_consumer(self):
        """
        获取底层的AIOKafkaConsumer实例（用于访问TopicPartition等）
        多消费者模式下返回消费者字典
        """
        if self._multi_consumer_mode:
            return self._topic_consumers
        return self.consumer
            
    async def stop(self):
        """停止所有消费者"""
        if self._multi_consumer_mode:
            # 停止所有topic的消费者
            for topic, consumer in self._topic_consumers.items():
                try:
                    await consumer.stop()
                    logging.info(f"Topic {topic} 消费者已停止")
                except Exception as e:
                    logging.error(f"停止topic {topic} 消费者失败: {e}")
        elif self.consumer:
            await self.consumer.stop()
    
    def get_lag_info(self):
        """
        获取消息积压信息（lag）
        
        Returns:
            dict: {topic: {partition: lag}}
        """
        lag_info = {}
        
        if self._multi_consumer_mode:
            for topic, consumer in self._topic_consumers.items():
                try:
                    lag_info[topic] = self._calculate_consumer_lag(consumer)
                except Exception as e:
                    logging.error(f"获取topic {topic} lag信息失败: {e}")
                    lag_info[topic] = {}
        elif self.consumer:
            try:
                # 单消费者模式：按topic分组
                all_partitions = self.consumer.assignment()
                for tp in all_partitions:
                    topic = tp.topic
                    if topic not in lag_info:
                        lag_info[topic] = {}
                    
                    position = self.consumer.position(tp)
                    # 注意：这需要同步调用，在异步环境中可能有问题
                    # 生产环境建议使用专门的监控工具
                    lag_info[topic][tp.partition] = {
                        'current_offset': position,
                        'lag': 'N/A'  # aiokafka 不直接支持获取end offset
                    }
            except Exception as e:
                logging.error(f"获取lag信息失败: {e}")
        
        return lag_info
    
    def _calculate_consumer_lag(self, consumer):
        """计算单个消费者的lag"""
        lag_by_partition = {}
        try:
            partitions = consumer.assignment()
            for tp in partitions:
                position = consumer.position(tp)
                lag_by_partition[tp.partition] = {
                    'current_offset': position,
                    'lag': 'N/A'
                }
        except Exception as e:
            logging.error(f"计算lag失败: {e}")
        return lag_by_partition


class AsyncKafkaProducerClient:
    def __init__(self, config, loop=None):
        self.config = config
        self.loop = loop or asyncio.get_event_loop()
        self.producer = None

    async def start(self):
        producer_cfg = self.config.get('producer', {}) if isinstance(self.config, dict) else {}
        bootstrap = _extract_bootstrap_servers(self.config, 'producer')
        if not bootstrap:
            raise KeyError("bootstrap_servers 未配置：请在顶层或producer子配置中提供")

        # Allowed keys for aiokafka AIOKafkaProducer
        allowed = {
            'acks', 'compression_type', 'linger_ms',
            # security (AIOKafka常用)
            'security_protocol', 'sasl_mechanism', 'sasl_plain_username', 'sasl_plain_password',
        }
        base_kwargs = _allowed_subset(producer_cfg, allowed)
        for k in list(allowed):
            if k not in base_kwargs and k in self.config:
                base_kwargs[k] = self.config[k]

        self.producer = AIOKafkaProducer(
            loop=self.loop,
            bootstrap_servers=bootstrap,
            value_serializer=lambda m: json.dumps(m, ensure_ascii=False).encode('utf-8'),
            **base_kwargs,
        )
        await self.producer.start()

    async def send(self, topic, value):
        try:
            if self.producer is None:
                raise RuntimeError("AsyncKafkaProducer 未启动，producer为None")
            await self.producer.send_and_wait(topic, value)
        except Exception as e:
            logging.error(f"AsyncKafkaProducer send异常: {e}")
            raise

    async def stop(self):
        if self.producer:
            await self.producer.stop()
