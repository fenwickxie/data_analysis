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

from .__init__ import handle_error, KafkaConnectionError

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
                consumer_config = self.config.get('consumer', {})
                self.consumer = KafkaConsumer(
                    *self.topics,
                    bootstrap_servers=self.config['bootstrap_servers'],
                    group_id=consumer_config.get('group_id', self.config.get('group_id', 'default-group')),
                    auto_offset_reset=consumer_config.get('auto_offset_reset', self.config.get('auto_offset_reset', 'latest')),
                    enable_auto_commit=consumer_config.get('enable_auto_commit', self.config.get('enable_auto_commit', True)),
                    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
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
                self.producer = KafkaProducer(
                    bootstrap_servers=self.config['bootstrap_servers'],
                    value_serializer=lambda m: json.dumps(m, ensure_ascii=False).encode('utf-8')
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

    async def start(self):
        consumer_config = self.config.get('consumer', {})
        self.consumer = AIOKafkaConsumer(
            *self.topics,
            loop=self.loop,
            bootstrap_servers=self.config['bootstrap_servers'],
            group_id=consumer_config.get('group_id', self.config.get('group_id', 'default-group')),
            auto_offset_reset=consumer_config.get('auto_offset_reset', self.config.get('auto_offset_reset', 'latest')),
            enable_auto_commit=consumer_config.get('enable_auto_commit', self.config.get('enable_auto_commit', True)),
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        await self.consumer.start()

    async def getone(self):
        try:
            msg = await self.consumer.getone()
            return msg
        except Exception as e:
            logging.error(f"AsyncKafkaConsumer getone异常: {e}")
            raise

    async def stop(self):
        if self.consumer:
            await self.consumer.stop()


class AsyncKafkaProducerClient:
    def __init__(self, config, loop=None):
        self.config = config
        self.loop = loop or asyncio.get_event_loop()
        self.producer = None

    async def start(self):
        self.producer = AIOKafkaProducer(
            loop=self.loop,
            bootstrap_servers=self.config['bootstrap_servers'],
            value_serializer=lambda m: json.dumps(m, ensure_ascii=False).encode('utf-8')
        )
        await self.producer.start()

    async def send(self, topic, value):
        try:
            await self.producer.send_and_wait(topic, value)
        except Exception as e:
            logging.error(f"AsyncKafkaProducer send异常: {e}")
            raise

    async def stop(self):
        if self.producer:
            await self.producer.stop()
