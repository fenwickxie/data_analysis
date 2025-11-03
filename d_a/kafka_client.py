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
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
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

        self.consumer = AIOKafkaConsumer(
            *self.topics,
            loop=self.loop,
            bootstrap_servers=bootstrap,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            **base_kwargs,
        )
        await self.consumer.start()

    async def getmany(self, timeout_ms=1000, max_records=None):
        if self.consumer is None:
            raise RuntimeError("AsyncKafkaConsumer 未启动，consumer为None")
        records = await self.consumer.getmany(
            timeout_ms=timeout_ms, max_records=max_records
        )
        batch = []
        for msgs in records.values():
            batch.extend(msgs)
        return batch

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
            
    async def stop(self):
        if self.consumer:
            await self.consumer.stop()


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
