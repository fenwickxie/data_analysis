#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Kafka client wrappers built on top of confluent-kafka."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from confluent_kafka import Consumer, KafkaException, Producer

from .errors import KafkaConnectionError, handle_error


def _extract_bootstrap_servers(config: Dict[str, Any], section: str) -> Optional[Any]:
    """Return bootstrap servers, checking nested section first then top-level."""
    if not isinstance(config, dict):
        return None
    nested = config.get(section, {})
    if isinstance(nested, dict) and nested.get("bootstrap_servers"):
        return nested.get("bootstrap_servers")
    return config.get("bootstrap_servers")


def _normalize_bootstrap(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, Iterable):
        return ",".join(str(v) for v in value)
    return str(value)


def _map_keys(source: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
    return {target: source[key] for key, target in mapping.items() if key in source}


CONSUMER_KEY_MAP = {
    "group_id": "group.id",
    "auto_offset_reset": "auto.offset.reset",
    "enable_auto_commit": "enable.auto.commit",
    "session_timeout_ms": "session.timeout.ms",
    "request_timeout_ms": "request.timeout.ms",
    "heartbeat_interval_ms": "heartbeat.interval.ms",
    "max_poll_interval_ms": "max.poll.interval.ms",
    "max_poll_records": "max.poll.records",
}


PRODUCER_KEY_MAP = {
    "acks": "acks",
    "compression_type": "compression.type",
    "linger_ms": "linger.ms",
    "max_in_flight_requests_per_connection": "max.in.flight.requests.per.connection",
}


SECURITY_KEY_MAP = {
    "security_protocol": "security.protocol",
    "sasl_mechanism": "sasl.mechanism",
    "sasl_plain_username": "sasl.username",
    "sasl_plain_password": "sasl.password",
    "ssl_cafile": "ssl.ca.location",
    "ssl_certfile": "ssl.certificate.location",
    "ssl_keyfile": "ssl.key.location",
}


def _build_consumer_conf(config: Dict[str, Any]) -> Dict[str, Any]:
    consumer_cfg = config.get("consumer", {}) if isinstance(config, dict) else {}
    conf: Dict[str, Any] = {}

    bootstrap = _extract_bootstrap_servers(config, "consumer")
    if not bootstrap:
        raise KeyError("bootstrap_servers 未配置：请在顶层或consumer子配置中提供")
    conf["bootstrap.servers"] = _normalize_bootstrap(bootstrap)

    conf.update(_map_keys(config, CONSUMER_KEY_MAP))
    conf.update(_map_keys(consumer_cfg, CONSUMER_KEY_MAP))
    conf.update(_map_keys(config, SECURITY_KEY_MAP))
    conf.update(_map_keys(consumer_cfg, SECURITY_KEY_MAP))

    if "group.id" not in conf:
        conf["group.id"] = consumer_cfg.get("group_id") or config.get("group_id", "data-analysis-default")

    return conf


def _build_producer_conf(config: Dict[str, Any]) -> Dict[str, Any]:
    producer_cfg = config.get("producer", {}) if isinstance(config, dict) else {}
    conf: Dict[str, Any] = {}

    bootstrap = _extract_bootstrap_servers(config, "producer")
    if not bootstrap:
        raise KeyError("bootstrap_servers 未配置：请在顶层或producer子配置中提供")
    conf["bootstrap.servers"] = _normalize_bootstrap(bootstrap)

    conf.update(_map_keys(config, PRODUCER_KEY_MAP))
    conf.update(_map_keys(producer_cfg, PRODUCER_KEY_MAP))
    conf.update(_map_keys(config, SECURITY_KEY_MAP))
    conf.update(_map_keys(producer_cfg, SECURITY_KEY_MAP))

    return conf


def _decode_message(payload: Optional[bytes]) -> Any:
    if payload is None:
        return None
    if not isinstance(payload, (bytes, bytearray)):
        return payload
    try:
        return json.loads(payload.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        handle_error(exc, context="Kafka消息JSON解析失败")
        return None


@dataclass(frozen=True)
class TopicPartition:
    topic: str
    partition: int


class KafkaMessage:
    __slots__ = ("topic", "partition", "offset", "value", "timestamp")

    def __init__(self, topic: str, partition: int, offset: int, value: Any, timestamp: Optional[int]):
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.value = value
        self.timestamp = timestamp


def _wrap_message(message) -> Optional[KafkaMessage]:
    if message is None:
        return None
    if message.error():
        handle_error(KafkaConnectionError(message.error()), context="Kafka消息错误")
        return None
    value = _decode_message(message.value())
    return KafkaMessage(
        topic=message.topic(),
        partition=message.partition(),
        offset=message.offset(),
        value=value,
        timestamp=message.timestamp()[1] if message.timestamp() else None,
    )


class KafkaConsumerClient:
    def __init__(self, topics: List[str], config: Dict[str, Any], max_retries: int = 5, retry_interval: int = 5):
        self.topics = topics
        self.config = config or {}
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        self.consumer: Optional[Consumer] = None
        self._lock = threading.Lock()
        self._connect()

    def _connect(self) -> None:
        for attempt in range(self.max_retries):
            try:
                conf = _build_consumer_conf(self.config)
                consumer = Consumer(conf)
                consumer.subscribe(self.topics)
                self.consumer = consumer
                return
            except Exception as exc:  # noqa: BLE001
                if attempt == self.max_retries - 1:
                    handle_error(KafkaConnectionError(exc), context="KafkaConsumer连接最终失败")
                    raise
                handle_error(KafkaConnectionError(exc), context=f"KafkaConsumer连接失败，重试{attempt + 1}/{self.max_retries}")
                time.sleep(self.retry_interval)

    def poll(self, timeout_ms: int = 1000) -> Dict[TopicPartition, List[KafkaMessage]]:
        with self._lock:
            if self.consumer is None:
                self._connect()
            if self.consumer is None:
                handle_error(KafkaConnectionError("KafkaConsumer为None，无法poll"), context="poll失败")
                return {}
            timeout = max(timeout_ms, 0) / 1000.0
            try:
                records = self.consumer.consume(num_messages=50, timeout=timeout)
            except KafkaException as exc:
                handle_error(KafkaConnectionError(exc), context="KafkaConsumer poll异常")
                self._connect()
                return {}

        if not records:
            return {}

        buckets: Dict[TopicPartition, List[KafkaMessage]] = defaultdict(list)
        for message in records:
            wrapped = _wrap_message(message)
            if wrapped is None:
                continue
            tp = TopicPartition(wrapped.topic, wrapped.partition)
            buckets[tp].append(wrapped)
        return dict(buckets)

    def close(self) -> None:
        with self._lock:
            if self.consumer is None:
                return
            try:
                self.consumer.close()
            except Exception as exc:  # noqa: BLE001
                handle_error(exc, context="KafkaConsumer关闭异常")
            finally:
                self.consumer = None


class KafkaProducerClient:
    def __init__(self, config: Dict[str, Any], max_retries: int = 5, retry_interval: int = 5):
        self.config = config or {}
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        self.producer: Optional[Producer] = None
        self._lock = threading.Lock()
        self._connect()

    def _connect(self) -> None:
        for attempt in range(self.max_retries):
            try:
                conf = _build_producer_conf(self.config)
                self.producer = Producer(conf)
                return
            except Exception as exc:  # noqa: BLE001
                if attempt == self.max_retries - 1:
                    handle_error(KafkaConnectionError(exc), context="KafkaProducer连接最终失败")
                    raise
                handle_error(KafkaConnectionError(exc), context=f"KafkaProducer连接失败，重试{attempt + 1}/{self.max_retries}")
                time.sleep(self.retry_interval)

    def _produce(self, producer: Producer, topic: str, value: Any) -> None:
        payload = json.dumps(value, ensure_ascii=False).encode("utf-8")

        def delivery_report(err, _msg) -> None:
            if err is not None:
                handle_error(KafkaConnectionError(err), context=f"KafkaProducer发送失败 topic={topic}")

        try:
            producer.produce(topic, payload, callback=delivery_report)
            producer.poll(0)
        except BufferError as exc:
            handle_error(exc, context="KafkaProducer缓冲区已满，尝试刷新")
            producer.flush()
            producer.produce(topic, payload, callback=delivery_report)
        except Exception as exc:  # noqa: BLE001
            raise KafkaConnectionError(exc) from exc

    def send(self, topic: str, value: Any) -> None:
        for attempt in range(self.max_retries):
            try:
                with self._lock:
                    if self.producer is None:
                        self._connect()
                    producer = self.producer
                    if producer is None:
                        raise KafkaConnectionError("KafkaProducer未连接")
                    self._produce(producer, topic, value)
                    producer.flush()
                return
            except Exception as exc:  # noqa: BLE001
                handle_error(exc, context=f"KafkaProducer发送异常，重试{attempt + 1}/{self.max_retries}")
                with self._lock:
                    self._connect()
        handle_error(KafkaConnectionError("KafkaProducer发送失败，重试多次后仍失败"), context="send失败")
        raise KafkaConnectionError("KafkaProducer发送失败，重试多次后仍失败")

    def close(self) -> None:
        with self._lock:
            if self.producer is None:
                return
            try:
                self.producer.flush()
            except Exception:
                pass
            finally:
                self.producer = None


class AsyncKafkaConsumerClient:
    def __init__(self, topics: List[str], config: Dict[str, Any], loop: Optional[asyncio.AbstractEventLoop] = None):
        self.topics = topics
        self.config = config or {}
        self.loop = loop or asyncio.get_event_loop()
        self.consumer: Optional[Consumer] = None
        self._consume_lock = threading.Lock()
        self._stopped = False
        self._timeout = 1.0

    async def start(self) -> None:
        if self.consumer is not None:
            return
        self._stopped = False
        conf = _build_consumer_conf(self.config)
        consumer = Consumer(conf)
        consumer.subscribe(self.topics)
        self.consumer = consumer

    def _consume_once(self) -> Optional[KafkaMessage]:
        if self.consumer is None:
            return None
        with self._consume_lock:
            messages = self.consumer.consume(num_messages=1, timeout=self._timeout)
        if not messages:
            return None
        return _wrap_message(messages[0])

    async def getone(self) -> KafkaMessage:
        if self.consumer is None:
            raise RuntimeError("AsyncKafkaConsumer 未启动，consumer为None")
        while not self._stopped:
            message = await asyncio.to_thread(self._consume_once)
            if message is None:
                await asyncio.sleep(0)
                continue
            return message
        raise asyncio.CancelledError  # pragma: no cover

    async def stop(self) -> None:
        self._stopped = True
        if self.consumer is None:
            return
        consumer = self.consumer
        self.consumer = None
        await asyncio.to_thread(consumer.close)


class AsyncKafkaProducerClient:
    def __init__(self, config: Dict[str, Any], loop: Optional[asyncio.AbstractEventLoop] = None):
        self.config = config or {}
        self.loop = loop or asyncio.get_event_loop()
        self.producer: Optional[Producer] = None
        self._produce_lock = threading.Lock()

    async def start(self) -> None:
        if self.producer is not None:
            return
        conf = _build_producer_conf(self.config)
        self.producer = Producer(conf)

    def _send_blocking(self, topic: str, value: Any) -> None:
        if self.producer is None:
            raise RuntimeError("AsyncKafkaProducer 未启动")
        with self._produce_lock:
            payload = json.dumps(value, ensure_ascii=False).encode("utf-8")

            def delivery_report(err, _msg) -> None:
                if err is not None:
                    handle_error(KafkaConnectionError(err), context=f"AsyncKafkaProducer发送失败 topic={topic}")

            self.producer.produce(topic, payload, callback=delivery_report)
            self.producer.poll(0)
            self.producer.flush()

    async def send(self, topic: str, value: Any) -> None:
        if self.producer is None:
            raise RuntimeError("AsyncKafkaProducer 未启动，producer为None")
        await asyncio.to_thread(self._send_blocking, topic, value)

    async def stop(self) -> None:
        if self.producer is None:
            return
        producer = self.producer
        self.producer = None
        await asyncio.to_thread(producer.flush)
