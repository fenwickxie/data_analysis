#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:11:01
project: data_analysis
filename: test_extreme_and_integration.py
version: 1.0
"""

import pytest
import asyncio
import threading
import time
import math
from data_analysis.analysis_service import DataAnalysisService, AsyncDataAnalysisService
from data_analysis.dispatcher import DataDispatcher
from data_analysis.kafka_client import KafkaProducerClient, KafkaConsumerClient

# 异常分支测试：dispatcher极端数据
@pytest.mark.parametrize("padding, seq, target, expect_type", [
    ('zero', [], 3, list),
    ('linear', [], 2, list),
    ('forward', [], 2, list),
    ('missing', [], 2, list),
])
def test_dispatcher_extreme_padding(padding, seq, target, expect_type):
    dispatcher = DataDispatcher()
    dispatcher.set_padding_strategy(padding)
    topic = 'SCHEDULE-STATION-REALTIME-DATA'
    for v in seq:
        dispatcher.update_topic_data('station_pad', topic, {'value': v})
    result = dispatcher.get_module_input('station_pad', 'load_prediction')
    if result is not None:
        window = result.get('value_window', [])
        assert isinstance(window, expect_type)
        assert len(window) == target

# dispatcher异常字段
def test_dispatcher_invalid_topic():
    dispatcher = DataDispatcher()
    try:
        dispatcher.update_topic_data('sid', 'INVALID_TOPIC', {'x': 1})
    except Exception as e:
        pytest.fail(f"update_topic_data异常未被捕获: {e}")
    result = dispatcher.get_module_input('sid', 'INVALID_MODULE')
    assert result is None

# Kafka真集成测试（需本地Kafka服务，跳过无服务时）
def test_kafka_real_integration(monkeypatch):
    import socket
    def kafka_port_open():
        s = socket.socket()
        try:
            s.settimeout(1)
            s.connect(('localhost', 9092))
            return True
        except Exception:
            return False
        finally:
            s.close()
    if not kafka_port_open():
        pytest.skip("本地未检测到Kafka服务，跳过真集成测试")
    topic = 'test_data_analysis_integration'
    producer = KafkaProducerClient({'bootstrap_servers': 'localhost:9092', 'group_id': 'g'})
    consumer = KafkaConsumerClient([topic], {'bootstrap_servers': 'localhost:9092', 'group_id': 'g', 'auto_offset_reset': 'earliest'})
    msg = {'test': 'ok'}
    producer.send(topic, msg)
    time.sleep(1)
    found = False
    for _ in range(5):
        msg_pack = consumer.poll(timeout_ms=1000)
        for tp, msgs in msg_pack.items():
            for m in msgs:
                if m.value.get('test') == 'ok':
                    found = True
                    break
    consumer.close()
    producer.close()
    assert found

@pytest.mark.asyncio
async def test_async_service_callback_exception():
    service = AsyncDataAnalysisService(module_name="load_prediction")
    orig_get_all_outputs = service.dispatcher.get_all_outputs
    def fake_get_all_outputs(station_id):
        return {'load_prediction': {'timestamp': 1}}
    service.dispatcher.get_all_outputs = fake_get_all_outputs
    async def bad_cb(station_id, module_input):
        raise ValueError("test async callback error")
    await service.add_station('sid_async_err', bad_cb)
    await asyncio.sleep(0.05)
    status = service.get_station_status()
    assert 'sid_async_err' in status
    await service.remove_station('sid_async_err')
    await asyncio.sleep(0.01)
    status2 = service.get_station_status()
    assert not status2['sid_async_err']['running']
    service.dispatcher.get_all_outputs = orig_get_all_outputs

def test_sync_service_callback_exception():
    service = DataAnalysisService(module_name='load_prediction')
    orig_get_all_outputs = service.dispatcher.get_all_outputs
    def fake_get_all_outputs(station_id):
        return {'load_prediction': {'timestamp': 1}}
    service.dispatcher.get_all_outputs = fake_get_all_outputs
    def bad_cb(station_id, module_input):
        raise ValueError("test sync callback error")
    sid = 'sid_sync_err'
    service._station_stop_events[sid] = threading.Event()
    t = threading.Thread(target=service._station_worker, args=(sid, bad_cb, service._station_stop_events[sid]), daemon=True)
    t.start()
    time.sleep(0.05)
    service._station_stop_events[sid].set()
    t.join()
    status = service.get_station_status()
    assert sid in status
    assert status[sid]['running'] is False
    service.dispatcher.get_all_outputs = orig_get_all_outputs

def test_dispatcher_empty_topic():
    dispatcher = DataDispatcher()
    dispatcher.update_topic_data('station1', 'UNKNOWN_TOPIC', {})
    assert dispatcher.get_all_outputs('station1') == {}

def test_dispatcher_extreme_window():
    dispatcher = DataDispatcher()
    topic = 'SCHEDULE-STATION-REALTIME-DATA'
    dispatcher.data_cache['station1'] = {topic: []}
    result = dispatcher.get_module_input('station1', 'load_prediction')
    assert isinstance(result, dict)

def test_callback_exception_logging(caplog):
    def bad_callback(station_id, module_input):
        raise ValueError("故意异常")
    service = DataAnalysisService(module_name="load_prediction")
    orig_get_all_outputs = service.dispatcher.get_all_outputs
    def fake_get_all_outputs(station_id):
        return {'load_prediction': {'timestamp': 123}}
    service.dispatcher.get_all_outputs = fake_get_all_outputs
    sid = 'station_cb'
    service._station_stop_events[sid] = threading.Event()
    t = threading.Thread(target=service._station_worker, args=(sid, bad_callback, service._station_stop_events[sid]), daemon=True)
    t.start()
    time.sleep(0.05)
    service._station_stop_events[sid].set()
    t.join()
    assert any("回调处理" in r for r in caplog.text)
    service.dispatcher.get_all_outputs = orig_get_all_outputs

def test_window_all_missing():
    dispatcher = DataDispatcher()
    topic = 'SCHEDULE-STATION-REALTIME-DATA'
    for _ in range(5):
        dispatcher.update_topic_data('station1', topic, {})
    result = dispatcher.get_module_input('station1', 'load_prediction')
    assert isinstance(result, dict)

def test_window_extreme_values():
    dispatcher = DataDispatcher()
    topic = 'SCHEDULE-STATION-REALTIME-DATA'
    for v in [1e10, -1e10, None, float('nan'), float('inf')]:
        dispatcher.update_topic_data('station1', topic, {'value': v})
    result = dispatcher.get_module_input('station1', 'load_prediction')
    assert isinstance(result, dict)
