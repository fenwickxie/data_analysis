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
    # 直接调用补全内部方法
    padded = dispatcher.get_module_input.__func__.__closure__[0].cell_contents['pad_or_interp'](dispatcher, seq, target)
    assert isinstance(padded, expect_type)
    assert len(padded) == target

# dispatcher异常字段
def test_dispatcher_invalid_topic():
    dispatcher = DataDispatcher()
    # 非法topic
    try:
        dispatcher.update_topic_data('sid', 'INVALID_TOPIC', {'x': 1})
    except Exception as e:
        pytest.fail(f"update_topic_data异常未被捕获: {e}")
    # get_module_input非法模块
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
    # 真实KafkaProducer/Consumer
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

# 异步服务异常回调
@pytest.mark.asyncio
async def test_async_service_callback_exception():
    service = AsyncDataAnalysisService(module_name="load_prediction")
    service.dispatcher.get_all_outputs = lambda sid: {'load_prediction': {'timestamp': 1}}
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

# 同步服务异常回调
def test_sync_service_callback_exception():
    service = DataAnalysisService(module_name='load_prediction')
    service.dispatcher.get_all_outputs = lambda sid: {'load_prediction': {'timestamp': 1}}
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
