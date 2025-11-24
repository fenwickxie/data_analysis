#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Service层统一测试模块
整合了异步服务、同步服务、Kafka上传、健康监控、配置重载等所有Service功能测试

合并自:
- test_async_service.py
- test_kafka_upload.py
- test_integration_extra.py
"""

import pytest
import asyncio
import threading
import time
import logging
from d_a.analysis_service import DataAnalysisService, AsyncDataAnalysisService


# ========== 异步服务测试 ==========

@pytest.mark.asyncio
async def test_async_service_basic(monkeypatch):
    """测试异步服务基础功能"""
    
    # 模拟Kafka客户端
    class DummyConsumer:
        def __init__(self):
            self._started = False
            self._stopped = False
            self._msgs = []
        
        async def start(self): 
            self._started = True
        
        async def getone(self):
            if self._msgs:
                return self._msgs.pop(0)
            await asyncio.sleep(0.01)
            return None
        
        async def stop(self): 
            self._stopped = True
    
    class DummyProducer:
        def __init__(self): 
            self._started = False
            self._stopped = False
            self.sent = []
        
        async def start(self): 
            self._started = True
        
        async def send(self, topic, value): 
            self.sent.append((topic, value))
        
        async def stop(self): 
            self._stopped = True
    
    monkeypatch.setattr(
        'd_a.analysis_service.AsyncKafkaConsumerClient', 
        lambda *a, **k: DummyConsumer()
    )
    monkeypatch.setattr(
        'd_a.analysis_service.AsyncKafkaProducerClient', 
        lambda *a, **k: DummyProducer()
    )
    
    service = AsyncDataAnalysisService(module_name="load_prediction")
    
    # 注入模拟消息
    service.consumer._msgs.append(
        type('msg', (), {
            'topic': 'TEST_TOPIC', 
            'value': {'station_id': 'S001', 'x': 1}
        })()
    )
    
    async def callback(station_id, module_input):
        return {"result": 42}
    
    await service.start(callback=callback)
    await asyncio.sleep(0.05)
    await service.stop()
    
    assert service.producer.sent
    assert service.get_station_status()['S001']['running'] is False


@pytest.mark.asyncio
async def test_async_add_remove_station():
    """测试异步添加/移除场站"""
    service = AsyncDataAnalysisService(module_name='load_prediction')
    
    # Mock dispatcher
    service.dispatcher.get_all_inputs = lambda sid: {
        'load_prediction': {'timestamp': 1}
    }
    
    async def callback(station_id, module_input):
        return {'result': 2}
    
    await service.add_station('sid_async', callback)
    await asyncio.sleep(0.05)
    
    status = service.get_station_status()
    assert 'sid_async' in status
    
    await service.remove_station('sid_async')
    await asyncio.sleep(0.01)
    
    status2 = service.get_station_status()
    assert not status2['sid_async']['running']


# ========== 同步服务测试 ==========

def test_kafka_upload(monkeypatch):
    """测试Kafka消息上传"""
    
    # 模拟KafkaProducerClient.send
    sent = {}
    
    def fake_send(self, topic, value):
        sent['topic'] = topic
        sent['value'] = value
    
    monkeypatch.setattr(
        'd_a.kafka_client.KafkaProducerClient.send', 
        fake_send
    )
    
    service = DataAnalysisService(module_name='load_prediction')
    
    # 模拟dispatcher输出
    service.dispatcher.get_all_inputs = lambda sid: {
        'load_prediction': {'timestamp': 123}
    }
    
    def dummy_callback(station_id, module_input):
        return {'result': 42}
    
    # 启动一个场站线程
    sid = 'station1'
    service._station_stop_events[sid] = threading.Event()
    t = threading.Thread(
        target=service._station_worker, 
        args=(sid, dummy_callback), 
        daemon=True
    )
    t.start()
    time.sleep(0.1)
    service._station_stop_events[sid].set()
    t.join()
    
    assert sent['topic'].startswith('MODULE-OUTPUT-LOAD_PREDICTION')
    assert sent['value']['station_id'] == sid
    assert sent['value']['output']['result'] == 42


def test_health_monitoring():
    """测试健康监控功能"""
    service = DataAnalysisService(module_name='load_prediction')
    
    # 模拟dispatcher输出
    service.dispatcher.get_all_inputs = lambda sid: {
        'load_prediction': {'timestamp': 123}
    }
    
    sid = 'station_health'
    service._station_stop_events[sid] = threading.Event()
    
    t = threading.Thread(
        target=service._station_worker, 
        args=(sid, lambda s, m: {'result': 1}, service._station_stop_events[sid]), 
        daemon=True
    )
    t.start()
    time.sleep(0.05)
    
    status = service.get_station_status()
    assert sid in status
    assert 'running' in status[sid]
    
    service._station_stop_events[sid].set()
    t.join()
    
    assert status[sid]['running'] is False


# ========== 配置管理测试 ==========

def test_reload_config(monkeypatch):
    """测试配置重载"""
    service = DataAnalysisService(module_name='load_prediction')
    
    called = {}
    
    def fake_reload(cfg):
        called['ok'] = True
    
    service.dispatcher.reload_config = fake_reload
    
    monkeypatch.setattr(
        'd_a.config.KAFKA_CONFIG', 
        {'bootstrap_servers': 'localhost:9092', 'group_id': 'g'}
    )
    monkeypatch.setattr(
        'd_a.config.TOPIC_TO_MODULES', 
        {'T': ['load_prediction']}
    )
    
    service.reload_config()
    assert called.get('ok')


# ========== 日志测试 ==========

def test_log_output(tmp_path, caplog):
    """测试日志输出"""
    log_file = tmp_path / 'test.log'
    handler = logging.FileHandler(log_file, encoding='utf-8')
    logger = logging.getLogger()
    logger.addHandler(handler)
    
    logger.error('test error log')
    handler.flush()
    
    with open(log_file, encoding='utf-8') as f:
        content = f.read()
    
    assert 'test error log' in content
    logger.removeHandler(handler)


# ========== 异常处理测试 ==========

@pytest.mark.asyncio
async def test_async_service_callback_exception():
    """测试异步服务回调异常处理"""
    service = AsyncDataAnalysisService(module_name='load_prediction')
    
    # Mock dispatcher
    service.dispatcher.get_all_inputs = lambda sid: {
        'load_prediction': {'timestamp': 1}
    }
    
    async def bad_callback(station_id, module_input):
        raise ValueError("test async callback error")
    
    sid = 'sid_async_err'
    
    # 启动场站处理
    task = asyncio.create_task(service.add_station(sid, bad_callback))
    await asyncio.sleep(0.05)
    
    # 验证错误被捕获，服务仍在运行
    status = service.get_station_status()
    assert sid in status
    
    await service.remove_station(sid)


def test_sync_service_callback_exception():
    """测试同步服务回调异常处理"""
    service = DataAnalysisService(module_name='load_prediction')
    
    orig_get_all_outputs = service.dispatcher.get_all_inputs
    
    def fake_get_all_outputs(station_id):
        return {'load_prediction': {'timestamp': 1}}
    
    service.dispatcher.get_all_inputs = fake_get_all_outputs
    
    def bad_callback(station_id, module_input):
        raise ValueError("test sync callback error")
    
    sid = 'sid_sync_err'
    service._station_stop_events[sid] = threading.Event()
    
    t = threading.Thread(
        target=service._station_worker, 
        args=(sid, bad_callback, service._station_stop_events[sid]), 
        daemon=True
    )
    t.start()
    time.sleep(0.05)
    service._station_stop_events[sid].set()
    t.join()
    
    status = service.get_station_status()
    assert sid in status
    assert status[sid]['running'] is False
    
    service.dispatcher.get_all_inputs = orig_get_all_outputs


def test_callback_exception_logging(caplog):
    """测试回调异常日志记录"""
    
    def bad_callback(station_id, module_input):
        raise ValueError("故意异常")
    
    service = DataAnalysisService(module_name="load_prediction")
    
    orig_get_all_outputs = service.dispatcher.get_all_inputs
    
    def fake_get_all_outputs(station_id):
        return {'load_prediction': {'timestamp': 123}}
    
    service.dispatcher.get_all_inputs = fake_get_all_outputs
    
    sid = 'station_cb'
    service._station_stop_events[sid] = threading.Event()
    
    t = threading.Thread(
        target=service._station_worker, 
        args=(sid, bad_callback, service._station_stop_events[sid]), 
        daemon=True
    )
    t.start()
    time.sleep(0.05)
    service._station_stop_events[sid].set()
    t.join()
    
    assert any("回调处理" in r or "error" in r.lower() for r in caplog.text)
    
    service.dispatcher.get_all_inputs = orig_get_all_outputs


# ========== 多线程安全测试 ==========

def test_concurrent_station_processing():
    """测试多场站并发处理"""
    service = DataAnalysisService(module_name='load_prediction')
    
    service.dispatcher.get_all_inputs = lambda sid: {
        'load_prediction': {'timestamp': 123}
    }
    
    def callback(station_id, module_input):
        time.sleep(0.01)
        return {'result': station_id}
    
    # 启动多个场站
    station_ids = [f'station_{i}' for i in range(5)]
    threads = []
    
    for sid in station_ids:
        service._station_stop_events[sid] = threading.Event()
        t = threading.Thread(
            target=service._station_worker,
            args=(sid, callback, service._station_stop_events[sid]),
            daemon=True
        )
        t.start()
        threads.append(t)
    
    time.sleep(0.1)
    
    # 停止所有场站
    for sid in station_ids:
        service._station_stop_events[sid].set()
    
    for t in threads:
        t.join(timeout=1.0)
    
    # 验证所有场站都已处理
    status = service.get_station_status()
    for sid in station_ids:
        assert sid in status


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
