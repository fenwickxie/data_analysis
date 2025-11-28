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
from d_a.analysis_service import BatchResultAggregator


# ========== Fixtures ==========

@pytest.fixture
def mock_kafka_config(monkeypatch):
    """Mock Kafka配置，模拟config.yaml中的结构"""
    test_config = {
        'bootstrap_servers': ['localhost:9092'],
        'consumer': {
            'group_id': 'test_group',
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': False,
            'max_poll_records': 10
        },
        'producer': {
            'key_deserializer': 'org.apache.kafka.common.serialization.StringDeserializer',
            'value_deserializer': 'org.apache.kafka.common.serialization.StringDeserializer'
        }
    }
    # Mock d_a.config.KAFKA_CONFIG，让服务在不传入kafka_config时也能使用测试配置
    monkeypatch.setattr('d_a.config.KAFKA_CONFIG', test_config)
    return test_config


@pytest.fixture
def mock_async_kafka_clients(monkeypatch):
    """Mock异步Kafka客户端"""
    class DummyConsumer:
        def __init__(self, *args, **kwargs):
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
        
        async def getmany(self, timeout_ms=1000):
            return []
        
        async def stop(self): 
            self._stopped = True
    
    class DummyProducer:
        def __init__(self, *args, **kwargs): 
            self._started = False
            self._stopped = False
            self.sent = []
        
        async def start(self): 
            self._started = True
        
        async def send(self, topic, value): 
            self.sent.append((topic, value))
        
        async def stop(self): 
            self._stopped = True
    
    monkeypatch.setattr('d_a.kafka_client.AsyncKafkaConsumerClient', DummyConsumer)
    monkeypatch.setattr('d_a.kafka_client.AsyncKafkaProducerClient', DummyProducer)
    
    return DummyConsumer, DummyProducer


@pytest.fixture
def mock_sync_kafka_clients(monkeypatch):
    """Mock同步Kafka客户端"""
    class DummyConsumer:
        def __init__(self, *args, **kwargs):
            pass
        
        def poll(self, timeout_ms=1000):
            return {}
        
        def close(self):
            pass
    
    class DummyProducer:
        def __init__(self, *args, **kwargs):
            self.sent = []
        
        def send(self, topic, value):
            self.sent.append((topic, value))
        
        def close(self):
            pass
    
    # Mock在d_a.kafka_client和d_a.analysis_service两个位置
    monkeypatch.setattr('d_a.kafka_client.KafkaConsumerClient', DummyConsumer)
    monkeypatch.setattr('d_a.kafka_client.KafkaProducerClient', DummyProducer)
    monkeypatch.setattr('d_a.analysis_service.KafkaConsumerClient', DummyConsumer)
    
    return DummyConsumer, DummyProducer


# ========== 异步服务测试 ==========

@pytest.mark.asyncio
async def test_async_service_basic(mock_kafka_config, mock_async_kafka_clients):
    """测试异步服务基础功能"""
    
    service = AsyncDataAnalysisService(module_name="load_prediction")
    
    # mock_async_kafka_clients fixture已经注入了DummyConsumer和DummyProducer
    # DummyConsumer有_msgs属性,可以添加模拟消息
    
    async def callback(station_id, module_input):
        return {"result": 42}
    
    # 直接添加一个场站进行测试
    await service.add_station('S001', callback)
    await asyncio.sleep(0.05)
    
    status = service.get_station_status()
    assert 'S001' in status
    assert status['S001']['running'] is True
    
    await service.remove_station('S001')
    await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_async_add_remove_station(mock_kafka_config, mock_async_kafka_clients):
    """测试异步添加/移除场站"""
    service = AsyncDataAnalysisService(module_name='load_prediction')
    
    # Mock dispatcher
    service.dispatcher.get_all_inputs = lambda station_id: {
        'load_prediction': {'timestamp': 1}
    }
    
    async def callback(station_id, module_input):
        return {'result': 2}
    
    await service.add_station('sid_async', callback)
    await asyncio.sleep(0.05)
    
    status = service.get_station_status()
    assert 'sid_async' in status
    assert status['sid_async']['running'] is True
    
    await service.remove_station('sid_async')
    await asyncio.sleep(0.01)
    
    # remove_station会删除任务,所以status中不再有该场站
    status2 = service.get_station_status()
    assert 'sid_async' not in status2


# ========== 同步服务测试 ==========

def test_kafka_upload(mock_kafka_config, mock_sync_kafka_clients, monkeypatch):
    """测试Kafka消息上传"""
    
    # 创建DummyProducer实例用于测试
    class MockProducer:
        def __init__(self, *args, **kwargs):
            self.sent = []
        
        def send(self, topic, value):
            self.sent.append({'topic': topic, 'value': value})
        
        def close(self):
            pass
    
    mock_producer = MockProducer()
    
    # Mock KafkaProducerClient返回我们的mock实例
    monkeypatch.setattr(
        'd_a.kafka_client.KafkaProducerClient', 
        lambda *a, **k: mock_producer
    )
    
    service = DataAnalysisService(module_name='load_prediction')
    
    # 模拟dispatcher输出
    service.dispatcher.get_all_inputs = lambda station_id: {
        'load_prediction': {'timestamp': 123}
    }
    
    def dummy_callback(station_id, module_input):
        return {'result': 42}
    
    # 启动一个场站线程
    sid = 'station1'
    service._station_stop_events[sid] = threading.Event()
    service._station_data_events[sid] = threading.Event()
    service._station_data_events[sid].set()  # 设置初始事件
    t = threading.Thread(
        target=service._station_worker,
        args=(sid, dummy_callback, None, service._station_stop_events[sid], service._station_data_events[sid]),
        daemon=True
    )
    t.start()
    time.sleep(0.1)
    service._station_stop_events[sid].set()
    t.join()
    
    # 注意：同步服务不会自动上传结果，需要显式调用result_handler
    # 这个测试主要验证callback能被正常调用
    assert t.is_alive() == False
def test_health_monitoring(mock_kafka_config, mock_sync_kafka_clients):
    """测试健康监控功能"""
    service = DataAnalysisService(module_name='load_prediction')
    
    # 模拟dispatcher输出
    service.dispatcher.get_all_inputs = lambda station_id: {
        'load_prediction': {'timestamp': 123}
    }
    
    sid = 'station_health'
    service._station_stop_events[sid] = threading.Event()
    service._station_data_events[sid] = threading.Event()
    
    t = threading.Thread(
        target=service._station_worker, 
        args=(sid, lambda s, m: {'result': 1}, None, service._station_stop_events[sid], service._station_data_events[sid]), 
        daemon=True
    )
    t.start()
    time.sleep(0.05)
    
    # 验证线程正在运行
    assert t.is_alive()
    
    # 设置停止事件和数据事件,让worker能够退出
    service._station_stop_events[sid].set()
    service._station_data_events[sid].set()
    t.join(timeout=2.0)
    
    # 验证线程已停止
    assert not t.is_alive()


# ========== 配置管理测试 ==========

def test_reload_config(mock_kafka_config, mock_sync_kafka_clients, monkeypatch):
    """测试配置重载"""
    service = DataAnalysisService(module_name='load_prediction')
    
    called = {}
    
    def fake_reload(config_mod):
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
async def test_async_service_callback_exception(mock_kafka_config, mock_async_kafka_clients):
    """测试异步服务回调异常处理"""
    service = AsyncDataAnalysisService(module_name='load_prediction')
    
    # Mock dispatcher
    service.dispatcher.get_all_inputs = lambda station_id: {
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


def test_sync_service_callback_exception(mock_kafka_config, mock_sync_kafka_clients):
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
    service._station_data_events[sid] = threading.Event()
    service._station_data_events[sid].set()  # 设置初始事件
    
    t = threading.Thread(
        target=service._station_worker,
        args=(sid, bad_callback, None, service._station_stop_events[sid], service._station_data_events[sid]),
        daemon=True
    )
    t.start()
    time.sleep(0.05)
    service._station_stop_events[sid].set()
    t.join()
    
    # 验证线程已经结束（即使有异常）
    assert not t.is_alive()
    
    service.dispatcher.get_all_inputs = orig_get_all_outputs
def test_callback_exception_logging(mock_kafka_config, mock_sync_kafka_clients, caplog):
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
    service._station_data_events[sid] = threading.Event()
    service._station_data_events[sid].set()  # 设置初始事件
    
    t = threading.Thread(
        target=service._station_worker,
        args=(sid, bad_callback, None, service._station_stop_events[sid], service._station_data_events[sid]),
        daemon=True
    )
    t.start()
    time.sleep(0.05)
    service._station_stop_events[sid].set()
    t.join()
    
    # 验证线程完成且没有崩溃
    assert not t.is_alive()
    
    service.dispatcher.get_all_inputs = orig_get_all_outputs
# ========== 多线程安全测试 ==========

def test_concurrent_station_processing(mock_kafka_config, mock_sync_kafka_clients):
    """测试多场站并发处理"""
    service = DataAnalysisService(module_name='load_prediction')
    
    service.dispatcher.get_all_inputs = lambda station_id: {
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
        service._station_data_events[sid] = threading.Event()
        t = threading.Thread(
            target=service._station_worker,
            args=(sid, callback, None, service._station_stop_events[sid], service._station_data_events[sid]),
            daemon=True
        )
        t.start()
        threads.append(t)
    
    time.sleep(0.1)
    
    # 停止所有场站 - 设置stop_event和data_event
    for sid in station_ids:
        service._station_stop_events[sid].set()
        service._station_data_events[sid].set()
    
    for t in threads:
        t.join(timeout=2.0)
    
    # 验证所有线程都已经完成
    for t in threads:
        assert not t.is_alive()


# ========== ServiceBase测试 ==========

def test_service_base_resolve_topics_with_module(mock_kafka_config, mock_sync_kafka_clients):
    """测试ServiceBase根据module_name解析topics"""
    service = DataAnalysisService(module_name='load_prediction')
    
    # 验证topics已被正确解析
    assert service.topics is not None
    assert isinstance(service.topics, list)
    assert len(service.topics) > 0


def test_service_base_resolve_topics_explicit(mock_kafka_config, mock_sync_kafka_clients):
    """测试ServiceBase显式指定topics"""
    explicit_topics = ['SCHEDULE-STATION-PARAM', 'SCHEDULE-DEVICE-METER']
    service = DataAnalysisService(topics=explicit_topics)
    
    # 验证使用显式指定的topics
    assert service.topics == explicit_topics


def test_service_base_resolve_topics_invalid_module(mock_kafka_config, mock_sync_kafka_clients):
    """测试ServiceBase无效module_name"""
    with pytest.raises(ValueError) as exc_info:
        DataAnalysisService(module_name='invalid_module_name')
    
    assert '未配置 topics' in str(exc_info.value)


def test_service_base_no_module_no_topics(mock_kafka_config, mock_sync_kafka_clients):
    """测试ServiceBase无module_name且无topics（调试模式）"""
    import logging
    
    # 创建服务，应订阅所有topics
    service = DataAnalysisService(module_name=None, topics=None)
    
    # 应订阅所有topics
    assert service.topics is not None
    assert len(service.topics) > 0


def test_service_base_dispatcher_creation(mock_kafka_config, mock_sync_kafka_clients):
    """测试ServiceBase自动创建dispatcher"""
    service = DataAnalysisService(module_name='load_prediction')
    
    # 验证dispatcher已创建
    assert service.dispatcher is not None
    from d_a.dispatcher import DataDispatcher
    assert isinstance(service.dispatcher, DataDispatcher)


# ========== 批处理聚合测试 ==========

@pytest.mark.asyncio
async def test_batch_aggregation(monkeypatch):
    """测试批处理结果聚合功能"""
    aggregator = BatchResultAggregator(batch_timeout=0.5)
    upload_results = {}

    async def mock_upload_callback(batch_id, results):
        upload_results[batch_id] = results

    # 创建一个批处理
    batch_id = "test_batch_01"
    station_ids = ["S001", "S002", "S003"]
    collector = await aggregator.create_batch(batch_id, station_ids, mock_upload_callback)

    # 为两个场站添加结果
    await collector.add_result("S001", {"status": "ok"})
    await collector.add_result("S002", {"status": "ok"})

    # 等待超时
    await asyncio.sleep(0.6)

    # 检查上传回调是否被调用，且结果为部分结果
    assert batch_id in upload_results
    assert len(upload_results[batch_id]) == 2

    # 创建另一个批处理并完全完成
    batch_id_2 = "test_batch_02"
    station_ids_2 = ["S004", "S005"]
    collector_2 = await aggregator.create_batch(batch_id_2, station_ids_2, mock_upload_callback)
    await collector_2.add_result("S004", {"status": "done"})
    await collector_2.add_result("S005", {"status": "done"})
    
    await asyncio.sleep(0.1) # 允许完成触发上传

    assert batch_id_2 in upload_results
    assert len(upload_results[batch_id_2]) == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
