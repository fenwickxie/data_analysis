#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:11:10
project: data_analysis
filename: test_integration_extra.py
version: 1.0
"""

import pytest
import threading
import time
import asyncio
from d_a.analysis_service import DataAnalysisService, AsyncDataAnalysisService

def test_health_monitoring():
    service = DataAnalysisService(module_name='load_prediction')
    # 模拟dispatcher输出
    service.dispatcher.get_all_inputs = lambda sid: {'load_prediction': {'timestamp': 123}}
    sid = 'station_health'
    service._station_stop_events[sid] = threading.Event()
    t = threading.Thread(target=service._station_worker, args=(sid, lambda s, m: {'result': 1}, service._station_stop_events[sid]), daemon=True)
    t.start()
    time.sleep(0.05)
    status = service.get_station_status()
    assert sid in status
    assert 'running' in status[sid]
    service._station_stop_events[sid].set()
    t.join()
    assert status[sid]['running'] is False

def test_reload_config(monkeypatch):
    service = DataAnalysisService(module_name='load_prediction')
    called = {}
    def fake_reload(cfg):
        called['ok'] = True
    service.dispatcher.reload_config = fake_reload
    monkeypatch.setattr('data_analysis.config.KAFKA_CONFIG', {'bootstrap_servers': 'localhost:9092', 'group_id': 'g'})
    monkeypatch.setattr('data_analysis.config.TOPIC_TO_MODULES', {'T': ['load_prediction']})
    service.reload_config()
    assert called.get('ok')

@pytest.mark.asyncio
async def test_async_add_remove_station():
    service = AsyncDataAnalysisService(module_name='load_prediction')
    # mock dispatcher
    service.dispatcher.get_all_inputs = lambda sid: {'load_prediction': {'timestamp': 1}}
    async def cb(station_id, module_input):
        return {'result': 2}
    await service.add_station('sid_async', cb)
    await asyncio.sleep(0.05)
    status = service.get_station_status()
    assert 'sid_async' in status
    await service.remove_station('sid_async')
    await asyncio.sleep(0.01)
    status2 = service.get_station_status()
    assert not status2['sid_async']['running']

def test_log_output(tmp_path, caplog):
    import logging
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
