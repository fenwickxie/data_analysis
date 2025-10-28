#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:11:17
project: data_analysis
filename: test_kafka_upload.py
version: 1.0
"""

from data_analysis.analysis_service import DataAnalysisService
import threading
import time

def dummy_callback(station_id, module_input):
    return {'result': 42}

def test_kafka_upload(monkeypatch):
    # 模拟KafkaProducerClient.send
    sent = {}
    def fake_send(self, topic, value):
        sent['topic'] = topic
        sent['value'] = value
    monkeypatch.setattr('data_analysis.kafka_producer.KafkaProducerClient.send', fake_send)
    service = DataAnalysisService(module_name='load_prediction')
    # 模拟dispatcher输出
    service.dispatcher.get_all_inputs = lambda sid: {'load_prediction': {'timestamp': 123}}
    # 启动一个场站线程
    sid = 'station1'
    service._station_stop_events[sid] = threading.Event()
    t = threading.Thread(target=service._station_worker, args=(sid, dummy_callback), daemon=True)
    t.start()
    time.sleep(0.1)
    service._station_stop_events[sid].set()
    t.join()
    assert sent['topic'].startswith('MODULE-OUTPUT-LOAD_PREDICTION')
    assert sent['value']['station_id'] == sid
    assert sent['value']['output']['result'] == 42
