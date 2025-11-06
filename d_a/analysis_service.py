#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:09:32
project: data_analysis
filename: analysis_service.py
version: 1.0
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import time
import threading

# 使用相对导入避免循环依赖
from .errors import (
    DataAnalysisError,
    KafkaConnectionError,
    DispatcherError,
    handle_error,
)
from .kafka_client import (
    KafkaConsumerClient,
    AsyncKafkaConsumerClient,
)
from .config import KAFKA_CONFIG, TOPIC_TO_MODULES, MODULE_TO_TOPICS
from .dispatcher import DataDispatcher


class AsyncDataAnalysisService:
    """异步数据解析服务，负责消费Kafka并交由业务回调处理结果。"""

    def __init__(
        self,
        module_name=None,
        topics=None,
        kafka_config=None,
        data_expire_seconds=600,
        result_handler=None,
    ):
        self.module_name = module_name
        if topics is not None:
            self.topics = topics
        elif module_name:
            default_topics = MODULE_TO_TOPICS.get(module_name) or []
            self.topics = list(dict.fromkeys(default_topics)) or list(
                TOPIC_TO_MODULES.keys()
            )
        else:
            self.topics = list(TOPIC_TO_MODULES.keys())
        self.kafka_config = kafka_config or KAFKA_CONFIG
        self.dispatcher = DataDispatcher(data_expire_seconds=data_expire_seconds)
        self.consumer = AsyncKafkaConsumerClient(self.topics, self.kafka_config)
        self._station_tasks = {}
        self._station_stop_flags = {}
        self._main_task = None
        self._stop_event = asyncio.Event()
        self._callback = None
        self._result_handler = result_handler

    async def _maybe_await(self, func, *args):
        if func is None:
            return None
        result = func(*args)
        if asyncio.iscoroutine(result):
            return await result
        return result

    async def _station_worker(self, station_id, callback, result_handler, stop_flag):
        while not stop_flag.is_set():
            try:
                inputs = self.dispatcher.get_all_inputs(station_id)
                module_input = (
                    inputs.get(self.module_name) if self.module_name else inputs
                )
                result = None
                if callback:
                    try:
                        result = await self._maybe_await(
                            callback, station_id, module_input
                        )
                    except Exception as exc:  # noqa: BLE001
                        handle_error(exc, context=f"回调处理 station_id={station_id}")
                if result_handler:
                    try:
                        await self._maybe_await(
                            result_handler, station_id, module_input, result
                        )
                    except Exception as exc:  # noqa: BLE001
                        handle_error(exc, context=f"结果处理 station_id={station_id}")
            except Exception as exc:  # noqa: BLE001
                handle_error(exc, context=f"场站任务 station_id={station_id}")
            await asyncio.sleep(2)

    async def _main_loop(self):
        try:
            await self.consumer.start()
        except Exception as exc:  # noqa: BLE001
            handle_error(KafkaConnectionError(exc), context="KafkaConsumer连接")
            await asyncio.sleep(5)
            return
        try:
            while not self._stop_event.is_set():
                try:
                    batch = await self.consumer.getmany(timeout_ms=1000)
                except Exception as exc:  # noqa: BLE001
                    handle_error(
                        exc, context="Kafka消费", recover=lambda: asyncio.sleep(2)
                    )
                    await asyncio.sleep(2)
                    continue
                if not batch:
                    await asyncio.sleep(0.2)
                    continue
                for msg in batch:
                    topic = msg.topic
                    value_str = msg.value.decode("utf-8") # New line to decode bytes to string
                    value=json.loads(value_str) # New line to parse JSON string to dict
                    
                    if not isinstance(value, dict):
                        handle_error(
                            DataAnalysisError("Kafka消息value为None或非dict"),
                            context=f"topic={topic}, value={value}",
                        )
                        continue
                    station_id = (
                        value.get("station_id")
                        or value.get("host_id")
                        or value.get("meter_id")
                    )
                    if not station_id:
                        continue
                    try:
                        self.dispatcher.update_topic_data(station_id, topic, value)
                    except Exception as exc:  # noqa: BLE001
                        handle_error(
                            DispatcherError(exc),
                            context=f"update_topic_data station_id={station_id}, topic={topic}",
                        )
                    if station_id not in self._station_tasks:
                        stop_flag = asyncio.Event()
                        self._station_stop_flags[station_id] = stop_flag
                        task = asyncio.create_task(
                            self._station_worker(
                                station_id,
                                self._callback,
                                self._result_handler,
                                stop_flag,
                            )
                        )
                        self._station_tasks[station_id] = task
                try:
                    self.dispatcher.clean_expired()
                except Exception as exc:  # noqa: BLE001
                    handle_error(DispatcherError(exc), context="clean_expired")
        finally:
            await self.consumer.stop()
            for flag in self._station_stop_flags.values():
                flag.set()
            if self._station_tasks:
                await asyncio.gather(
                    *self._station_tasks.values(), return_exceptions=True
                )

    async def start(self, callback=None, result_handler=None):
        self._callback = callback or self._callback
        if result_handler is not None:
            self._result_handler = result_handler
        self._main_task = asyncio.create_task(self._main_loop())

    async def stop(self):
        self._stop_event.set()
        if self._main_task:
            await self._main_task

    def get_inputs(self, station_id):
        return self.dispatcher.get_all_inputs(station_id)

    async def add_station(self, station_id, callback=None, result_handler=None):
        if station_id in self._station_tasks:
            return
        stop_flag = asyncio.Event()
        self._station_stop_flags[station_id] = stop_flag
        task = asyncio.create_task(
            self._station_worker(
                station_id,
                callback or self._callback,
                result_handler if result_handler is not None else self._result_handler,
                stop_flag,
            )
        )
        self._station_tasks[station_id] = task

    async def remove_station(self, station_id):
        """
        动态移除场站异步任务
        """
        if station_id in self._station_stop_flags:
            self._station_stop_flags[station_id].set()
        if station_id in self._station_tasks:
            await self._station_tasks[station_id]
            del self._station_tasks[station_id]
            del self._station_stop_flags[station_id]

    def get_station_status(self):
        """
        获取所有场站异步任务健康状态。
        返回: dict {station_id: {'running': bool, 'task': asyncio.Task}}
        """
        status = {}
        for sid, task in self._station_tasks.items():
            status[sid] = {"running": not task.done(), "task": task}
        return status


class DataAnalysisService:
    """同步数据解析服务，负责从Kafka消费数据并驱动业务回调。"""

    def __init__(
        self,
        module_name=None,
        topics=None,
        kafka_config=None,
        data_expire_seconds=600,
        result_handler=None,
    ):
        self.module_name = module_name
        if topics is not None:
            self.topics = topics
        elif module_name:
            default_topics = MODULE_TO_TOPICS.get(module_name) or []
            self.topics = list(dict.fromkeys(default_topics)) or list(
                TOPIC_TO_MODULES.keys()
            )
        else:
            self.topics = list(TOPIC_TO_MODULES.keys())
        self.kafka_config = kafka_config or KAFKA_CONFIG
        self.dispatcher = DataDispatcher(data_expire_seconds=data_expire_seconds)
        try:
            self.consumer = KafkaConsumerClient(self.topics, self.kafka_config)
        except Exception as exc:  # noqa: BLE001
            logging.error(f"Kafka连接初始化失败: {exc}")
            raise
        self._callback = None
        self._result_handler = result_handler
        self._stop_event = threading.Event()
        self._executor = ThreadPoolExecutor(max_workers=32)
        self._thread = None
        self._station_threads = {}
        self._station_stop_events = {}

    def _station_worker(self, station_id, callback, result_handler, stop_event):
        while not stop_event.is_set():
            try:
                outputs = self.dispatcher.get_all_inputs(station_id)
                module_input = (
                    outputs.get(self.module_name) if self.module_name else outputs
                )
                result = None
                if callback:
                    try:
                        result = callback(station_id, module_input)
                    except Exception as exc:  # noqa: BLE001
                        handle_error(exc, context=f"回调处理 station_id={station_id}")
                if result_handler:
                    try:
                        result_handler(station_id, module_input, result)
                    except Exception as exc:  # noqa: BLE001
                        handle_error(exc, context=f"结果处理 station_id={station_id}")
            except Exception as exc:  # noqa: BLE001
                handle_error(exc, context=f"场站线程 station_id={station_id}")
            time.sleep(2)

    def _main_loop(self):
        try:
            while not self._stop_event.is_set():
                try:
                    msg_pack = self.consumer.poll(timeout_ms=1000)
                except Exception as exc:  # noqa: BLE001
                    handle_error(exc, context="Kafka消费", recover=lambda: time.sleep(2))
                    time.sleep(2)
                    continue
                for tp, msgs in msg_pack.items():
                    topic = tp.topic
                    for msg in msgs:
                        value = msg.value
                        if not isinstance(value, dict):
                            handle_error(
                                DataAnalysisError("Kafka消息value为None或非dict"),
                                context=f"topic={topic}, value={value}",
                            )
                            continue
                        try:
                            station_id = (
                                value.get("station_id")
                                or value.get("host_id")
                                or value.get("meter_id")
                            )
                            if not station_id:
                                continue
                            self.dispatcher.update_topic_data(station_id, topic, value)
                            if station_id not in self._station_threads:
                                stop_event = threading.Event()
                                self._station_stop_events[station_id] = stop_event
                                future = self._executor.submit(
                                    self._station_worker,
                                    station_id,
                                    self._callback,
                                    self._result_handler,
                                    stop_event,
                                )
                                self._station_threads[station_id] = future
                        except Exception as exc:  # noqa: BLE001
                            handle_error(
                                DispatcherError(exc), context=f"数据解析 topic={topic}"
                            )
                try:
                    self.dispatcher.clean_expired()
                except Exception as exc:  # noqa: BLE001
                    handle_error(DispatcherError(exc), context="数据清理")
                time.sleep(1)
        finally:
            for event in self._station_stop_events.values():
                event.set()
            for future in self._station_threads.values():
                try:
                    future.result(timeout=5)
                except Exception:
                    pass
            self._executor.shutdown(wait=True)
            try:
                self.consumer.close()
            except Exception as exc:  # noqa: BLE001
                handle_error(exc, context="KafkaConsumer关闭")

    def start(self, callback=None, result_handler=None, background=True):
        self._callback = callback or self._callback
        if result_handler is not None:
            self._result_handler = result_handler
        if background:
            self._thread = threading.Thread(target=self._main_loop, daemon=True)
            self._thread.start()
        else:
            self._main_loop()

    def stop(self):
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join()

    def get_inputs(self, station_id):
        return self.dispatcher.get_all_inputs(station_id)

    def get_outputs(self, station_id):
        return self.get_inputs(station_id)

    def reload_config(self):
        import importlib
        import d_a.config as config_mod

        importlib.reload(config_mod)
        global KAFKA_CONFIG, TOPIC_TO_MODULES, MODULE_TO_TOPICS
        KAFKA_CONFIG = config_mod.KAFKA_CONFIG
        TOPIC_TO_MODULES = config_mod.TOPIC_TO_MODULES
        MODULE_TO_TOPICS = config_mod.MODULE_TO_TOPICS
        if hasattr(self.dispatcher, "reload_config"):
            self.dispatcher.reload_config(config_mod)
        logging.info("配置热更新完成")

    def get_station_status(self):
        status = {}
        for sid, fut in self._station_threads.items():
            status[sid] = {"running": fut.running(), "future": fut}
        return status

    def get_service_status(self):
        return {
            "main_thread_alive": self._thread.is_alive() if self._thread else False,
            "consumer_alive": self.consumer is not None,
            "result_handler_registered": self._result_handler is not None,
            "station_count": len(self._station_threads),
        }
