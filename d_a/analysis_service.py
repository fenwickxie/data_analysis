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
from collections import defaultdict

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
from .config import KAFKA_CONFIG, TOPIC_TO_MODULES, MODULE_TO_TOPICS, OFFSET_COMMIT_CONFIG
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
        offset_commit_config=None,
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
        
        # Offset管理相关
        self.offset_config = offset_commit_config or OFFSET_COMMIT_CONFIG
        self._pending_offsets = {}  # {TopicPartition: offset}
        self._processed_count = 0   # 已处理消息计数
        self._last_commit_time = time.time()  # 上次提交时间
        self._commit_lock = asyncio.Lock()  # 提交锁，确保线程安全

    async def _maybe_await(self, func, *args):
        if func is None:
            return None
        result = func(*args)
        if asyncio.iscoroutine(result):
            return await result
        return result
    
    async def _commit_offsets_with_retry(self, offsets_to_commit=None):
        """
        带重试机制的offset提交
        
        Args:
            offsets_to_commit: TopicPartition到OffsetAndMetadata的字典，为None时提交当前位置
            
        Returns:
            bool: 提交是否成功
        """
        max_retries = self.offset_config.get('max_commit_retries', 3)
        retry_delay = self.offset_config.get('commit_retry_delay', 1.0)
        
        for attempt in range(max_retries):
            try:
                success = await self.consumer.commit_offsets(offsets_to_commit)
                if success:
                    logging.info(f"成功提交offset: {len(offsets_to_commit) if offsets_to_commit else 'all'} 个分区")
                    return True
                else:
                    logging.warning(f"提交offset失败，尝试 {attempt + 1}/{max_retries}")
            except Exception as exc:
                logging.error(f"提交offset异常，尝试 {attempt + 1}/{max_retries}: {exc}")
            
            if attempt < max_retries - 1:
                await asyncio.sleep(retry_delay)
        
        logging.error("达到最大重试次数，offset提交失败")
        return False
    
    async def _should_commit_offsets(self):
        """
        判断是否应该提交offset
        
        Returns:
            bool: 是否应该提交
        """
        commit_interval = self.offset_config.get('commit_interval_seconds', 5.0)
        commit_batch_size = self.offset_config.get('commit_batch_size', 100)
        
        # 检查是否达到批次大小
        if self._processed_count >= commit_batch_size:
            return True
        
        # 检查是否达到时间间隔
        if time.time() - self._last_commit_time >= commit_interval:
            return True
        
        return False
    
    async def _process_and_track_message(self, msg):
        """
        处理单条消息并跟踪其offset
        
        职责：
        1. 解码消息
        2. 提取场站列表
        3. 分发原始数据到各场站的dispatcher
        
        Args:
            msg: Kafka消息对象
            
        Returns:
            bool: 消息是否成功处理
        """
        topic = msg.topic
        try:
            value_str = msg.value.decode("utf-8")
            value = json.loads(value_str)

            # 提取场站列表和对应的数据
            station_data_list = self._extract_station_data(topic, value)
            
            if not station_data_list:
                logging.debug(f"消息中没有提取到场站数据: topic={topic}")
                return False
            
            # 处理每个场站的数据
            all_success = True
            for station_id, station_data in station_data_list:
                try:
                    # 直接将原始数据交给dispatcher
                    # parser会在dispatcher.get_module_input()时被调用
                    self.dispatcher.update_topic_data(station_id, topic, station_data)
                    
                    # 创建或管理场站任务
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
                    
                except Exception as exc:
                    handle_error(
                        DispatcherError(exc),
                        context=f"update_topic_data station_id={station_id}, topic={topic}",
                    )
                    all_success = False
            
            # 只有所有场站数据都处理成功才记录offset
            if all_success and len(station_data_list) > 0:
                tp_key = (msg.topic, msg.partition)
                self._pending_offsets[tp_key] = msg.offset + 1
                return True
            
            return False
            
        except Exception as exc:
            handle_error(exc, context=f"处理消息 topic={topic}, partition={msg.partition}, offset={msg.offset}")
            return False
    
    def _extract_station_data(self, topic, value):
        """
        从消息中提取场站数据列表
        
        职责：只负责识别消息格式并提取场站数据，不做任何解析
        
        Args:
            topic: topic名称
            value: 解析后的JSON数据（dict或list）
            
        Returns:
            list: [(station_id, raw_data), ...] 场站ID和对应原始数据的列表
        """
        station_data_list = []
        
        try:
            # 格式1：直接列表 [{'stationId': '...', ...}, ...]
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        station_id = item.get('stationId')
                        if station_id:
                            station_data_list.append((station_id, item))
                return station_data_list
            
            # 格式2：字典格式
            if not isinstance(value, dict):
                return station_data_list
            
            # 格式2a：{'realTimeData': [{...}, ...]}
            real_time_data = value.get('realTimeData')
            if real_time_data and isinstance(real_time_data, list):
                for item in real_time_data:
                    if isinstance(item, dict):
                        station_id = item.get('stationId')
                        if station_id:
                            station_data_list.append((station_id, item))
                return station_data_list
            
            # 格式2b：单场站数据 {'stationId': '...', ...}
            station_id = value.get('stationId')
            if station_id:
                station_data_list.append((station_id, value))
                return station_data_list
            
            # 格式2c：全局数据（无stationId）
            # 广播到所有已知场站
            if not station_id:
                known_stations = list(self._station_tasks.keys())
                if known_stations:
                    for sid in known_stations:
                        station_data_list.append((sid, value))
                else:
                    # 如果还没有场站，存储到特殊的全局标识
                    station_data_list.append(('__global__', value))
                return station_data_list
            
        except Exception as exc:
            handle_error(exc, context=f"提取场站数据 topic={topic}")
        
        return station_data_list

    async def _station_worker(self, station_id, callback, result_handler, stop_flag):
        while not stop_flag.is_set():
            try:
                # 获取解析后的输入数据
                inputs = self.dispatcher.get_all_inputs(station_id)
                module_input = (
                    inputs.get(self.module_name) if self.module_name else inputs
                )
                result = None
                if callback:
                    try:
                        # 调用业务回调（传给业务模块）
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
                    # 即使没有消息，也检查是否需要定时提交
                    if self._pending_offsets and await self._should_commit_offsets():
                        async with self._commit_lock:
                            await self._commit_pending_offsets()
                    await asyncio.sleep(0.2)
                    continue
                
                # 处理批次中的每条消息
                successful_count = 0
                for msg in batch:
                    if await self._process_and_track_message(msg):
                        successful_count += 1
                
                # 更新处理计数
                self._processed_count += successful_count
                
                # 检查是否需要提交offset
                if self._pending_offsets and await self._should_commit_offsets():
                    async with self._commit_lock:
                        await self._commit_pending_offsets()
                
                # 清理过期数据
                try:
                    self.dispatcher.clean_expired()
                except Exception as exc:  # noqa: BLE001
                    handle_error(DispatcherError(exc), context="clean_expired")
        finally:
            # 停止前提交所有待处理的offset
            if self._pending_offsets:
                logging.info("服务停止，提交剩余offset...")
                async with self._commit_lock:
                    await self._commit_pending_offsets()
            
            await self.consumer.stop()
            for flag in self._station_stop_flags.values():
                flag.set()
            if self._station_tasks:
                await asyncio.gather(
                    *self._station_tasks.values(), return_exceptions=True
                )
    
    async def _commit_pending_offsets(self):
        """
        提交所有待处理的offset
        """
        if not self._pending_offsets:
            return
        
        try:
            # 构造aiokafka需要的offset字典
            # aiokafka的commit需要TopicPartition对象
            from aiokafka import TopicPartition, OffsetAndMetadata
            
            offsets_dict = {}
            for (topic, partition), offset in self._pending_offsets.items():
                tp = TopicPartition(topic, partition)
                offsets_dict[tp] = OffsetAndMetadata(offset, "")
            
            # 提交offset
            success = await self._commit_offsets_with_retry(offsets_dict)
            
            if success:
                # 提交成功，清空待处理offset和计数器
                committed_count = len(self._pending_offsets)
                processed = self._processed_count
                self._pending_offsets.clear()
                self._processed_count = 0
                self._last_commit_time = time.time()
                logging.info(f"成功提交 {committed_count} 个分区的offset，共处理 {processed} 条消息")
            else:
                # 提交失败，保留待处理offset供下次重试
                logging.warning(f"Offset提交失败，保留 {len(self._pending_offsets)} 个待处理offset")
        
        except Exception as exc:
            logging.error(f"提交pending offsets时发生异常: {exc}")
            # 异常时也保留offset供下次重试

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
                inputs = self.dispatcher.get_all_inputs(station_id)
                module_input = (
                    inputs.get(self.module_name) if self.module_name else inputs
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
                    handle_error(
                        exc, context="Kafka消费", recover=lambda: time.sleep(2)
                    )
                    time.sleep(2)
                    continue
                for tp, msgs in msg_pack.items():
                    topic = tp.topic
                    for msg in msgs:
                        value = msg.value
                        
                        try:
                            # 提取场站列表和对应的数据
                            station_data_list = self._extract_station_data_sync(topic, value)
                            
                            if not station_data_list:
                                logging.debug(f"消息中没有提取到场站数据: topic={topic}")
                                continue
                            
                            # 处理每个场站的数据
                            for station_id, station_data in station_data_list:
                                try:
                                    # 直接将原始数据交给dispatcher
                                    self.dispatcher.update_topic_data(station_id, topic, station_data)
                                    
                                    # 创建或管理场站线程
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
                                except Exception as exc:
                                    handle_error(
                                        DispatcherError(exc),
                                        context=f"update_topic_data station_id={station_id}, topic={topic}",
                                    )
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
    
    def _extract_station_data_sync(self, topic, value):
        """
        从消息中提取场站数据列表（同步版本）
        
        Args:
            topic: topic名称
            value: 解析后的JSON数据（dict或list）
            
        Returns:
            list: [(station_id, raw_data), ...] 场站ID和对应原始数据的列表
        """
        station_data_list = []
        
        try:
            # 格式1：直接列表 [{'stationId': '...', ...}, ...]
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        station_id = item.get('stationId')
                        if station_id:
                            station_data_list.append((station_id, item))
                return station_data_list
            
            # 格式2：字典格式
            if not isinstance(value, dict):
                return station_data_list
            
            # 格式2a：{'realTimeData': [{...}, ...]}
            real_time_data = value.get('realTimeData')
            if real_time_data and isinstance(real_time_data, list):
                for item in real_time_data:
                    if isinstance(item, dict):
                        station_id = item.get('stationId')
                        if station_id:
                            station_data_list.append((station_id, item))
                return station_data_list
            
            # 格式2b：单场站数据 {'stationId': '...', ...}
            station_id = value.get('stationId')
            if station_id:
                station_data_list.append((station_id, value))
                return station_data_list
            
            # 格式2c：全局数据（无stationId）
            # 广播到所有已知场站
            if not station_id:
                known_stations = list(self._station_threads.keys())
                if known_stations:
                    for sid in known_stations:
                        station_data_list.append((sid, value))
                else:
                    # 如果还没有场站，存储到特殊的全局标识
                    station_data_list.append(('__global__', value))
                return station_data_list
            
        except Exception as exc:
            handle_error(exc, context=f"提取场站数据 topic={topic}")
        
        return station_data_list

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
