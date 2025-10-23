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
import logging
import time
import threading

# 使用相对导入避免循环依赖
from . import (
    DataAnalysisError,
    KafkaConnectionError,
    DispatcherError,
    handle_error,
)
from .kafka_client import KafkaConsumerClient, KafkaProducerClient, AsyncKafkaConsumerClient, AsyncKafkaProducerClient
from .config import KAFKA_CONFIG, TOPIC_TO_MODULES
from .dispatcher import DataDispatcher


class AsyncDataAnalysisService:
    """
    异步数据解析服务，支持多场站并发、窗口补全/插值、依赖聚合、自动上传Kafka。
    
    该服务基于asyncio实现，适合高并发、云原生场景，提供比同步服务更高的吞吐量。
    每个场站使用独立的异步任务处理数据，支持异步回调函数，
    可以更好地处理IO密集型和计算密集型任务。
    
    主要特性：
    - 基于asyncio的高并发处理
    - 支持异步回调函数，适合模型推理、远程调用等场景
    - 动态添加/移除场站任务
    - 完整的任务状态监控
    - 与同步服务相同的窗口处理和依赖聚合功能
    
    Args:
        module_name (str, optional): 业务模块名称，用于标识当前处理模块
        topics (list, optional): 需要消费的topic列表，默认为配置文件中所有topic
        kafka_config (dict, optional): Kafka连接配置，默认为config.KAFKA_CONFIG
        data_expire_seconds (int, optional): 数据过期时间(秒)，默认为600
        output_topic_prefix (str, optional): 输出topic前缀，默认为"MODULE-OUTPUT-"
        
    Raises:
        KafkaConnectionError: 当Kafka连接失败时抛出
    """

    def __init__(
        self,
        module_name=None,
        topics=None,
        kafka_config=None,
        data_expire_seconds=600,
        output_topic_prefix="MODULE-OUTPUT-",
    ):
        self.module_name = module_name
        self.topics = topics or list(TOPIC_TO_MODULES.keys())
        self.kafka_config = kafka_config or KAFKA_CONFIG
        self.dispatcher = DataDispatcher(data_expire_seconds=data_expire_seconds)
        self.consumer = AsyncKafkaConsumerClient(self.topics, self.kafka_config)
        self.producer = AsyncKafkaProducerClient(self.kafka_config)
        self.output_topic = (
            f"{output_topic_prefix}{module_name.upper()}" if module_name else None
        )
        self._station_tasks = {}
        self._station_stop_flags = {}
        self._main_task = None
        self._stop_event = asyncio.Event()

    async def _station_worker(self, station_id, callback, stop_flag):
        while not stop_flag.is_set():
            try:
                outputs = self.dispatcher.get_all_outputs(station_id)
                module_input = (
                    outputs.get(self.module_name) if self.module_name else outputs
                )
                result = None
                if callback:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            result = await callback(station_id, module_input)
                        else:
                            result = callback(station_id, module_input)
                    except Exception as e:
                        handle_error(e, context=f"回调处理 station_id={station_id}")
                if self.output_topic and module_input is not None:
                    out = result if result is not None else module_input
                    out_data = {
                        "station_id": station_id,
                        "output": out,
                        "timestamp": (
                            module_input.get("timestamp")
                            if isinstance(module_input, dict)
                            else None
                        ),
                    }
                    try:
                        await self.producer.send(self.output_topic, out_data)
                    except Exception as e:
                        handle_error(e, context=f"Kafka上传 station_id={station_id}")
            except Exception as e:
                handle_error(e, context=f"场站任务 station_id={station_id}")
            await asyncio.sleep(2)

    async def _main_loop(self, callback=None):
        try:
            await self.consumer.start()
        except Exception as e:
            handle_error(KafkaConnectionError(e), context="KafkaConsumer连接")
            await asyncio.sleep(5)
            return
        try:
            await self.producer.start()
        except Exception as e:
            handle_error(KafkaConnectionError(e), context="KafkaProducer连接")
            await asyncio.sleep(5)
            return
        try:
            while not self._stop_event.is_set():
                try:
                    msg = await self.consumer.getone()
                except Exception as e:
                    handle_error(
                        e, context="Kafka消费", recover=lambda: asyncio.sleep(2)
                    )
                    await asyncio.sleep(2)
                    continue
                topic = msg.topic
                value = msg.value
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
                except Exception as e:
                    handle_error(
                        DispatcherError(e),
                        context=f"update_topic_data station_id={station_id}, topic={topic}",
                    )
                if station_id not in self._station_tasks:
                    stop_flag = asyncio.Event()
                    self._station_stop_flags[station_id] = stop_flag
                    task = asyncio.create_task(
                        self._station_worker(station_id, callback, stop_flag)
                    )
                    self._station_tasks[station_id] = task
                try:
                    self.dispatcher.clean_expired()
                except Exception as e:
                    handle_error(DispatcherError(e), context="clean_expired")
        finally:
            await self.consumer.stop()
            await self.producer.stop()
            for flag in self._station_stop_flags.values():
                flag.set()
            await asyncio.gather(*self._station_tasks.values(), return_exceptions=True)

    async def start(self, callback=None):
        self._main_task = asyncio.create_task(self._main_loop(callback))

    async def stop(self):
        self._stop_event.set()
        if self._main_task:
            await self._main_task

    def get_outputs(self, station_id):
        return self.dispatcher.get_all_outputs(station_id)

    async def add_station(self, station_id, callback):
        """
        动态添加场站异步任务
        """
        if station_id in self._station_tasks:
            return
        stop_flag = asyncio.Event()
        self._station_stop_flags[station_id] = stop_flag
        task = asyncio.create_task(
            self._station_worker(station_id, callback, stop_flag)
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
    """
    通用数据解析服务，可嵌入各业务模型或独立运行。
    
    该服务从Kafka消费各topic数据，解析并整合为各业务模块所需格式，
    提供统一的数据分发接口，支持多场站并发处理和自动上传到Kafka。
    
    主要功能：
    - 多场站多线程，自动缓存窗口数据，补零/插值
    - 自动处理模型间依赖关系
    - 自动上传本模型输出到Kafka供其他模型消费
    - 支持自定义回调处理
    - 支持配置热更新
    
    Args:
        module_name (str, optional): 业务模块名称，用于标识当前处理模块
        topics (list, optional): 需要消费的topic列表，默认为配置文件中所有topic
        kafka_config (dict, optional): Kafka连接配置，默认为config.KAFKA_CONFIG
        data_expire_seconds (int, optional): 数据过期时间(秒)，默认为600
        output_topic_prefix (str, optional): 输出topic前缀，默认为"MODULE-OUTPUT-"
        
    Raises:
        KafkaConnectionError: 当Kafka连接失败时抛出
    """

    def __init__(
        self,
        module_name=None,
        topics=None,
        kafka_config=None,
        data_expire_seconds=600,
        output_topic_prefix="MODULE-OUTPUT-",
    ):
        """
        初始化数据解析服务，自动连接Kafka，初始化分发器。
        
        创建服务实例时会自动初始化Kafka消费者和生产者，
        并设置数据分发器。服务启动前不会实际连接Kafka。
        
        Args:
            module_name (str, optional): 业务模块名称，用于标识当前处理模块。
                如果指定，服务将只处理该模块所需的数据，并将结果上传到对应的输出topic。
            topics (list, optional): 需要消费的topic列表，默认为配置文件中所有topic。
                如果指定，服务将只消费这些topic的数据。
            kafka_config (dict, optional): Kafka连接配置，默认为config.KAFKA_CONFIG。
                包含bootstrap_servers、group_id等连接参数。
            data_expire_seconds (int, optional): 数据过期时间(秒)，默认为600。
                超过此时间未更新的场站数据将被自动清理。
            output_topic_prefix (str, optional): 输出topic前缀，默认为"MODULE-OUTPUT-"。
                完整的输出topic为"前缀+模块名大写"。
                
        Raises:
            KafkaConnectionError: 当Kafka连接初始化失败时抛出
        """
        self.module_name = module_name
        self.topics = topics or list(TOPIC_TO_MODULES.keys())
        self.kafka_config = kafka_config or KAFKA_CONFIG
        self.dispatcher = DataDispatcher(data_expire_seconds=data_expire_seconds)
        try:
            self.consumer = KafkaConsumerClient(self.topics, self.kafka_config)
            self.producer = KafkaProducerClient(self.kafka_config)
        except Exception as e:
            logging.error(f"Kafka连接初始化失败: {e}")
            raise
        self.output_topic = (
            f"{output_topic_prefix}{module_name.upper()}" if module_name else None
        )
        self._stop_event = threading.Event()
        self._executor = ThreadPoolExecutor(max_workers=32)  # 可根据实际场站规模调整
        self._thread = None
        self._station_threads = {}
        self._station_stop_events = {}

    def _station_worker(self, station_id, callback, stop_event):
        while not stop_event.is_set():
            try:
                outputs = self.dispatcher.get_all_outputs(station_id)
                module_input = (
                    outputs.get(self.module_name) if self.module_name else outputs
                )
                result = None
                if callback:
                    try:
                        result = callback(station_id, module_input)
                    except Exception as e:
                        handle_error(e, context=f"回调处理 station_id={station_id}")
                if self.output_topic and module_input is not None:
                    out = result if result is not None else module_input
                    out_data = {
                        "station_id": station_id,
                        "output": out,
                        "timestamp": (
                            module_input.get("timestamp")
                            if isinstance(module_input, dict)
                            else None
                        ),
                    }
                    try:
                        self.producer.send(self.output_topic, out_data)
                    except Exception as e:
                        handle_error(e, context=f"Kafka上传 station_id={station_id}")
            except Exception as e:
                handle_error(e, context=f"场站线程 station_id={station_id}")
            time.sleep(2)

    def _main_loop(self, callback=None):
        try:
            while not self._stop_event.is_set():
                try:
                    msg_pack = self.consumer.poll(timeout_ms=1000)
                except Exception as e:
                    handle_error(e, context="Kafka消费", recover=lambda: time.sleep(2))
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
                                    callback,
                                    stop_event,
                                )
                                self._station_threads[station_id] = future
                        except Exception as e:
                            handle_error(
                                DispatcherError(e), context=f"数据解析 topic={topic}"
                            )
                try:
                    self.dispatcher.clean_expired()
                except Exception as e:
                    handle_error(DispatcherError(e), context="数据清理")
                time.sleep(1)
        finally:
            try:
                self.consumer.close()
                self.producer.close()
                self._executor.shutdown(wait=True)
            except Exception as e:
                handle_error(e, context="Kafka关闭")
            for e in self._station_stop_events.values():
                e.set()

    def start(self, callback=None, background=True):
        """
        启动数据解析服务。
        
        启动后会开始从Kafka消费数据，并根据配置处理数据。
        如果指定了回调函数，每个场站数据更新时都会调用该函数。
        
        Args:
            callback (function, optional): 回调函数(station_id, module_input)，
                每个场站数据更新时调用。回调函数接收场站ID和模块输入数据，
                返回值会自动上传到Kafka。默认为None。
            background (bool, optional): 是否后台线程运行。如果为True，
                服务将在后台线程中运行；如果为False，服务将阻塞当前线程。
                默认为True。
                
        注意:
            - 如果background=True，需要调用stop()方法停止服务
            - 回调函数应尽量保持轻量，避免耗时操作
            - 回调函数中的错误会被系统捕获并记录，不会影响整体服务运行
        """
        if background:
            self._thread = threading.Thread(
                target=self._main_loop, args=(callback,), daemon=True
            )
            self._thread.start()
        else:
            self._main_loop(callback)

    def stop(self):
        """
        停止数据解析服务。
        
        停止后会关闭Kafka连接，停止所有场站线程，并清理资源。
        如果服务是在后台线程中运行的，此方法会等待线程结束。
        
        注意:
            - 调用此方法后，服务将无法继续处理数据
            - 如果服务是前台运行的，此方法会阻塞直到服务完全停止
            - 停止后需要重新创建服务实例才能再次启动
        """
        self._stop_event.set()
        if self._thread:
            self._thread.join()

    def get_outputs(self, station_id):
        """
        获取指定场站所有模块的输入结构
        
        返回的数据包含所有业务模块的输入，每个模块的数据已经过窗口处理和依赖聚合。
        如果指定场站没有数据，返回空字典。
        
        Args:
            station_id (str): 场站ID
            
        Returns:
            dict: 包含所有模块输入数据的字典，格式为 {module_name: module_input}
                  module_input是各模块所需的结构化数据
                  
        注意:
            - 此方法返回的是当前缓存的数据，可能不是最新的
            - 如果场站数据已过期，可能返回空值或不完整数据
            - 建议在回调函数中使用此方法获取其他模块的数据
        """
        return self.dispatcher.get_all_outputs(station_id)

    def reload_config(self):
        """
        动态热加载config.py配置（topic、窗口、依赖等），并同步dispatcher。
        
        此方法允许在不重启服务的情况下更新配置，包括topic映射、
        数据窗口大小、模块依赖关系等。更新后的配置会立即生效，
        但已缓存的数据不会自动清理，需要等待自然过期或手动清理。
        
        使用场景：
        - 需要添加新的topic或模块
        - 需要调整数据窗口大小
        - 需要修改模块依赖关系
        - 需要更新Kafka连接参数
        
        注意:
        - 配置文件修改后需要调用此方法才能生效
        - Kafka连接参数修改后可能需要重启服务才能生效
        - 某些配置修改可能导致数据不一致，请谨慎操作
        - 建议在低峰期执行配置更新
        """
        import importlib
        import data_analysis.config as config_mod

        importlib.reload(config_mod)
        # 更新本地配置
        global KAFKA_CONFIG, TOPIC_TO_MODULES
        KAFKA_CONFIG = config_mod.KAFKA_CONFIG
        TOPIC_TO_MODULES = config_mod.TOPIC_TO_MODULES
        # 通知dispatcher热更新
        if hasattr(self.dispatcher, "reload_config"):
            self.dispatcher.reload_config(config_mod)
        logging.info("配置热更新完成")

    def get_station_status(self):
        """
        获取所有场站线程健康状态。
        
        此方法用于监控各个场站处理线程的运行状态，可用于服务健康检查和故障排查。
        每个场站对应一个独立的处理线程，负责该场站的数据处理和回调执行。
        
        Returns:
            dict: 场站状态字典，格式为 {station_id: status_info}，其中
                station_id (str): 场站ID
                status_info (dict): 包含以下字段
                    - 'running' (bool): 线程是否正在运行
                    - 'future' (concurrent.futures.Future): 线程的Future对象
                    可用于获取线程执行结果和异常信息
                    
        示例:
            >>> status = service.get_station_status()
            >>> for station_id, info in status.items():
            ...     print(f"场站 {station_id}: 运行中={info['running']}")
            
        注意:
            - 如果线程刚刚启动，'running'可能为True但实际处理还未开始
            - 如果线程已结束但未清理，'running'可能为False但future对象仍存在
            - 可结合get_service_status()方法检查整体服务状态
        """
        status = {}
        for sid, fut in self._station_threads.items():
            status[sid] = {"running": fut.running(), "future": fut}
        return status

    def get_service_status(self):
        """
        获取服务主线程及Kafka连接健康状态。
        
        此方法用于监控整个服务的运行状态，包括主线程、Kafka连接和场站数量，
        可用于服务健康检查、监控和故障排查。返回的状态信息可用于构建
        服务监控面板或自动健康检查脚本。
        
        Returns:
            dict: 服务状态字典，包含以下字段
                - main_thread_alive (bool): 主线程是否正在运行
                - consumer_alive (bool): Kafka消费者是否已初始化
                - producer_alive (bool): Kafka生产者是否已初始化
                - station_count (int): 当前活跃场站数量
                
        示例:
            >>> status = service.get_service_status()
            >>> print(f"主线程状态: {status['main_thread_alive']}")
            >>> print(f"场站数量: {status['station_count']}")
            
        注意:
            - consumer_alive和producer_alive仅表示对象是否存在，
              不代表实际连接状态，连接状态需查看日志
            - 如果服务是前台运行的且未启动，main_thread_alive可能为False
            - station_count表示当前有处理线程的场站数量，
              不等于实际有数据的场站数量
            - 建议结合get_station_status()方法获取更详细的场站状态
        """
        return {
            "main_thread_alive": self._thread.is_alive() if self._thread else False,
            "consumer_alive": self.consumer is not None,
            "producer_alive": self.producer is not None,
            "station_count": len(self._station_threads),
        }
