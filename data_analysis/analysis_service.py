import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import time
import threading

from data_analysis import (
    DataAnalysisError,
    KafkaConnectionError,
    DispatcherError,
    handle_error,
)
from data_analysis.kafka_client import KafkaConsumerClient, KafkaProducerClient

from .kafka_client import AsyncKafkaConsumerClient, AsyncKafkaProducerClient
from .config import KAFKA_CONFIG, TOPIC_TO_MODULES
from .dispatcher import DataDispatcher


class AsyncDataAnalysisService:
    """
    异步数据解析服务，支持多场站并发、窗口补全/插值、依赖聚合、自动上传Kafka。
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
    支持：
    - 多场站多线程，自动缓存窗口数据，补零/插值
    - 自动处理模型间依赖
    - 自动上传本模型输出到Kafka供其他模型消费
    - 支持自定义回调处理
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
        callback: 回调函数(station_id, module_input)，每个场站数据更新时调用。
        background: 是否后台线程运行。
        """
        if background:
            self._thread = threading.Thread(
                target=self._main_loop, args=(callback,), daemon=True
            )
            self._thread.start()
        else:
            self._main_loop(callback)

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join()

    def get_outputs(self, station_id):
        """
        获取指定场站所有模块的输入结构
        """
        return self.dispatcher.get_all_outputs(station_id)

    def reload_config(self):
        """
        动态热加载config.py配置（topic、窗口、依赖等），并同步dispatcher。
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
        返回: dict {station_id: {'running': bool, 'future': Future}}
        """
        status = {}
        for sid, fut in self._station_threads.items():
            status[sid] = {"running": fut.running(), "future": fut}
        return status

    def get_service_status(self):
        """
        获取服务主线程及Kafka连接健康状态。
        返回: dict {main_thread_alive, consumer_alive, producer_alive, station_count}
        """
        return {
            "main_thread_alive": self._thread.is_alive() if self._thread else False,
            "consumer_alive": self.consumer is not None,
            "producer_alive": self.producer is not None,
            "station_count": len(self._station_threads),
        }
