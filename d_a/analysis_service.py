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
from .config import (
    KAFKA_CONFIG,
    TOPIC_TO_MODULES,
    MODULE_TO_TOPICS,
    MODULE_OUTPUT_TOPICS,
    OFFSET_COMMIT_CONFIG,
)
from .dispatcher import DataDispatcher
from .service_base import ServiceBase
from .offset_manager import OffsetManager
from .batch_result_aggregator import BatchResultAggregator


class AsyncDataAnalysisService(ServiceBase):
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
        # 调用基类初始化（Topic订阅逻辑）
        super().__init__(
            module_name=module_name,
            topics=topics,
            kafka_config=kafka_config or KAFKA_CONFIG,
            data_expire_seconds=data_expire_seconds,
        )

        self.dispatcher = DataDispatcher(data_expire_seconds, False)
        self.consumer = AsyncKafkaConsumerClient(self.topics, self.kafka_config)

        # Offset管理器
        self.offset_manager = OffsetManager(
            self.consumer, offset_commit_config or OFFSET_COMMIT_CONFIG
        )

        # 批次结果聚合器
        self.batch_aggregator = BatchResultAggregator(
            batch_timeout=5.0, cleanup_interval=60.0  # 5秒超时
        )

        self._station_tasks = {}  # 每个场站的任务
        self._station_stop_flags = {}  # 每个场站的停止标志
        self._station_data_events = {}  # 每个场站的数据就绪事件
        self._station_batch_info = {}  # 场站的批次信息: {station_id: batch_id}
        self._station_initialized = (
            {}
        )  # 场站是否已初始化（首次数据完整）: {station_id: bool}
        self._global_data_cache = {}  # 缓存全局数据: {topic: latest_data}
        self._main_task = None  # 主循环任务
        self._stop_event = asyncio.Event()  # 停止事件
        self._callback = None  # 单场站处理回调
        self._result_handler = result_handler  # 单场站结果处理回调
        self._batch_upload_handler = None  # 批次上传回调

        # Topic处理器映射：每个topic直接对应一个处理方法
        # 优势：直观、易扩展、无需预定义格式字典
        self._topic_handlers = self._build_topic_handlers()

    def _build_topic_handlers(self):
        """
        构建 topic 到处理器的映射

        每个 topic 独立处理方法，直观明确，易于扩展

        Returns:
            dict: {topic: handler_function}
        """
        handlers = {
            "SCHEDULE-STATION-PARAM": self._handle_station_param,
            "SCHEDULE-STATION-REALTIME-DATA": self._handle_station_realtime_data,
            "SCHEDULE-ENVIRONMENT-CALENDAR": self._handle_environment_calendar,
            "SCHEDULE-DEVICE-METER": self._handle_device_meter,
            "SCHEDULE-DEVICE-GUN": self._handle_device_gun,
            "SCHEDULE-CAR-ORDER": self._handle_car_order,
            "SCHEDULE-CAR-PRICE": self._handle_car_price,
            "SCHEDULE-DEVICE-ERROR": self._handle_device_error,
            "SCHEDULE-DEVICE-HOST-DCDC": self._handle_device_host_dcdc,
            "SCHEDULE-DEVICE-HOST-ACDC": self._handle_device_host_acdc,
            "SCHEDULE-DEVICE-STORAGE": self._handle_device_storage,
            "SCHEDULE-ENVIRONMENT-WEATHER": self._handle_environment_weather,
            "SCHEDULE-DEVICE-PV": self._handle_device_pv,
        }

        # 自动添加模型输出topic处理器
        for module_name, topic_name in MODULE_OUTPUT_TOPICS.items():
            if topic_name not in handlers:
                handlers[topic_name] = self._handle_model_output

        return handlers

    # ==================== Topic 处理器：每个 topic 独立方法 ====================

    @staticmethod
    def _handle_station_param(value):
        """
        处理 SCHEDULE-STATION-PARAM
        格式: [{'stationId': '...', ...}, ...]
        """
        if not isinstance(value, list):
            return []

        station_data_list = []
        for item in value:
            if isinstance(item, dict):
                station_id = item.get("stationId")
                if station_id:
                    station_data_list.append((station_id, item))
        return station_data_list

    @staticmethod
    def _handle_station_realtime_data(value):
        """
        处理 SCHEDULE-STATION-REALTIME-DATA
        格式: {'realTimeData': [{'stationId': '...', 'sendTime': '...', ...}, ...]}
        """
        if not isinstance(value, dict):
            return []

        data_list = value.get("realTimeData")
        if not data_list or not isinstance(data_list, list):
            return []

        # 按场站分组
        from collections import defaultdict

        station_groups = defaultdict(list)

        for item in data_list:
            if isinstance(item, dict):
                station_id = item.get("stationId")
                if station_id:
                    station_groups[station_id].append(item)

        # 对每个场站的数据按 sendTime 排序
        station_data_list = []
        for station_id, items in station_groups.items():
            sorted_items = sorted(items, key=lambda x: x.get("sendTime", ""))
            station_data_list.append((station_id, sorted_items))

        return station_data_list

    @staticmethod
    def _handle_environment_calendar(value):
        """
        处理 SCHEDULE-ENVIRONMENT-CALENDAR
        格式: {'calendar': [...]}
        全局数据，无 stationId
        """
        if not isinstance(value, dict):
            return []
        return [("__global__", value)]

    @staticmethod
    def _handle_environment_weather(value):
        """
        处理 SCHEDULE-ENVIRONMENT-WEATHER，天气数据分厂站
        格式: {"stationId":str,"weatherSituationTomorrow":str,"weatherSituationYesterday":str,"seasonTomorrow":str}
        """
        if not isinstance(value, dict):
            return []

        station_id = value.get("stationId")
        if station_id:
            return [(station_id, value)]
        return []

    @staticmethod
    def _handle_device_meter(value):
        """
        处理 SCHEDULE-DEVICE-METER
        格式: {'stationId': '...', ...}
        """
        if not isinstance(value, dict):
            return []

        station_id = value.get("stationId")
        if station_id:
            return [(station_id, value)]
        return []

    @staticmethod
    def _handle_device_gun(value):
        """
        处理 SCHEDULE-DEVICE-GUN
        格式: {'stationId': '...', ...}
        """
        if not isinstance(value, dict):
            return []

        station_id = value.get("stationId")
        if station_id:
            return [(station_id, value)]
        return []

    @staticmethod
    def _handle_car_order(value):
        """
        处理 SCHEDULE-CAR-ORDER
        格式: {'stationId': '...', ...}
        """
        if not isinstance(value, dict):
            return []

        station_id = value.get("stationId")
        if station_id:
            return [(station_id, value)]
        return []

    @staticmethod
    def _handle_car_price(value):
        """
        处理 SCHEDULE-CAR-PRICE
        格式: {'fee': [{'stationId': '...', 'sendTime': '...', ...}, ...]}
        """
        if not isinstance(value, dict):
            return []

        data_list = value.get("fee")
        if not data_list or not isinstance(data_list, list):
            return []

        # 按场站分组
        from collections import defaultdict

        station_groups = defaultdict(list)

        for item in data_list:
            if isinstance(item, dict):
                station_id = item.get("stationId")
                if station_id:
                    station_groups[station_id].append(item)

        # 对每个场站的数据按 sendTime 排序
        station_data_list = []
        for station_id, items in station_groups.items():
            sorted_items = sorted(items, key=lambda x: x.get("sendTime", ""))
            station_data_list.append((station_id, sorted_items))

        return station_data_list

    @staticmethod
    def _handle_device_error(value):
        """
        处理 SCHEDULE-DEVICE-ERROR
        格式: {'stationId': '...', ...}
        """
        if not isinstance(value, dict):
            return []

        station_id = value.get("stationId")
        if station_id:
            return [(station_id, value)]
        return []

    @staticmethod
    def _handle_device_host_dcdc(value):
        """
        处理 SCHEDULE-DEVICE-HOST-DCDC
        格式: {'stationId': '...', ...}
        """
        if not isinstance(value, dict):
            return []

        station_id = value.get("stationId")
        if station_id:
            return [(station_id, value)]
        return []

    @staticmethod
    def _handle_device_host_acdc(value):
        """
        处理 SCHEDULE-DEVICE-HOST-ACDC
        格式: {'stationId': '...', ...}
        """
        if not isinstance(value, dict):
            return []

        station_id = value.get("stationId")
        if station_id:
            return [(station_id, value)]
        return []

    @staticmethod
    def _handle_device_storage(value):
        """
        处理 SCHEDULE-DEVICE-STORAGE
        格式: {'stationId': '...', ...}
        """
        if not isinstance(value, dict):
            return []

        station_id = value.get("stationId")
        if station_id:
            return [(station_id, value)]
        return []

    @staticmethod
    def _handle_device_pv(value):
        """
        处理 SCHEDULE-DEVICE-PV
        格式: {'stationId': '...', ...}
        """
        if not isinstance(value, dict):
            return []

        station_id = value.get("stationId")
        if station_id:
            return [(station_id, value)]
        return []

    @staticmethod
    def _handle_model_output(value):
        """
        处理模型输出 topic (MODULE-OUTPUT-*)
        格式: {'results': [{'station_id': '...', ...}, ...]}
        """
        if not isinstance(value, dict):
            return []

        station_data_list = []
        results = value.get("results", [])
        if isinstance(results, list):
            for item in results:
                if isinstance(item, dict):
                    station_id = item.get("station_id")
                    if station_id:
                        station_data_list.append((station_id, item))
        return station_data_list

    async def _maybe_await(self, func, *args):
        if func is None:
            return None
        result = func(*args)
        if asyncio.iscoroutine(result):
            return await result
        return result

    async def _process_message(self, msg, batch_id=None):
        """
        处理单条消息，并触发数据就绪事件。

        Args:
            msg: Kafka消息对象
            batch_id: 批次ID（可选，由_main_loop传入）

        Returns:
            tuple: (是否成功, 场站ID列表)
        """
        topic = msg.topic
        kafka_timestamp = msg.timestamp / 1000.0

        try:
            value_str = msg.value.decode("utf-8")
            value = json.loads(value_str)

            # 根据 topic 直接获取对应的处理器
            handler = self._topic_handlers.get(topic)
            if not handler:
                logging.warning(f"未配置处理器的 topic: {topic}，尝试自动识别格式")
                station_data_list = self._auto_detect_format(value)
            else:
                # 直接调用对应的处理器
                station_data_list = handler(value)

            return await self._process_message_with_parsed_data(
                msg, value, station_data_list, batch_id
            )

        except Exception as exc:
            handle_error(
                exc,
                context=f"处理消息 topic={topic}, partition={msg.partition}, offset={msg.offset}",
            )
            return False, []

    @staticmethod
    def _auto_detect_format(value):
        """
        自动识别未配置处理器的topic格式（后备方案）

        Args:
            value: JSON数据

        Returns:
            list: [(station_id, raw_data), ...]
        """
        if isinstance(value, list):
            # 尝试作为列表格式处理（SCHEDULE-STATION-PARAM）
            return AsyncDataAnalysisService._handle_station_param(value)
        elif isinstance(value, dict):
            # 检查是否是单场站数据
            if "stationId" in value:
                return AsyncDataAnalysisService._handle_car_order(value)

            # 检查是否是模型输出
            if "results" in value:
                return AsyncDataAnalysisService._handle_model_output(value)

            # 可能是窗口格式，尝试常见的key
            if "realTimeData" in value and isinstance(value["realTimeData"], list):
                logging.info("自动识别为实时数据窗口格式")
                return AsyncDataAnalysisService._handle_station_realtime_data(value)

            if "fee" in value and isinstance(value["fee"], list):
                logging.info("自动识别为价格数据窗口格式")
                return AsyncDataAnalysisService._handle_car_price(value)

            # 可能是全局数据
            logging.info("自动识别为全局数据格式")
            return AsyncDataAnalysisService._handle_environment_calendar(value)

        logging.warning(f"无法识别的消息格式: {type(value)}")
        return []

    async def _process_message_with_parsed_data(
        self, msg, value, station_data_list, batch_id=None
    ):
        """
        使用预解析的数据处理单个消息（避免重复解析JSON）

        Args:
            msg: Kafka消息对象
            value: 已解析的JSON值
            station_data_list: 已提取的场站数据列表
            batch_id: 批次ID（可选）

        Returns:
            tuple: (是否成功, 场站ID列表)
        """
        topic = msg.topic
        kafka_timestamp = msg.timestamp / 1000.0

        try:
            if not station_data_list:
                logging.debug(f"消息中没有提取到场站数据: topic={topic}")
                return False, []

            # 提取场站列表（排除全局数据）
            station_ids = [sid for sid, _ in station_data_list if sid != "__global__"]

            # 处理每个场站的数据
            all_success = True
            for station_id, station_data in station_data_list:
                try:
                    # 处理全局数据
                    if station_id == "__global__":
                        # 缓存全局数据（最新的）
                        self._global_data_cache[topic] = station_data
                        logging.info(f"全局数据已缓存 topic={topic}，等待场站注册")
                        continue

                    # 处理场站数据
                    # 将原始数据交给dispatcher（使用Kafka原生时间戳）
                    should_trigger = self.dispatcher.update_topic_data(
                        station_id, topic, station_data, kafka_timestamp
                    )

                    # 记录场站的批次信息
                    self._station_batch_info[station_id] = batch_id

                    # 创建场站任务（如果不存在）
                    if station_id not in self._station_tasks:
                        # 1. 先应用全局缓存数据，再创建场站任务
                        # 避免竞态条件：确保场站 worker 启动时已有完整的全局数据
                        if self._global_data_cache:
                            logging.info(
                                f"新场站 {station_id} 注册，应用 {len(self._global_data_cache)} 个全局数据"
                            )
                            for (
                                global_topic,
                                global_data,
                            ) in self._global_data_cache.items():
                                self.dispatcher.update_topic_data(
                                    station_id,
                                    global_topic,
                                    global_data,
                                    kafka_timestamp,
                                )

                        # 2. 创建场站任务（此时全局数据已就绪）
                        self._create_station_task(station_id)
                        # 3. 标记场站为未初始化（需要等待关键数据完整）
                        self._station_initialized[station_id] = False

                    # 根据 dispatcher 返回值决定是否触发数据就绪事件
                    # 订单类 topic：只有进入新的一秒时才触发（上一秒订单已聚合完成）
                    # 其他 topic：每次更新都触发
                    if should_trigger and station_id in self._station_data_events:
                        # 数据完整性检查：场站初始化后直接触发，初始化前需检查关键数据是否完整
                        if self._station_initialized.get(station_id, False):
                            # 已初始化，直接触发
                            self._station_data_events[station_id].set()
                        else:
                            # 未初始化，检查是否有足够的数据
                            if self._check_station_data_ready(station_id):
                                # 关键数据已完整，标记为已初始化并触发
                                self._station_initialized[station_id] = True
                                self._station_data_events[station_id].set()
                                logging.info(
                                    f"场站 {station_id} 首次数据完整，开始处理"
                                )
                            else:
                                # 数据不完整，不触发（等待更多数据）
                                logging.debug(
                                    f"场站 {station_id} 数据尚未完整，等待更多topic数据"
                                )

                except Exception as exc:
                    handle_error(
                        DispatcherError(exc),
                        context=f"update_topic_data station_id={station_id}, topic={topic}",
                    )
                    all_success = False

            # 使用 OffsetManager 跟踪消息
            if all_success and len(station_data_list) > 0:
                self.offset_manager.track_message(msg)
                return True, station_ids

            return False, station_ids

        except Exception as exc:
            handle_error(
                exc,
                context=f"处理消息 topic={topic}, partition={msg.partition}, offset={msg.offset}",
            )
            return False, []

    def _check_station_data_ready(self, station_id):
        """
        检查场站的关键数据是否完整（用于首次触发判断）

        策略：
        1. 如果指定了 module_name，检查该模块需要的所有 topic 是否都有数据
        2. 如果没有指定 module_name，检查是否至少有 50% 的 topic 有数据
        3. 可以根据业务需求调整阈值

        Args:
            station_id: 场站ID

        Returns:
            bool: True 表示数据完整可以开始处理，False 表示还需等待
        """
        if station_id not in self.dispatcher.data_cache:
            return False

        if self.module_name:
            # 指定了模块，检查该模块需要的所有 topic
            required_topics = MODULE_TO_TOPICS.get(self.module_name, [])
            if not required_topics:
                return True  # 没有依赖 topic，直接返回 True

            # 检查每个必需的 topic 是否有数据
            available_count = 0
            for topic in required_topics:
                window = self.dispatcher.get_topic_window(station_id, topic)
                if window:
                    available_count += 1
                # 提前返回，避免不必要的计算
                else:
                    break

            # 要求所有必需的 topic 都有数据（100% 完整）
            data_ready = available_count == len(required_topics)

            if not data_ready:
                logging.debug(
                    f"场站 {station_id} 数据未完整: {available_count}/{len(required_topics)} 个topic有数据"
                )

            return data_ready
        else:
            # 没有指定模块，使用宽松策略：至少 50% 的已订阅 topic 有数据
            station_topics = self.dispatcher.data_cache[station_id]
            total_topics = len(self.topics)
            available_count = sum(
                1 for topic in self.topics if station_topics.get(topic)
            )

            # 至少 50% 的 topic 有数据才触发
            threshold = max(1, total_topics // 2)
            return available_count >= threshold

    def _create_station_task(self, station_id):
        """
        创建场站任务，如果已存在则跳过
        Args:
            station_id: 场站ID
        Returns:
            None
        """
        stop_flag = asyncio.Event()
        data_event = asyncio.Event()
        self._station_stop_flags[station_id] = stop_flag
        self._station_data_events[station_id] = data_event
        task = asyncio.create_task(
            self._station_worker(
                station_id,
                self._callback,
                self._result_handler,
                stop_flag,
                data_event,
            )
        )
        self._station_tasks[station_id] = task

    async def _station_worker(
        self, station_id, callback, result_handler, stop_flag, data_event
    ):
        """
        场站Worker - 事件驱动模式，当有新数据到达时立即处理，否则等待数据事件或超时触发。

        超时机制：
        - 等待数据事件最多2秒
        - 超时后也会触发一次处理（确保订单等聚合数据不会永久等待）
        - 业务模块通过 _data_quality 判断数据是否完整

        Args:
            station_id: 场站ID
            callback: 业务回调函数
            result_handler: 结果处理函数
            stop_flag: 停止标志
            data_event: 数据就绪事件
        Returns:
            None
        """
        while not stop_flag.is_set():
            try:
                # 等待数据就绪事件（有新数据到达）或超时
                # 超时设置为2秒：确保即使没有新订单，上一秒的订单也会被处理
                data_arrived = False
                try:
                    await asyncio.wait_for(data_event.wait(), timeout=2.0)
                    data_arrived = True
                except asyncio.TimeoutError:
                    # 超时：可能有未处理的聚合数据（如订单）
                    # 继续执行，让业务模块决定是否处理
                    pass

                # 清除事件，准备下次触发
                data_event.clear()

                # 获取场站的批次ID
                batch_id = self._station_batch_info.get(station_id)

                # 获取解析后的输入数据（只获取指定模块的输入）
                if self.module_name:
                    module_input = self.dispatcher.get_module_input(
                        station_id, self.module_name
                    )
                else:
                    # 如果未指定模块名，则获取所有模块输入
                    module_input = self.dispatcher.get_all_inputs(station_id)

                # 如果没有数据可用，跳过处理
                if not module_input:
                    logging.debug(f"场站 {station_id} 没有可用数据，跳过处理")
                    continue

                # 检查是否有可用的topic数据
                data_quality = None
                if isinstance(module_input, dict):
                    data_quality = module_input.get("_data_quality")
                if data_quality and not data_quality.get("available_topics"):
                    logging.debug(f"场站 {station_id} 没有可用的topic数据")
                    continue

                logging.info(f"场站 {station_id} 开始处理，batch_id={batch_id}")

                result = None
                if callback:
                    try:
                        # 调用业务回调（传给业务模块）
                        result = await self._maybe_await(
                            callback, station_id, module_input
                        )
                        logging.info(
                            f"场站 {station_id} 处理完成，result={'有' if result else '无'}"
                        )
                    except Exception as exc:  # noqa: BLE001
                        handle_error(exc, context=f"回调处理 station_id={station_id}")
                        result = None  # 确保失败时result为None

                # 提交结果到批次聚合器（即使result是None也要提交）
                if batch_id and self._batch_upload_handler:
                    batch_collector = self.batch_aggregator._batches.get(batch_id)
                    if batch_collector:
                        await batch_collector.add_result(station_id, result)
                        logging.info(f"场站 {station_id} 结果已提交到批次 {batch_id}")
                    else:
                        logging.warning(
                            f"场站 {station_id} 找不到批次 {batch_id}，可用批次: {list(self.batch_aggregator._batches.keys())}"
                        )
                else:
                    if not batch_id:
                        logging.warning(f"场站 {station_id} 没有batch_id")
                    if not self._batch_upload_handler:
                        logging.warning(
                            f"场站 {station_id} 没有配置batch_upload_handler"
                        )

                # 保持原有的result_handler逻辑（单场站处理）
                if result_handler:
                    try:
                        await self._maybe_await(
                            result_handler, station_id, module_input, result
                        )
                    except Exception as exc:  # noqa: BLE001
                        handle_error(exc, context=f"结果处理 station_id={station_id}")
            except Exception as exc:  # noqa: BLE001
                handle_error(exc, context=f"场站任务 station_id={station_id}")
                await asyncio.sleep(1)  # 错误后短暂延迟

    async def _main_loop(self):
        """主循环，负责消费Kafka消息并分发处理，管理批次，提交offset等"""
        try:
            await self.consumer.start()  # 启动消费者
        except Exception as exc:  # noqa: BLE001
            handle_error(KafkaConnectionError(exc), context="KafkaConsumer连接")
            await asyncio.sleep(5)  # 连接失败后等待5秒再重试
            return
        try:
            while not self._stop_event.is_set():  # 主循环，直到收到停止信号
                try:
                    batch = await self.consumer.getmany(timeout_ms=1000)  # 拉取一批消息
                except Exception as exc:  # noqa: BLE001
                    handle_error(
                        exc, context="Kafka消费", recover=lambda: asyncio.sleep(2)
                    )
                    await asyncio.sleep(2)
                    continue

                if not batch:
                    # 即使没有消息，也检查是否需要定时提交
                    if self.offset_manager.should_commit():
                        await self.offset_manager.commit()
                    await asyncio.sleep(0.2)
                    continue

                # 为这一批消息生成批次ID
                batch_id = f"batch_{int(time.time() * 1000)}_{id(batch)}"

                # 第一遍：解析所有消息并缓存结果（避免重复解析JSON）
                parsed_messages = []  # [(msg, value, station_data_list), ...]
                all_station_ids = []

                for msg in batch:
                    try:
                        value_str = msg.value.decode("utf-8")
                        value = json.loads(value_str)

                        # 使用topic处理器映射
                        handler = self._topic_handlers.get(msg.topic)
                        if not handler:
                            logging.warning(
                                f"未配置处理器的 topic: {msg.topic}，尝试自动识别格式"
                            )
                            station_data_list = self._auto_detect_format(value)
                        else:
                            station_data_list = handler(value)

                        # 缓存解析结果
                        parsed_messages.append((msg, value, station_data_list))

                        if station_data_list:
                            # 提取场站列表（排除全局数据）
                            station_ids = [
                                sid
                                for sid, _ in station_data_list
                                if sid != "__global__"
                            ]
                            all_station_ids.extend(station_ids)
                    except Exception as exc:
                        logging.error(f"解析消息失败: {exc}")
                        # 即使解析失败，也要保留消息以便后续错误处理
                        parsed_messages.append((msg, None, None))

                # 1. 先创建批次（如果有场站数据且配置了上传回调）
                if all_station_ids and self._batch_upload_handler:
                    # 去重场站ID
                    unique_stations = list(set(all_station_ids))
                    await self.batch_aggregator.get_or_create_batch(
                        batch_id=batch_id,
                        expected_stations=unique_stations,
                        upload_callback=self._batch_upload_handler,
                    )
                    logging.info(
                        f"创建批次 {batch_id}，包含 {len(unique_stations)} 个场站"
                    )

                # 2. 再处理所有消息（使用缓存的解析结果）
                for msg, value, station_data_list in parsed_messages:
                    if value is None or station_data_list is None:
                        # 跳过解析失败的消息
                        continue

                    # 使用预解析的数据进行处理
                    await self._process_message_with_parsed_data(
                        msg, value, station_data_list, batch_id
                    )

                # 检查是否需要提交offset
                if self.offset_manager.should_commit():
                    await self.offset_manager.commit()

                # 清理过期数据
                try:
                    self.dispatcher.clean_expired()
                except Exception as exc:  # noqa: BLE001
                    handle_error(DispatcherError(exc), context="clean_expired")
        finally:
            # 停止前提交所有待处理的offset
            logging.info("服务停止,提交剩余offset...")
            await self.offset_manager.commit()

            await self.consumer.stop()
            for flag in self._station_stop_flags.values():
                flag.set()
            if self._station_tasks:
                await asyncio.gather(
                    *self._station_tasks.values(), return_exceptions=True
                )

    async def start(
        self, callback=None, result_handler=None, batch_upload_handler=None
    ):
        """
        启动服务

        Args:
            callback: 单场站处理回调 (station_id, module_input) -> result
            result_handler: 单场站结果处理回调 (station_id, module_input, result) -> None
            batch_upload_handler: 批次上传回调 (batch_id, results_list) -> None
                - results_list: 所有场站输出结果组成的列表 [result1, result2, ...]
                - 每个result是场站的输出字典
        """
        self._callback = callback or self._callback
        if result_handler is not None:
            self._result_handler = result_handler
        if batch_upload_handler is not None:
            self._batch_upload_handler = batch_upload_handler
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
        data_event = asyncio.Event()  # 新增数据事件
        self._station_stop_flags[station_id] = stop_flag
        self._station_data_events[station_id] = data_event
        task = asyncio.create_task(
            self._station_worker(
                station_id,
                callback or self._callback,
                result_handler if result_handler is not None else self._result_handler,
                stop_flag,
                data_event,  # 传递数据事件
            )
        )
        self._station_tasks[station_id] = task

    async def remove_station(self, station_id):
        """
        动态移除场站异步任务
        """
        if station_id in self._station_stop_flags:
            self._station_stop_flags[station_id].set()
        if station_id in self._station_data_events:
            # 触发事件以便worker能够退出
            self._station_data_events[station_id].set()
            del self._station_data_events[station_id]
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


class DataAnalysisService(ServiceBase):
    """同步数据解析服务，负责从Kafka消费数据并驱动业务回调。"""

    def __init__(
        self,
        module_name=None,
        topics=None,
        kafka_config=None,
        data_expire_seconds=600,
        result_handler=None,
    ):
        # 使用基类初始化
        super().__init__(module_name, topics, kafka_config, data_expire_seconds)

        # DataAnalysisService 特有的同步消费者
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
        self._station_data_events = {}  # 新增：每个场站的数据就绪事件
        self._global_data_cache = {}  # 缓存全局数据: {topic: latest_data}

        # Topic处理器映射（与AsyncDataAnalysisService相同）
        self._topic_handlers = self._build_topic_handlers()

    def _build_topic_handlers(self):
        """构建 topic 处理器映射 - 每个 topic 独立方法"""
        handlers = {
            "SCHEDULE-STATION-PARAM": self._handle_station_param,
            "SCHEDULE-STATION-REALTIME-DATA": self._handle_station_realtime_data,
            "SCHEDULE-ENVIRONMENT-CALENDAR": self._handle_environment_calendar,
            "SCHEDULE-DEVICE-METER": self._handle_device_meter,
            "SCHEDULE-DEVICE-GUN": self._handle_device_gun,
            "SCHEDULE-CAR-ORDER": self._handle_car_order,
            "SCHEDULE-CAR-PRICE": self._handle_car_price,
            "SCHEDULE-DEVICE-ERROR": self._handle_device_error,
            "SCHEDULE-DEVICE-HOST-DCDC": self._handle_device_host_dcdc,
            "SCHEDULE-DEVICE-HOST-ACDC": self._handle_device_host_acdc,
            "SCHEDULE-DEVICE-STORAGE": self._handle_device_storage,
        }
        for module_name, topic_name in MODULE_OUTPUT_TOPICS.items():
            if topic_name not in handlers:
                handlers[topic_name] = self._handle_model_output
        return handlers

    # ==================== Topic 处理器：每个 topic 独立方法 ====================
    # （与 AsyncDataAnalysisService 相同的处理器）

    @staticmethod
    def _handle_station_param(value):
        """处理 SCHEDULE-STATION-PARAM"""
        if not isinstance(value, list):
            return []
        station_data_list = []
        for item in value:
            if isinstance(item, dict):
                station_id = item.get("stationId")
                if station_id:
                    station_data_list.append((station_id, item))
        return station_data_list

    @staticmethod
    def _handle_station_realtime_data(value):
        """处理 SCHEDULE-STATION-REALTIME-DATA"""
        if not isinstance(value, dict):
            return []
        data_list = value.get("realTimeData")
        if not data_list or not isinstance(data_list, list):
            return []
        from collections import defaultdict

        station_groups = defaultdict(list)
        for item in data_list:
            if isinstance(item, dict):
                station_id = item.get("stationId")
                if station_id:
                    station_groups[station_id].append(item)
        station_data_list = []
        for station_id, items in station_groups.items():
            sorted_items = sorted(items, key=lambda x: x.get("sendTime", ""))
            station_data_list.append((station_id, sorted_items))
        return station_data_list

    @staticmethod
    def _handle_environment_calendar(value):
        """处理 SCHEDULE-ENVIRONMENT-CALENDAR"""
        if not isinstance(value, dict):
            return []
        return [("__global__", value)]

    @staticmethod
    def _handle_device_meter(value):
        """处理 SCHEDULE-DEVICE-METER"""
        if not isinstance(value, dict):
            return []
        station_id = value.get("stationId")
        if station_id:
            return [(station_id, value)]
        return []

    @staticmethod
    def _handle_device_gun(value):
        """处理 SCHEDULE-DEVICE-GUN"""
        if not isinstance(value, dict):
            return []
        station_id = value.get("stationId")
        if station_id:
            return [(station_id, value)]
        return []

    @staticmethod
    def _handle_car_order(value):
        """处理 SCHEDULE-CAR-ORDER"""
        if not isinstance(value, dict):
            return []
        station_id = value.get("stationId")
        if station_id:
            return [(station_id, value)]
        return []

    @staticmethod
    def _handle_car_price(value):
        """处理 SCHEDULE-CAR-PRICE"""
        if not isinstance(value, dict):
            return []
        data_list = value.get("fee")
        if not data_list or not isinstance(data_list, list):
            return []
        from collections import defaultdict

        station_groups = defaultdict(list)
        for item in data_list:
            if isinstance(item, dict):
                station_id = item.get("stationId")
                if station_id:
                    station_groups[station_id].append(item)
        station_data_list = []
        for station_id, items in station_groups.items():
            sorted_items = sorted(items, key=lambda x: x.get("sendTime", ""))
            station_data_list.append((station_id, sorted_items))
        return station_data_list

    @staticmethod
    def _handle_device_error(value):
        """处理 SCHEDULE-DEVICE-ERROR"""
        if not isinstance(value, dict):
            return []
        station_id = value.get("stationId")
        if station_id:
            return [(station_id, value)]
        return []

    @staticmethod
    def _handle_device_host_dcdc(value):
        """处理 SCHEDULE-DEVICE-HOST-DCDC"""
        if not isinstance(value, dict):
            return []
        station_id = value.get("stationId")
        if station_id:
            return [(station_id, value)]
        return []

    @staticmethod
    def _handle_device_host_acdc(value):
        """处理 SCHEDULE-DEVICE-HOST-ACDC"""
        if not isinstance(value, dict):
            return []
        station_id = value.get("stationId")
        if station_id:
            return [(station_id, value)]
        return []

    @staticmethod
    def _handle_device_storage(value):
        """处理 SCHEDULE-DEVICE-STORAGE"""
        if not isinstance(value, dict):
            return []
        station_id = value.get("stationId")
        if station_id:
            return [(station_id, value)]
        return []

    @staticmethod
    def _handle_model_output(value):
        """处理模型输出 topic"""
        if not isinstance(value, dict):
            return []
        station_data_list = []
        results = value.get("results", [])
        if isinstance(results, list):
            for item in results:
                if isinstance(item, dict):
                    station_id = item.get("station_id")
                    if station_id:
                        station_data_list.append((station_id, item))
        return station_data_list

    def _station_worker(
        self, station_id, callback, result_handler, stop_event, data_event
    ):
        """
        场站工作线程（同步版本）

        等待数据就绪事件，然后处理数据。

        超时机制：
        - 等待数据事件最多2秒
        - 超时后也会触发一次处理（确保订单等聚合数据不会永久等待）
        - 业务模块通过 _data_quality 判断数据是否完整
        """
        while not stop_event.is_set():
            try:
                # 等待数据就绪事件（最多等待2秒）
                # 超时设置为2秒：确保即使没有新订单，上一秒的订单也会被处理
                data_arrived = data_event.wait(timeout=2.0)

                # 清除事件标志，为下一次数据准备
                data_event.clear()

                # 获取解析后的输入数据（只获取指定模块的输入）
                if self.module_name:
                    module_input = self.dispatcher.get_module_input(
                        station_id, self.module_name
                    )
                else:
                    # 如果未指定模块名，则获取所有模块输入
                    module_input = self.dispatcher.get_all_inputs(station_id)

                # 如果没有数据可用，跳过处理
                if not module_input:
                    continue

                # 检查是否有可用的topic数据
                data_quality = None
                if isinstance(module_input, dict):
                    data_quality = module_input.get("_data_quality")
                if data_quality and not data_quality.get("available_topics"):
                    continue

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
                        # 获取Kafka消息的原生时间戳（毫秒转秒）
                        kafka_timestamp = msg.timestamp / 1000.0

                        try:
                            # 使用topic处理器映射提取圼站数据
                            handler = self._topic_handlers.get(topic)
                            if not handler:
                                logging.warning(f"未配置处理器的 topic: {topic}")
                                station_data_list = []
                            else:
                                station_data_list = handler(value)

                            if not station_data_list:
                                logging.debug(
                                    f"消息中没有提取到场站数据: topic={topic}"
                                )
                                continue

                            # 处理每个场站的数据
                            for station_id, station_data in station_data_list:
                                try:
                                    # 处理全局数据
                                    if station_id == "__global__":
                                        self._global_data_cache[topic] = station_data
                                        logging.info(
                                            f"全局数据已缓存 topic={topic}，等待场站注册"
                                        )
                                        continue

                                    # 处理场站数据
                                    # 将原始数据交给dispatcher（使用Kafka原生时间戳）
                                    should_trigger = self.dispatcher.update_topic_data(
                                        station_id, topic, station_data, kafka_timestamp
                                    )

                                    # 创建或管理场站线程
                                    if station_id not in self._station_threads:
                                        # ⚠️ 重要：先应用全局缓存数据，再创建场站线程
                                        # 避免竞态条件：确保场站 worker 启动时已有完整的全局数据
                                        if self._global_data_cache:
                                            logging.info(
                                                f"新场站 {station_id} 注册，应用 {len(self._global_data_cache)} 个全局数据"
                                            )
                                            for (
                                                global_topic,
                                                global_data,
                                            ) in self._global_data_cache.items():
                                                self.dispatcher.update_topic_data(
                                                    station_id,
                                                    global_topic,
                                                    global_data,
                                                    kafka_timestamp,
                                                )

                                        # 创建场站线程（此时全局数据已就绪）
                                        stop_event = threading.Event()
                                        data_event = threading.Event()
                                        self._station_stop_events[station_id] = (
                                            stop_event
                                        )
                                        self._station_data_events[station_id] = (
                                            data_event
                                        )
                                        future = self._executor.submit(
                                            self._station_worker,
                                            station_id,
                                            self._callback,
                                            self._result_handler,
                                            stop_event,
                                            data_event,
                                        )
                                        self._station_threads[station_id] = future

                                    # 根据 dispatcher 返回值决定是否触发数据就绪事件
                                    # 订单类 topic：只有进入新的一秒时才触发（上一秒订单已聚合完成）
                                    # 其他 topic：每次更新都触发
                                    if (
                                        should_trigger
                                        and station_id in self._station_data_events
                                    ):
                                        self._station_data_events[station_id].set()
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

        # 触发所有场站的数据事件，让worker退出
        for data_event in self._station_data_events.values():
            data_event.set()

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
