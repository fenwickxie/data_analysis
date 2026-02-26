#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2026-02-25
project: data_analysis
filename: mock_data.py
version: 1.0
description: 虚拟数据注入模块 - 用于测试时补全缺失的 topic 数据

使用方式:
    from d_a.mock import MockDataManager, StaticMockProvider, RandomMockProvider
    from d_a.dispatcher import DataDispatcher

    dispatcher = DataDispatcher()
    mock_mgr = MockDataManager(dispatcher)

    # 注册静态虚拟数据（全局生效，所有场站）
    mock_mgr.register(
        topic="SCHEDULE-DEVICE-STORAGE",
        provider=StaticMockProvider({
            "storageCode": "MOCK_STORAGE_001",
            "batteryGroupSoc": 80,
            "stationId": "{station_id}",
        })
    )

    # 仅针对特定场站
    mock_mgr.register(
        topic="SCHEDULE-CAR-ORDER",
        station_id="station_001",
        provider=StaticMockProvider({...})
    )

    # 注入所有已注册的虚拟数据（仅补全缺失）
    mock_mgr.inject_all("station_001")

    # 强制覆盖所有数据
    mock_mgr.inject_all("station_001", force=True)
"""

import copy
import logging
import random
import time
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ==================== 虚拟数据提供者基类 ====================


class MockProvider(ABC):
    """
    虚拟数据提供者抽象基类。

    子类需实现 get_data 方法，返回与真实 topic 原始消息相同结构的数据。

    注意：
        - 对于设备聚合类 topic（SCHEDULE-DEVICE-STORAGE、SCHEDULE-DEVICE-HOST-DCDC、
          SCHEDULE-DEVICE-HOST-ACDC），每次 get_data 应返回**单个设备**的数据 dict，
          dispatcher 会自动按 storageCode/hostCode 聚合；若需模拟多设备，
          请使用 MultiDeviceMockProvider。
        - 支持 {station_id} 占位符，会在注入时自动替换为实际场站 ID。
    """

    @abstractmethod
    def get_data(self, station_id: str, topic: str) -> Any:
        """
        生成虚拟数据。

        Args:
            station_id: 场站 ID（调用时自动传入）
            topic: topic 名称

        Returns:
            与真实原始消息相同结构的数据（dict / list）
        """
        ...

    def _render(self, obj: Any, station_id: str) -> Any:
        """递归替换 {station_id} 占位符"""
        if isinstance(obj, str):
            return obj.replace("{station_id}", station_id)
        if isinstance(obj, dict):
            return {k: self._render(v, station_id) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._render(item, station_id) for item in obj]
        return obj


class StaticMockProvider(MockProvider):
    """
    静态虚拟数据提供者 —— 每次返回固定数据（深拷贝，互不影响）。

    支持模板变量替换: {station_id}

    Example::

        provider = StaticMockProvider({
            "stationId": "{station_id}",
            "batteryGroupSoc": 80,
            "storageCode": "MOCK_001",
        })
    """

    def __init__(self, data: Any):
        """
        Args:
            data: 静态数据模板，支持 {station_id} 占位符
        """
        self._data = data

    def get_data(self, station_id: str, topic: str) -> Any:
        rendered = self._render(copy.deepcopy(self._data), station_id)
        return rendered


class RandomMockProvider(MockProvider):
    """
    随机虚拟数据提供者 —— 每次在指定范围内生成随机数值。

    Example::

        provider = RandomMockProvider(
            template={"stationId": "{station_id}", "activePower": None, "soc": None},
            ranges={"activePower": (-200.0, 200.0), "soc": (0, 100)},
            int_fields=["soc"],
        )
    """

    def __init__(
        self,
        template: Dict[str, Any],
        ranges: Dict[str, Tuple[float, float]],
        int_fields: Optional[List[str]] = None,
        precision: int = 2,
    ):
        """
        Args:
            template: 数据模板，值为 None 的字段会按 ranges 随机生成
            ranges: {字段名: (min_val, max_val)}
            int_fields: 需要取整的字段名列表
            precision: 浮点数保留小数位数，默认 2
        """
        self._template = template
        self._ranges = ranges
        self._int_fields = set(int_fields or [])
        self._precision = precision

    def get_data(self, station_id: str, topic: str) -> Any:
        result: Dict[str, Any] = {}
        for key, val in self._template.items():
            if isinstance(val, str):
                result[key] = val.replace("{station_id}", station_id)
            elif val is None and key in self._ranges:
                lo, hi = self._ranges[key]
                raw = random.uniform(lo, hi)
                result[key] = int(raw) if key in self._int_fields else round(raw, self._precision)
            else:
                result[key] = val
        return result


class CallableMockProvider(MockProvider):
    """
    函数式虚拟数据提供者 —— 通过自定义函数生成数据（最灵活）。

    Example::

        import random
        provider = CallableMockProvider(
            lambda station_id, topic: {
                "stationId": station_id,
                "soc": random.randint(20, 100),
            }
        )
    """

    def __init__(self, func: Callable[[str, str], Any]):
        """
        Args:
            func: callable(station_id: str, topic: str) -> Any
        """
        self._func = func

    def get_data(self, station_id: str, topic: str) -> Any:
        return self._func(station_id, topic)


class SequenceMockProvider(MockProvider):
    """
    序列虚拟数据提供者 —— 依次返回预定义数据列表，适合模拟时序变化。

    Example::

        provider = SequenceMockProvider([
            {"soc": 20, "stationId": "{station_id}"},
            {"soc": 50, "stationId": "{station_id}"},
            {"soc": 80, "stationId": "{station_id}"},
        ])
        # 第1次 get_data -> soc=20
        # 第2次 get_data -> soc=50
        # 第3次 get_data -> soc=80
        # 第4次 get_data -> soc=20 (循环)
    """

    def __init__(self, sequence: List[Any], loop: bool = True):
        """
        Args:
            sequence: 数据序列
            loop: True=循环；False=到末尾后一直返回最后一条
        """
        self._sequence = sequence
        self._loop = loop
        self._index = 0

    def get_data(self, station_id: str, topic: str) -> Any:
        if not self._sequence:
            return {}
        item = copy.deepcopy(self._sequence[self._index])
        rendered = self._render(item, station_id)
        # 前进索引
        if self._loop:
            self._index = (self._index + 1) % len(self._sequence)
        else:
            self._index = min(self._index + 1, len(self._sequence) - 1)
        return rendered

    def reset(self):
        """重置序列到起点"""
        self._index = 0


class MultiDeviceMockProvider(MockProvider):
    """
    多设备虚拟数据提供者 —— 专为设备聚合类 topic 设计。

    设备聚合类 topic（SCHEDULE-DEVICE-STORAGE / SCHEDULE-DEVICE-HOST-DCDC /
    SCHEDULE-DEVICE-HOST-ACDC）的 dispatcher 更新器每次处理的是单设备消息。
    此 Provider 内部持有多个设备的 Provider，inject 时会逐一注入每个设备数据。

    本 Provider 的 get_data 返回所有设备数据的列表；MockDataManager 会在
    _is_multi_device_topic 检测到时逐条调用 dispatcher.update_topic_data。

    Example::

        provider = MultiDeviceMockProvider([
            StaticMockProvider({"storageCode": "STORAGE_A", "batteryGroupSoc": 80, "stationId": "{station_id}"}),
            StaticMockProvider({"storageCode": "STORAGE_B", "batteryGroupSoc": 60, "stationId": "{station_id}"}),
        ])
    """

    def __init__(self, device_providers: List[MockProvider]):
        """
        Args:
            device_providers: 每个元素对应一个设备的 MockProvider
        """
        self._device_providers = device_providers

    def get_data(self, station_id: str, topic: str) -> List[Any]:
        """返回所有设备数据组成的列表"""
        return [p.get_data(station_id, topic) for p in self._device_providers]


# ==================== 虚拟数据管理器 ====================


# 设备聚合类 topic（dispatcher 按设备 ID 聚合）
_DEVICE_AGGREGATION_TOPICS = {
    "SCHEDULE-DEVICE-STORAGE",
    "SCHEDULE-DEVICE-HOST-DCDC",
    "SCHEDULE-DEVICE-HOST-ACDC",
}


class MockDataManager:
    """
    虚拟数据管理器。

    职责：
    - 维护 (station_id, topic) -> MockProvider 注册表
    - 精确匹配优先于全局匹配（station_id=None）
    - 通过调用 dispatcher.update_topic_data 注入数据，不修改 dispatcher 内部逻辑
    - 默认只补全缺失数据（force=False），force=True 时强制覆盖

    设计约束：
    - 仅用于测试环境，勿在生产中使用
    - 注入的数据格式必须与真实原始消息一致，以便 dispatcher 的 updater 正常解析

    Attributes:
        _dispatcher: DataDispatcher 实例
        _registry: {(station_id_or_None, topic): MockProvider}
        _enabled: 是否启用虚拟数据注入
    """

    def __init__(self, dispatcher):
        """
        Args:
            dispatcher: DataDispatcher 实例
        """
        self._dispatcher = dispatcher
        self._registry: Dict[Tuple[Optional[str], str], MockProvider] = {}
        self._enabled: bool = True

    # -------------------- 启用/禁用 --------------------

    def enable(self) -> None:
        """启用虚拟数据注入"""
        self._enabled = True
        logger.debug("[MockData] 已启用")

    def disable(self) -> None:
        """禁用虚拟数据注入（不清除注册表，恢复 enable 即可重新注入）"""
        self._enabled = False
        logger.debug("[MockData] 已禁用")

    @property
    def enabled(self) -> bool:
        return self._enabled

    # -------------------- 注册管理 --------------------

    def register(
        self,
        topic: str,
        provider: MockProvider,
        station_id: Optional[str] = None,
    ) -> None:
        """
        注册虚拟数据提供者。

        Args:
            topic: topic 名称，如 "SCHEDULE-DEVICE-STORAGE"
            provider: 虚拟数据提供者实例
            station_id: 精确场站 ID；None 表示全局生效（所有场站均使用此 Provider，
                        但会被精确注册覆盖）
        """
        key = (station_id, topic)
        self._registry[key] = provider
        scope = f"场站[{station_id}]" if station_id else "全局"
        logger.info(
            f"[MockData] 注册: topic={topic}, 范围={scope}, "
            f"provider={type(provider).__name__}"
        )

    def unregister(self, topic: str, station_id: Optional[str] = None) -> bool:
        """
        注销虚拟数据提供者。

        Args:
            topic: topic 名称
            station_id: 场站 ID；None 表示注销全局注册

        Returns:
            bool: 是否成功找到并注销
        """
        key = (station_id, topic)
        if key in self._registry:
            del self._registry[key]
            logger.info(f"[MockData] 注销: topic={topic}, station_id={station_id}")
            return True
        return False

    def clear(self) -> None:
        """清空所有注册"""
        self._registry.clear()
        logger.info("[MockData] 已清空所有注册")

    # -------------------- 查询 --------------------

    def _get_provider(
        self, station_id: str, topic: str
    ) -> Optional[MockProvider]:
        """
        按优先级查找 Provider：精确场站 > 全局。

        Args:
            station_id: 场站 ID
            topic: topic 名称

        Returns:
            MockProvider 或 None
        """
        return self._registry.get((station_id, topic)) or self._registry.get(
            (None, topic)
        )

    def get_registry_info(self) -> List[Dict[str, str]]:
        """
        获取注册表信息，便于调试查看。

        Returns:
            list of dict，每项包含 station_id、topic、provider 字段
        """
        return [
            {
                "station_id": sid if sid is not None else "*",
                "topic": topic,
                "provider": type(provider).__name__,
            }
            for (sid, topic), provider in self._registry.items()
        ]

    def registered_topics(self, station_id: Optional[str] = None) -> List[str]:
        """
        获取指定场站（或全局）已注册的 topic 列表。

        Args:
            station_id: None 表示返回所有已注册 topic（去重）

        Returns:
            topic 名称列表
        """
        topics = set()
        for sid, topic in self._registry:
            if station_id is None or sid is None or sid == station_id:
                topics.add(topic)
        return sorted(topics)

    # -------------------- 注入 --------------------

    def inject(
        self,
        station_id: str,
        topic: str,
        force: bool = False,
        timestamp: Optional[float] = None,
    ) -> bool:
        """
        向指定场站/topic 注入一次虚拟数据。

        Args:
            station_id: 场站 ID
            topic: topic 名称
            force: True=强制覆盖（即使已有真实数据）；
                   False=仅在缺失时补全（默认）
            timestamp: 注入时间戳（秒），None 使用当前时间

        Returns:
            bool: 是否实际写入了数据
        """
        if not self._enabled:
            return False

        # 非强制模式：已有数据则跳过
        if not force:
            existing = self._dispatcher.get_topic_window(station_id, topic)
            if existing:
                logger.debug(
                    f"[MockData] 跳过（已有数据）: station={station_id}, topic={topic}"
                )
                return False

        provider = self._get_provider(station_id, topic)
        if provider is None:
            return False

        ts = timestamp if timestamp is not None else time.time()
        mock_data = provider.get_data(station_id, topic)

        # MultiDeviceMockProvider / 普通 Provider 分支处理
        if isinstance(provider, MultiDeviceMockProvider) or (
            topic in _DEVICE_AGGREGATION_TOPICS and isinstance(mock_data, list)
        ):
            # 多设备：逐条注入，每条是单设备 dict
            if not mock_data:
                return False
            for device_data in mock_data:
                self._dispatcher.update_topic_data(station_id, topic, device_data, ts)
        else:
            self._dispatcher.update_topic_data(station_id, topic, mock_data, ts)

        logger.info(
            f"[MockData] 注入: station={station_id}, topic={topic}, force={force}"
        )
        return True

    def inject_all(
        self,
        station_id: str,
        force: bool = False,
        timestamp: Optional[float] = None,
    ) -> Dict[str, bool]:
        """
        向指定场站注入所有已注册 topic 的虚拟数据。

        Args:
            station_id: 场站 ID
            force: 是否强制覆盖
            timestamp: 注入时间戳

        Returns:
            dict: {topic: 是否成功注入}
        """
        if not self._enabled:
            return {}

        results: Dict[str, bool] = {}
        # 收集该场站相关的所有 topic（精确匹配 + 全局匹配）
        topics: set = set()
        for sid, topic in self._registry:
            if sid is None or sid == station_id:
                topics.add(topic)

        for topic in sorted(topics):
            results[topic] = self.inject(
                station_id, topic, force=force, timestamp=timestamp
            )

        return results

    def inject_all_stations(
        self,
        force: bool = False,
        timestamp: Optional[float] = None,
    ) -> Dict[str, Dict[str, bool]]:
        """
        向 dispatcher 中所有已知场站注入虚拟数据。

        Args:
            force: 是否强制覆盖
            timestamp: 注入时间戳

        Returns:
            dict: {station_id: {topic: 是否成功注入}}
        """
        results: Dict[str, Dict[str, bool]] = {}
        for station_id in list(self._dispatcher.data_cache.keys()):
            results[station_id] = self.inject_all(
                station_id, force=force, timestamp=timestamp
            )
        return results
