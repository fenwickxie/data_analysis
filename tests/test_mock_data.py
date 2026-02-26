#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2026-02-25
project: data_analysis
filename: test_mock_data.py
version: 1.0
description: 虚拟数据注入模块单元测试

运行:
    python -m pytest tests/test_mock_data.py -v
    # 或单独运行
    python tests/test_mock_data.py
"""

import sys
import time
from pathlib import Path

# 确保项目根目录在 Python 路径中
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import pytest
from d_a.dispatcher import DataDispatcher
from d_a.mock.mock_data import (
    CallableMockProvider,
    MockDataManager,
    MultiDeviceMockProvider,
    RandomMockProvider,
    SequenceMockProvider,
    StaticMockProvider,
)
from d_a.mock.mock_presets import (
    apply_default_mocks,
    build_preset_provider,
    list_preset_topics,
)


# ==================== Fixtures ====================


@pytest.fixture
def dispatcher():
    """每个测试用例独立的 DataDispatcher，关闭数据过期"""
    return DataDispatcher(data_expire_seconds=600, enable_data_expiration=False)


@pytest.fixture
def mock_mgr(dispatcher):
    """每个测试用例独立的 MockDataManager"""
    return MockDataManager(dispatcher)


STATION_A = "mock_station_001"
STATION_B = "mock_station_002"


# ==================== StaticMockProvider ====================


class TestStaticMockProvider:
    def test_returns_static_data(self):
        provider = StaticMockProvider({"soc": 80, "stationId": "{station_id}"})
        data = provider.get_data(STATION_A, "SCHEDULE-DEVICE-STORAGE")
        assert data["soc"] == 80
        assert data["stationId"] == STATION_A

    def test_placeholder_replaced(self):
        provider = StaticMockProvider({
            "stationId": "{station_id}",
            "nested": {"id": "{station_id}"},
            "list": ["{station_id}", "other"],
        })
        data = provider.get_data("my_station", "ANY_TOPIC")
        assert data["stationId"] == "my_station"
        assert data["nested"]["id"] == "my_station"
        assert data["list"][0] == "my_station"

    def test_deep_copy_isolation(self):
        """多次 get_data 返回的对象互不影响"""
        provider = StaticMockProvider({"items": [1, 2, 3]})
        d1 = provider.get_data(STATION_A, "T")
        d2 = provider.get_data(STATION_A, "T")
        d1["items"].append(99)
        assert d2["items"] == [1, 2, 3]


# ==================== RandomMockProvider ====================


class TestRandomMockProvider:
    def test_values_in_range(self):
        provider = RandomMockProvider(
            template={"power": None, "soc": None, "stationId": "{station_id}"},
            ranges={"power": (-100.0, 100.0), "soc": (0, 100)},
            int_fields=["soc"],
        )
        for _ in range(20):
            data = provider.get_data(STATION_A, "T")
            assert -100.0 <= data["power"] <= 100.0
            assert isinstance(data["soc"], int)
            assert 0 <= data["soc"] <= 100
            assert data["stationId"] == STATION_A

    def test_non_random_fields_unchanged(self):
        provider = RandomMockProvider(
            template={"fixed": "hello", "rand": None},
            ranges={"rand": (0, 1)},
        )
        data = provider.get_data(STATION_A, "T")
        assert data["fixed"] == "hello"


# ==================== CallableMockProvider ====================


class TestCallableMockProvider:
    def test_calls_function(self):
        provider = CallableMockProvider(
            lambda sid, topic: {"station": sid, "topic": topic, "ts": time.time()}
        )
        data = provider.get_data(STATION_A, "MY_TOPIC")
        assert data["station"] == STATION_A
        assert data["topic"] == "MY_TOPIC"
        assert "ts" in data

    def test_custom_logic(self):
        call_count = [0]

        def gen(sid, topic):
            call_count[0] += 1
            return {"count": call_count[0]}

        provider = CallableMockProvider(gen)
        assert provider.get_data(STATION_A, "T")["count"] == 1
        assert provider.get_data(STATION_A, "T")["count"] == 2


# ==================== SequenceMockProvider ====================


class TestSequenceMockProvider:
    def test_sequence_loop(self):
        provider = SequenceMockProvider(
            [{"soc": 20}, {"soc": 50}, {"soc": 80}], loop=True
        )
        assert provider.get_data(STATION_A, "T")["soc"] == 20
        assert provider.get_data(STATION_A, "T")["soc"] == 50
        assert provider.get_data(STATION_A, "T")["soc"] == 80
        # 第4次循环回到头
        assert provider.get_data(STATION_A, "T")["soc"] == 20

    def test_sequence_no_loop(self):
        provider = SequenceMockProvider([{"v": 1}, {"v": 2}], loop=False)
        assert provider.get_data(STATION_A, "T")["v"] == 1
        assert provider.get_data(STATION_A, "T")["v"] == 2
        assert provider.get_data(STATION_A, "T")["v"] == 2  # 停在末尾

    def test_reset(self):
        provider = SequenceMockProvider([{"n": 1}, {"n": 2}])
        provider.get_data(STATION_A, "T")
        provider.reset()
        assert provider.get_data(STATION_A, "T")["n"] == 1

    def test_placeholder_in_sequence(self):
        provider = SequenceMockProvider([{"id": "{station_id}"}])
        data = provider.get_data("xyz", "T")
        assert data["id"] == "xyz"

    def test_deep_copy_isolation(self):
        provider = SequenceMockProvider([{"items": [1, 2]}])
        d1 = provider.get_data(STATION_A, "T")
        provider.reset()
        d2 = provider.get_data(STATION_A, "T")
        d1["items"].append(99)
        assert d2["items"] == [1, 2]


# ==================== MockDataManager 核心逻辑 ====================


class TestMockDataManagerRegister:
    def test_register_global(self, mock_mgr):
        provider = StaticMockProvider({"soc": 1})
        mock_mgr.register("TOPIC_X", provider)
        info = mock_mgr.get_registry_info()
        assert any(i["station_id"] == "*" and i["topic"] == "TOPIC_X" for i in info)

    def test_register_specific_station(self, mock_mgr):
        provider = StaticMockProvider({"soc": 1})
        mock_mgr.register("TOPIC_X", provider, station_id=STATION_A)
        info = mock_mgr.get_registry_info()
        assert any(i["station_id"] == STATION_A and i["topic"] == "TOPIC_X" for i in info)

    def test_unregister(self, mock_mgr):
        provider = StaticMockProvider({})
        mock_mgr.register("TOPIC_X", provider)
        assert mock_mgr.unregister("TOPIC_X") is True
        assert mock_mgr.unregister("TOPIC_X") is False  # 已不存在

    def test_clear(self, mock_mgr):
        mock_mgr.register("A", StaticMockProvider({}))
        mock_mgr.register("B", StaticMockProvider({}))
        mock_mgr.clear()
        assert mock_mgr.get_registry_info() == []

    def test_registered_topics(self, mock_mgr):
        mock_mgr.register("T1", StaticMockProvider({}))
        mock_mgr.register("T2", StaticMockProvider({}), station_id=STATION_A)
        topics = mock_mgr.registered_topics()
        assert "T1" in topics and "T2" in topics

    def test_specific_overrides_global(self, mock_mgr, dispatcher):
        """精确场站注册应优先于全局注册"""
        mock_mgr.register(
            "SCHEDULE-STATION-PARAM",
            StaticMockProvider({"gridCapacity": 999.0, "stationId": "{station_id}"}),
        )
        mock_mgr.register(
            "SCHEDULE-STATION-PARAM",
            StaticMockProvider({"gridCapacity": 100.0, "stationId": "{station_id}"}),
            station_id=STATION_A,
        )
        mock_mgr.inject(STATION_A, "SCHEDULE-STATION-PARAM")
        window = dispatcher.get_topic_window(STATION_A, "SCHEDULE-STATION-PARAM")
        assert window[-1]["gridCapacity"] == 100.0


class TestMockDataManagerInject:
    def test_inject_fills_missing(self, mock_mgr, dispatcher):
        mock_mgr.register(
            "SCHEDULE-STATION-PARAM",
            StaticMockProvider({"gridCapacity": 250.0, "stationId": "{station_id}"}),
        )
        assert dispatcher.get_topic_window(STATION_A, "SCHEDULE-STATION-PARAM") == []
        result = mock_mgr.inject(STATION_A, "SCHEDULE-STATION-PARAM")
        assert result is True
        window = dispatcher.get_topic_window(STATION_A, "SCHEDULE-STATION-PARAM")
        assert len(window) == 1
        assert window[0]["gridCapacity"] == 250.0
        assert window[0]["stationId"] == STATION_A

    def test_inject_skips_existing_when_not_forced(self, mock_mgr, dispatcher):
        """非强制模式：已有数据时不覆盖"""
        dispatcher.update_topic_data(
            STATION_A,
            "SCHEDULE-STATION-PARAM",
            {"gridCapacity": 100.0, "stationId": STATION_A},
            None,
        )
        mock_mgr.register(
            "SCHEDULE-STATION-PARAM",
            StaticMockProvider({"gridCapacity": 999.0, "stationId": "{station_id}"}),
        )
        result = mock_mgr.inject(STATION_A, "SCHEDULE-STATION-PARAM", force=False)
        assert result is False
        window = dispatcher.get_topic_window(STATION_A, "SCHEDULE-STATION-PARAM")
        assert window[-1]["gridCapacity"] == 100.0  # 真实数据未被覆盖

    def test_inject_force_overrides(self, mock_mgr, dispatcher):
        """强制模式：覆盖已有数据"""
        dispatcher.update_topic_data(
            STATION_A,
            "SCHEDULE-STATION-PARAM",
            {"gridCapacity": 100.0, "stationId": STATION_A},
            None,
        )
        mock_mgr.register(
            "SCHEDULE-STATION-PARAM",
            StaticMockProvider({"gridCapacity": 999.0, "stationId": "{station_id}"}),
        )
        result = mock_mgr.inject(STATION_A, "SCHEDULE-STATION-PARAM", force=True)
        assert result is True
        window = dispatcher.get_topic_window(STATION_A, "SCHEDULE-STATION-PARAM")
        # 强制覆盖后，最新数据应为 mock 数据
        assert window[-1]["gridCapacity"] == 999.0

    def test_inject_no_provider_returns_false(self, mock_mgr):
        result = mock_mgr.inject(STATION_A, "NO_SUCH_TOPIC")
        assert result is False

    def test_inject_disabled_returns_false(self, mock_mgr):
        mock_mgr.register(
            "SCHEDULE-STATION-PARAM",
            StaticMockProvider({"gridCapacity": 1.0}),
        )
        mock_mgr.disable()
        result = mock_mgr.inject(STATION_A, "SCHEDULE-STATION-PARAM")
        assert result is False

    def test_inject_all(self, mock_mgr, dispatcher):
        mock_mgr.register(
            "SCHEDULE-STATION-PARAM",
            StaticMockProvider({"gridCapacity": 250.0, "stationId": "{station_id}"}),
        )
        mock_mgr.register(
            "SCHEDULE-CAR-PRICE",
            # 格式为经 _handle_car_price 解析后的列表，不是 Kafka 原始 {"fee": [...]} 包装
            StaticMockProvider([{"stationId": "{station_id}", "peakElectricFee": 1.2, "ebbElectricFee": 0.3}]),
        )
        results = mock_mgr.inject_all(STATION_A)
        assert results.get("SCHEDULE-STATION-PARAM") is True
        assert results.get("SCHEDULE-CAR-PRICE") is True

    def test_inject_all_stations(self, mock_mgr, dispatcher):
        # 先让两个场站在 dispatcher 中存在
        dispatcher.update_topic_data(
            STATION_A, "SCHEDULE-STATION-PARAM",
            {"stationId": STATION_A}, None
        )
        dispatcher.update_topic_data(
            STATION_B, "SCHEDULE-STATION-PARAM",
            {"stationId": STATION_B}, None
        )
        mock_mgr.register(
            "SCHEDULE-CAR-PRICE",
            StaticMockProvider([]),  # 空列表，与实际存储格式一致
        )
        results = mock_mgr.inject_all_stations()
        assert STATION_A in results
        assert STATION_B in results


# ==================== 设备聚合类 topic 测试 ====================


class TestDeviceAggregationTopics:
    def test_storage_multi_device_inject(self, mock_mgr, dispatcher):
        """储能多设备注入后，dispatcher 聚合字典中应有两个设备"""
        mock_mgr.register(
            "SCHEDULE-DEVICE-STORAGE",
            MultiDeviceMockProvider([
                StaticMockProvider({
                    "stationId": "{station_id}",
                    "storageCode": "STORAGE_A",
                    "batteryGroupSoc": 80,
                }),
                StaticMockProvider({
                    "stationId": "{station_id}",
                    "storageCode": "STORAGE_B",
                    "batteryGroupSoc": 60,
                }),
            ]),
        )
        mock_mgr.inject(STATION_A, "SCHEDULE-DEVICE-STORAGE")
        window = dispatcher.get_topic_window(STATION_A, "SCHEDULE-DEVICE-STORAGE")
        # get_topic_window 对聚合类返回 [聚合字典]
        assert len(window) == 1
        storage_dict = window[0]
        assert "STORAGE_A" in storage_dict
        assert "STORAGE_B" in storage_dict
        assert storage_dict["STORAGE_A"][0]["batteryGroupSoc"] == 80
        assert storage_dict["STORAGE_B"][0]["batteryGroupSoc"] == 60

    def test_dcdc_multi_device_inject(self, mock_mgr, dispatcher):
        mock_mgr.register(
            "SCHEDULE-DEVICE-HOST-DCDC",
            MultiDeviceMockProvider([
                StaticMockProvider({
                    "stationId": "{station_id}",
                    "hostCode": "HOST_001",
                    "dcWorkStatus": [1, 1],
                }),
                StaticMockProvider({
                    "stationId": "{station_id}",
                    "hostCode": "HOST_002",
                    "dcWorkStatus": [0, 1],
                }),
            ]),
        )
        mock_mgr.inject(STATION_A, "SCHEDULE-DEVICE-HOST-DCDC")
        window = dispatcher.get_topic_window(STATION_A, "SCHEDULE-DEVICE-HOST-DCDC")
        assert len(window) == 1
        host_dict = window[0]
        assert "HOST_001" in host_dict and "HOST_002" in host_dict


# ==================== 预设测试 ====================


class TestMockPresets:
    def test_list_preset_topics(self):
        topics = list_preset_topics()
        assert "SCHEDULE-DEVICE-STORAGE" in topics
        assert "SCHEDULE-CAR-PRICE" in topics
        assert "SCHEDULE-STATION-PARAM" in topics

    def test_build_preset_provider(self):
        provider = build_preset_provider("SCHEDULE-STATION-PARAM")
        data = provider.get_data(STATION_A, "SCHEDULE-STATION-PARAM")
        assert data["stationId"] == STATION_A

    def test_build_preset_provider_unknown(self):
        with pytest.raises(KeyError):
            build_preset_provider("NO_SUCH_TOPIC")

    def test_apply_default_mocks_all(self, mock_mgr):
        apply_default_mocks(mock_mgr)
        topics = mock_mgr.registered_topics()
        assert "SCHEDULE-DEVICE-STORAGE" in topics
        assert "SCHEDULE-CAR-PRICE" in topics

    def test_apply_default_mocks_partial(self, mock_mgr):
        apply_default_mocks(mock_mgr, topics=["SCHEDULE-STATION-PARAM", "SCHEDULE-CAR-PRICE"])
        topics = mock_mgr.registered_topics()
        assert "SCHEDULE-STATION-PARAM" in topics
        assert "SCHEDULE-CAR-PRICE" in topics
        assert "SCHEDULE-DEVICE-STORAGE" not in topics

    def test_apply_default_mocks_with_station(self, mock_mgr):
        apply_default_mocks(mock_mgr, station_id=STATION_A)
        info = mock_mgr.get_registry_info()
        assert all(i["station_id"] == STATION_A for i in info)

    def test_full_inject_with_presets(self, mock_mgr, dispatcher):
        """预设数据全量注入后，dispatcher 缓存中应有对应 topic 数据"""
        apply_default_mocks(mock_mgr)
        results = mock_mgr.inject_all(STATION_A)

        # 验证普通 topic
        assert results.get("SCHEDULE-STATION-PARAM") is True
        window = dispatcher.get_topic_window(STATION_A, "SCHEDULE-STATION-PARAM")
        assert len(window) > 0
        assert window[-1]["stationId"] == STATION_A

        # 验证设备聚合 topic
        assert results.get("SCHEDULE-DEVICE-STORAGE") is True
        storage_window = dispatcher.get_topic_window(STATION_A, "SCHEDULE-DEVICE-STORAGE")
        assert len(storage_window) == 1
        storage_dict = storage_window[0]
        assert "MOCK_STORAGE_001" in storage_dict
        assert "MOCK_STORAGE_002" in storage_dict


# ==================== 综合场景测试 ====================


class TestIntegrationScenarios:
    def test_sequence_simulate_time_series(self, mock_mgr, dispatcher):
        """使用 SequenceMockProvider 模拟 SOC 随时间变化"""
        mock_mgr.register(
            "SCHEDULE-DEVICE-STORAGE",
            SequenceMockProvider([
                MultiDeviceMockProvider([
                    StaticMockProvider({"storageCode": "S1", "batteryGroupSoc": 20, "stationId": "{station_id}"}),
                ]),
                MultiDeviceMockProvider([
                    StaticMockProvider({"storageCode": "S1", "batteryGroupSoc": 50, "stationId": "{station_id}"}),
                ]),
            ]),
        )
        mock_mgr.inject(STATION_A, "SCHEDULE-DEVICE-STORAGE", force=True)
        soc1 = dispatcher.get_topic_window(STATION_A, "SCHEDULE-DEVICE-STORAGE")[0]["S1"][0]["batteryGroupSoc"]
        mock_mgr.inject(STATION_A, "SCHEDULE-DEVICE-STORAGE", force=True)
        soc2 = dispatcher.get_topic_window(STATION_A, "SCHEDULE-DEVICE-STORAGE")[0]["S1"][0]["batteryGroupSoc"]
        assert soc1 == 20
        assert soc2 == 50

    def test_multi_station_isolation(self, mock_mgr, dispatcher):
        """不同场站的虚拟数据互不干扰"""
        mock_mgr.register(
            "SCHEDULE-STATION-PARAM",
            StaticMockProvider({"stationId": "{station_id}", "gridCapacity": 999.0}),
            station_id=STATION_A,
        )
        mock_mgr.register(
            "SCHEDULE-STATION-PARAM",
            StaticMockProvider({"stationId": "{station_id}", "gridCapacity": 100.0}),
            station_id=STATION_B,
        )
        mock_mgr.inject(STATION_A, "SCHEDULE-STATION-PARAM")
        mock_mgr.inject(STATION_B, "SCHEDULE-STATION-PARAM")

        w_a = dispatcher.get_topic_window(STATION_A, "SCHEDULE-STATION-PARAM")
        w_b = dispatcher.get_topic_window(STATION_B, "SCHEDULE-STATION-PARAM")
        assert w_a[-1]["gridCapacity"] == 999.0
        assert w_b[-1]["gridCapacity"] == 100.0
        assert w_a[-1]["stationId"] == STATION_A
        assert w_b[-1]["stationId"] == STATION_B

    def test_callable_dynamic_generation(self, mock_mgr, dispatcher):
        """CallableMockProvider 动态生成含时间戳的数据"""
        mock_mgr.register(
            "SCHEDULE-DEVICE-METER",
            CallableMockProvider(
                lambda sid, topic: {
                    "stationId": sid,
                    "activePower": 100.0,
                    "ts": time.time(),
                }
            ),
        )
        mock_mgr.inject(STATION_A, "SCHEDULE-DEVICE-METER")
        window = dispatcher.get_topic_window(STATION_A, "SCHEDULE-DEVICE-METER")
        assert len(window) == 1
        assert window[0]["stationId"] == STATION_A
        assert "ts" in window[0]

    def test_enable_disable(self, mock_mgr, dispatcher):
        mock_mgr.register(
            "SCHEDULE-STATION-PARAM",
            StaticMockProvider({"stationId": "{station_id}", "gridCapacity": 1.0}),
        )
        mock_mgr.disable()
        result = mock_mgr.inject(STATION_A, "SCHEDULE-STATION-PARAM")
        assert result is False
        assert dispatcher.get_topic_window(STATION_A, "SCHEDULE-STATION-PARAM") == []

        mock_mgr.enable()
        result = mock_mgr.inject(STATION_A, "SCHEDULE-STATION-PARAM")
        assert result is True
        assert len(dispatcher.get_topic_window(STATION_A, "SCHEDULE-STATION-PARAM")) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
