#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2026-02-25
project: data_analysis
filename: mock_presets.py
version: 1.0
description: 预定义虚拟数据集合 - 常见 topic 的默认 mock 数据

快速使用::

    from d_a.mock_data import MockDataManager
    from d_a.mock_presets import apply_default_mocks, build_preset_provider
    from d_a.dispatcher import DataDispatcher

    dispatcher = DataDispatcher()
    mock_mgr = MockDataManager(dispatcher)
    
    # 场景1：只给指定场站补电价
    apply_default_mocks(mock_mgr, topics=["SCHEDULE-CAR-PRICE"])
    mock_mgr.inject("station_C", "SCHEDULE-CAR-PRICE")
    mock_mgr.inject("station_D", "SCHEDULE-CAR-PRICE")

    # 场景2：给指定场站补多个 topic
    apply_default_mocks(mock_mgr, topics=["SCHEDULE-CAR-PRICE", "SCHEDULE-DEVICE-STORAGE"])
    mock_mgr.inject("station_C", "SCHEDULE-CAR-PRICE")
    mock_mgr.inject("station_C", "SCHEDULE-DEVICE-STORAGE")
    # 或等价的简写（此时注册表只有这2个，inject_all 不会误伤）：
    mock_mgr.inject_all("station_C")

    # 场景3：给指定场站补全所有 topic（明确指定场站，全量注入）
    apply_default_mocks(mock_mgr, station_id="station_C")  # 精确注册到 station_C
    apply_default_mocks(mock_mgr, station_id="station_D")  # 精确注册到 station_D
    mock_mgr.inject_all("station_C")
    mock_mgr.inject_all("station_D")

    # 场景4：全局兜底（所有场站缺什么补什么，force=False 不覆盖真实数据）
    apply_default_mocks(mock_mgr)          # 全局注册
    mock_mgr.inject_all_stations()         # 对 dispatcher 中所有已知场站补全
"""

from .mock_data import (
    MockDataManager,
    MockProvider,
    MultiDeviceMockProvider,
    RandomMockProvider,
    StaticMockProvider,
)

# =============================================================================
# 预定义 Provider 字典
#
# 注意事项：
#   - 设备聚合类 topic（STORAGE/DCDC/ACDC）使用 MultiDeviceMockProvider，
#     内含多个单设备 StaticMockProvider，模拟多设备接入场景。
#   - 其余 topic 使用 StaticMockProvider 或 RandomMockProvider。
#   - 所有 {station_id} 占位符在注入时会被自动替换为实际场站 ID。
# =============================================================================

_DEFAULT_PRESETS: dict = {
    # ---------- 场站参数（1条/变更） ----------
    "SCHEDULE-STATION-PARAM": StaticMockProvider(
        {
            "stationId": "{station_id}",
            "stationName": "MOCK场站",
            "gridCapacity": 250.0,
            "gunNum": 4,
            "powerNum": 2,
            "hostCode": ["MOCK_HOST_001", "MOCK_HOST_002"],
            "stationLat": 30.0,
            "stationLng": 120.0,
        }
    ),

    # ---------- 场站实时数据（1秒/次） ----------
    # ⚠️ 格式说明：_update_station_realtime_data 会对 raw_data 做 for item in raw_data 迭代，
    # 然后逐条 append 到 deque。因此 mock 数据必须是列表（list[dict]），而非单个 dict。
    # 每条 item 格式对应 parse() 期望的单帧数据（含 sendTime / gunPower / hostCode）。
    "SCHEDULE-STATION-REALTIME-DATA": StaticMockProvider(
        [
            {
                "stationId": "{station_id}",
                "hostCode": "MOCK_HOST_001",
                "sendTime": "2026-02-25 08:00:00",
                "outputPowerPerStationMax": 150.0,
                "outputPowerPerStationAvg": 80.0,
                "gunPower": {
                    "gunNo": ["01", "02"],
                    "outputPowerPerGunAvg": [50.0, 30.0],
                    "outputPowerPerGunMax": [100.0, 80.0],
                },
            }
        ]
    ),

    # ---------- 电表（1秒/次） ----------
    "SCHEDULE-DEVICE-METER": RandomMockProvider(
        template={
            "stationId": "{station_id}",
            "activePower": None,
            "reactivePower": None,
            "totalActiveEnergy": None,
        },
        ranges={
            "activePower": (-200.0, 200.0),
            "reactivePower": (0.0, 30.0),
            "totalActiveEnergy": (0.0, 5000.0),
        },
    ),

    # ---------- 充电枪（1秒/次） ----------
    "SCHEDULE-DEVICE-GUN": StaticMockProvider(
        {
            "stationId": "{station_id}",
            "gunNo": "01",
            "hostCode": "MOCK_HOST_001",
            "gunStatus": 2,
            "outputPower": 50.0,
            "outputCurrent": 100.0,
            "outputVoltage": 500.0,
        }
    ),

    # ---------- 订单（事件触发） ----------
    "SCHEDULE-CAR-ORDER": StaticMockProvider(
        {
            "stationId": "{station_id}",
            "transactionSerialNo": "MOCK_ORDER_20260225_001",
            "gunNo": "01",
            "hostCode": "MOCK_HOST_001",
            "soc": 50.0,
            "outputPower": 100.0,
            "batteryNominalTotalCapacity": 80.0,
            "terminalRequireElectric": 80.0,
            "terminalRequireVoltage": 750.0,
            "terminalMaxOutElectric": 200.0,
            "carProducerCode": "0000000000000000",
            "startChargeTime": "2026-02-25 08:00:00",
            "endChargeTime": "2070-01-01 00:00:00",
        }
    ),

    # ---------- 电价（低频/变更） ----------
    # ⚠️ 格式说明：dispatcher 存储的是 _handle_car_price 解析后的每场站列表，
    # 不是 Kafka 原始的 {"fee": [...]} 包装格式。
    # CarPriceParser.parse_window 通过 window_data[-1][-1] 取最新一条 fee item，
    # 因此 mock 数据应为 [fee_item, ...] 列表。
    "SCHEDULE-CAR-PRICE": StaticMockProvider(
        [
            {
                "stationId": "{station_id}",
                "sendTime": "2026-02-25 00:00:00",
                "peakElectricFee": 0.7,
                "peakServerFee": 0.2,
                "flatElectricFee": 0.6,
                "flatServerFee": 0.0,
                "sharpElectricFee": 0.8,
                "sharpServerFee": 0.1,
                "ebbElectricFee": 0.8,
                "ebbServerFee": 0.4,
                # feeNo1-feeNo48：00=尖 01=峰 02=平 03=谷
                # 示例：0-7点谷/8-11峰/11-14平/14-19峰/19-21平/21-24谷（每段2个半小时）
                "feeNo1": "03", "feeNo2": "03", "feeNo3": "03", "feeNo4": "03",
                "feeNo5": "03", "feeNo6": "03", "feeNo7": "03", "feeNo8": "03",
                "feeNo9": "03", "feeNo10": "03", "feeNo11": "03", "feeNo12": "03",
                "feeNo13": "03", "feeNo14": "03", "feeNo15": "03", "feeNo16": "03",
                "feeNo17": "01", "feeNo18": "01", "feeNo19": "01", "feeNo20": "01",
                "feeNo21": "01", "feeNo22": "01", "feeNo23": "02", "feeNo24": "02",
                "feeNo25": "02", "feeNo26": "02", "feeNo27": "02", "feeNo28": "02",
                "feeNo29": "01", "feeNo30": "01", "feeNo31": "01", "feeNo32": "01",
                "feeNo33": "01", "feeNo34": "01", "feeNo35": "01", "feeNo36": "01",
                "feeNo37": "01", "feeNo38": "01", "feeNo39": "02", "feeNo40": "02",
                "feeNo41": "02", "feeNo42": "02", "feeNo43": "03", "feeNo44": "03",
                "feeNo45": "03", "feeNo46": "03", "feeNo47": "03", "feeNo48": "03",
            }
        ]
    ),

    # ---------- 故障信息（事件触发） ----------
    "SCHEDULE-DEVICE-ERROR": StaticMockProvider(
        {
            "stationId": "{station_id}",
            "hostCode": "MOCK_HOST_001",
            "errorCode": "0000",
            "errorLevel": 0,
            "errorDesc": "MOCK_NO_ERROR",
        }
    ),

    # ---------- 日历（低频/变更） ----------
    # ⚠️ 字段说明：EnvironmentCalendarParser.parse() 读 isHoliday / isWeekend（int 0/1），
    # 不是 dayType 字符串。
    "SCHEDULE-ENVIRONMENT-CALENDAR": StaticMockProvider(
        {
            "calendar": [
                {"date": "2026-02-25", "isHoliday": 0, "isWeekend": 0},
                {"date": "2026-02-26", "isHoliday": 0, "isWeekend": 0},
                {"date": "2026-02-27", "isHoliday": 0, "isWeekend": 0},
                {"date": "2026-02-28", "isHoliday": 0, "isWeekend": 0},
                {"date": "2026-03-01", "isHoliday": 0, "isWeekend": 1},
                {"date": "2026-03-02", "isHoliday": 0, "isWeekend": 1},
                {"date": "2026-03-03", "isHoliday": 0, "isWeekend": 0},
            ]
        }
    ),

    # ---------- 天气（低频/变更） ----------
    # ⚠️ 格式说明：_handle_environment_weather 解包 {"weather": [...]} 后逐项存储，
    # dispatcher 中保存的是单条 item dict，mock 直接提供该 item 即可。
    "SCHEDULE-ENVIRONMENT-WEATHER": StaticMockProvider(
        {
            "stationId": "{station_id}",
            "weatherSituationTomorrow": "晴",
            "weatherSituationYesterday": "多云",
            "seasonTomorrow": "冬",
            "temperatureMaxTomorrow": 15.0,
            "temperatureMinTomorrow": 5.0,
        }
    ),

    # ---------- 光伏设备（1秒/次） ----------
    # ⚠️ 格式说明：_handle_device_pv 解包 {"photovoltaicMessage": [...]} 后逐项存储，
    # dispatcher 中保存的是单条 item dict，mock 直接提供该 item 即可。
    "SCHEDULE-DEVICE-PV": StaticMockProvider(
        {
            "stationId": "{station_id}",
            "hostCode": "MOCK_HOST_001",
            "pvPreDcPower": [10.0, 20.0, 30.0, 25.0, 15.0],
            "pvRealDcPower": 18.5,
        }
    ),

    # ---------- 储能设备（15秒/次，多设备聚合）----------
    # MultiDeviceMockProvider 会逐一注入每个设备数据，dispatcher 自动聚合
    "SCHEDULE-DEVICE-STORAGE": MultiDeviceMockProvider(
        [
            StaticMockProvider(
                {
                    "stationId": "{station_id}",
                    "storageCode": "MOCK_STORAGE_001",
                    "hostCode": "MOCK_HOST_001",
                    "batteryGroupSoc": 75,
                    "batteryGroupSoh": 95,
                    "batteryGroupElectric": 10.0,
                    "batteryGroupOutPower": 5.0,
                    "maxTemperature": 35.0,
                    "minTemperature": 10.0,
                }
            ),
            StaticMockProvider(
                {
                    "stationId": "{station_id}",
                    "storageCode": "MOCK_STORAGE_002",
                    "hostCode": "MOCK_HOST_002",
                    "batteryGroupSoc": 60,
                    "batteryGroupSoh": 90,
                    "batteryGroupElectric": 8.0,
                    "batteryGroupOutPower": 4.0,
                    "maxTemperature": 33.0,
                    "minTemperature": 12.0,
                }
            ),
        ]
    ),

    # ---------- DCDC 主机（1秒/次，多设备聚合）----------
    "SCHEDULE-DEVICE-HOST-DCDC": MultiDeviceMockProvider(
        [
            StaticMockProvider(
                {
                    "stationId": "{station_id}",
                    "hostCode": "MOCK_HOST_001",
                    "dcWorkStatus": [1, 1, 0, 1],
                    "dcOutputPower": [50.0, 60.0, 0.0, 45.0],
                    "dcOutputCurrent": [100.0, 120.0, 0.0, 90.0],
                    "dcOutputVoltage": [500.0, 500.0, 0.0, 500.0],
                }
            ),
            StaticMockProvider(
                {
                    "stationId": "{station_id}",
                    "hostCode": "MOCK_HOST_002",
                    "dcWorkStatus": [1, 0],
                    "dcOutputPower": [80.0, 0.0],
                    "dcOutputCurrent": [160.0, 0.0],
                    "dcOutputVoltage": [500.0, 0.0],
                }
            ),
        ]
    ),

    # ---------- ACDC 主机（1秒/次，多设备聚合）----------
    "SCHEDULE-DEVICE-HOST-ACDC": MultiDeviceMockProvider(
        [
            StaticMockProvider(
                {
                    "stationId": "{station_id}",
                    "hostCode": "MOCK_HOST_001",
                    "acPower": 100.0,
                    "acCurrent": 150.0,
                    "acVoltage": 380.0,
                    "acWorkStatus": 1,
                }
            ),
            StaticMockProvider(
                {
                    "stationId": "{station_id}",
                    "hostCode": "MOCK_HOST_002",
                    "acPower": 80.0,
                    "acCurrent": 120.0,
                    "acVoltage": 380.0,
                    "acWorkStatus": 1,
                }
            ),
        ]
    ),
}


def build_preset_provider(topic: str) -> MockProvider:
    """
    获取指定 topic 的预设 Provider 副本。

    Args:
        topic: topic 名称

    Returns:
        MockProvider 实例

    Raises:
        KeyError: 未找到对应预设时抛出
    """
    if topic not in _DEFAULT_PRESETS:
        raise KeyError(
            f"未找到 topic '{topic}' 的预设，可用预设: {list(_DEFAULT_PRESETS.keys())}"
        )
    return _DEFAULT_PRESETS[topic]


def list_preset_topics() -> list:
    """返回所有预设 topic 列表"""
    return list(_DEFAULT_PRESETS.keys())


def apply_default_mocks(
    mock_mgr: MockDataManager,
    topics: list = None,
    station_id: str = None,
) -> None:
    """
    向 MockDataManager 批量注册预定义虚拟数据。

    调用后需要再调用 mock_mgr.inject_all(station_id) 才会真正写入 dispatcher 缓存。

    Args:
        mock_mgr: MockDataManager 实例
        topics: 要注册的 topic 列表；None 表示注册所有预设
        station_id: 精确场站 ID；None 表示全局生效（所有场站）

    Example::

        apply_default_mocks(mock_mgr)                          # 全部全局注册
        apply_default_mocks(mock_mgr, station_id="station_1") # 全部精确注册
        apply_default_mocks(                                   # 部分全局注册
            mock_mgr,
            topics=["SCHEDULE-DEVICE-STORAGE", "SCHEDULE-CAR-PRICE"],
        )
    """
    target_topics = topics if topics is not None else list(_DEFAULT_PRESETS.keys())
    for topic in target_topics:
        if topic in _DEFAULT_PRESETS:
            mock_mgr.register(topic, _DEFAULT_PRESETS[topic], station_id=station_id)
        else:
            import logging
            logging.getLogger(__name__).warning(
                f"[MockData] apply_default_mocks: 未找到 topic '{topic}' 的预设，已跳过"
            )
