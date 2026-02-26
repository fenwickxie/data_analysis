#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
虚拟数据注入配置模块

本文件负责虚拟数据注入的所有配置与生命周期管理，与 main.py 业务逻辑解耦。
在本地测试时只需修改此文件，生产部署时 ENABLE_MOCK=0（默认），本模块不产生任何副作用。

────────────────────────────────────────────────────────────────────
  注入架构（三层触发）
  1. build_mock_manager   → 注册"WHAT"：对哪些场站/topic 用什么数据
  2. mock_startup_inject  → 启动时"WHO/WHEN"：对已知场站立即写入 dispatcher
  3. make_station_init_hook → Kafka 零窗口：新场站出现时自动触发
  4. mock_auto_inject_loop  → 后台任务：非 force 兜底一次 / force 持续刷新
────────────────────────────────────────────────────────────────────

启动命令：
  MOCK_DATA=1 python main.py
  MOCK_DATA=1 MOCK_STATIONS=station_001,station_002 python main.py
  MOCK_DATA=1 MOCK_FORCE=1 MOCK_STATIONS=station_001 python main.py
"""

import asyncio
import logging
import os

# ===================== 虚拟数据开关 =====================
# 通过环境变量控制，生产环境无需改动代码：
#   MOCK_DATA=1            启用虚拟数据补全（仅补全缺失 topic，不覆盖真实数据）
#   MOCK_STATIONS=id1,id2  预注入指定场站（逗号分隔）；留空则只补全 Kafka 动态发现的场站
#   MOCK_FORCE=1           强制覆盖所有 topic（完全屏蔽 Kafka 数据，用于纯本地闭环测试）
ENABLE_MOCK: bool = os.environ.get("MOCK_DATA", "0") == "1"
MOCK_FORCE: bool = os.environ.get("MOCK_FORCE", "0") == "1"
MOCK_STATIONS: list = [
    s.strip()
    for s in os.environ.get("MOCK_STATIONS", "").split(",")
    if s.strip()
]


# ===================== 虚拟数据管理 =====================

def build_mock_manager(service):
    """
    构建并配置 MockDataManager。

    根据测试场景选择对应的代码块取消注释。各场景互斥，保留一个即可。

    ┌─────────────────────────────────────────────────────────────────┐
    │  场景速查                                                        │
    │  A. 全量兜底     - 对所有场站补全所有缺失 topic（推荐默认）        │
    │  B. 指定 topic   - 只补全部分 topic，其余依赖真实 Kafka 数据      │
    │  C. 双向精确     - 指定场站 × 指定 topic（最小影响范围）           │
    │  D. 指定场站     - 仅对特定场站注入所有 topic，不影响其他场站      │
    │  E. 差异化场站   - 不同场站注入不同数据（特殊测试场景）            │
    │  F. 自定义覆盖   - 在全局预设基础上用精确注册覆盖个别 topic        │
    │  G. 纯自定义     - 不用预设，完全手写所有数据（精细控制）          │
    └─────────────────────────────────────────────────────────────────┘

    Args:
        service: AsyncDataAnalysisService 实例

    Returns:
        MockDataManager
    """
    import time  # noqa: F401 （场景 F 的 lambda 中使用）

    from .mock_data import (
        CallableMockProvider,  # noqa: F401
        MockDataManager,
        MultiDeviceMockProvider,  # noqa: F401
        RandomMockProvider,  # noqa: F401
        SequenceMockProvider,  # noqa: F401
        StaticMockProvider,  # noqa: F401
    )
    from .mock_presets import apply_default_mocks

    mock_mgr = MockDataManager(service.dispatcher)

    # ================================================================
    # 场景 A【默认推荐】全量兜底
    #   对 Kafka 动态接入的所有场站，缺什么 topic 补什么。
    #   force=False（默认），真实 Kafka 数据优先，不会覆盖。
    #   启动命令：MOCK_DATA=1 python main.py
    # ================================================================
    # apply_default_mocks(mock_mgr)          # 注册全部 13 个 topic 的全局预设

    # ================================================================
    # 场景 B 只补全指定 topic（其他 topic 完全依赖真实 Kafka 数据）
    #   适合：模型只缺少电价 / 日历 / 天气等低频数据的场景。
    #   启动命令：MOCK_DATA=1 python main.py
    # ================================================================
    # apply_default_mocks(mock_mgr, topics=[
    #     "SCHEDULE-CAR-PRICE",
    #     "SCHEDULE-ENVIRONMENT-CALENDAR",
    #     "SCHEDULE-ENVIRONMENT-WEATHER",
    # ])

    # ================================================================
    # 场景 C 指定场站 + 指定 topic（双向精确控制）
    #   适合：只对某几个场站补全某几个 topic，其余场站和 topic 完全不动。
    #   apply_default_mocks 同时传 topics= 和 station_id= 即可。
    #   启动命令：MOCK_DATA=1 python main.py
    # ================================================================
    apply_default_mocks(mock_mgr,
        topics=["SCHEDULE-CAR-PRICE"],
        station_id="CN00151807",
    )
    apply_default_mocks(mock_mgr,
        topics=["SCHEDULE-CAR-PRICE"],
        station_id="CN00198099",
    )
    apply_default_mocks(mock_mgr,
        topics=["SCHEDULE-CAR-PRICE"],
        station_id="CN00235802",
    )

    # # 也可以直接用 register 注册单条（不依赖预设）：
    # from .mock_presets import build_preset_provider
    # mock_mgr.register(
    #     "SCHEDULE-CAR-PRICE",
    #     station_id="station_E",
    #     provider=build_preset_provider("SCHEDULE-CAR-PRICE"),
    # )

    # ================================================================
    # 场景 D 仅对指定场站注入所有缺失 topic（不影响其他场站）
    #   适合：线上部分场站数据缺失，不影响已有真实数据场站。
    #   注意：此处只注册，实际注入发生在 mock_auto_inject_loop 或钩子里。
    #   启动命令：MOCK_DATA=1 MOCK_STATIONS=station_C,station_D python main.py
    # ================================================================
    # apply_default_mocks(mock_mgr, station_id="station_C")  # 精确绑定 station_C
    # apply_default_mocks(mock_mgr, station_id="station_D")  # 精确绑定 station_D

    # ================================================================
    # 场景 E 不同场站注入差异化数据
    #   适合：测试场站在不同电价区间 / 不同储能容量下的预测差异。
    # ================================================================
    # # station_C：低谷电价场景
    # mock_mgr.register(
    #     "SCHEDULE-CAR-PRICE",
    #     station_id="station_C",
    #     provider=StaticMockProvider([{
    #         "stationId": "{station_id}", "sendTime": "2026-02-25 00:00:00",
    #         "peakElectricFee": 0.7, "flatElectricFee": 0.5,
    #         "sharpElectricFee": 0.9, "ebbElectricFee": 0.3,
    #         "peakServerFee": 0.0, "flatServerFee": 0.0,
    #         "sharpServerFee": 0.0, "ebbServerFee": 0.0,
    #         **{f"feeNo{i}": "03" for i in range(1, 17)},   # 0-8点谷
    #         **{f"feeNo{i}": "01" for i in range(17, 29)},  # 8-14点峰
    #         **{f"feeNo{i}": "02" for i in range(29, 41)},  # 14-20点平
    #         **{f"feeNo{i}": "03" for i in range(41, 49)},  # 20-24点谷
    #     }]),
    # )
    # # station_D：高峰电价场景（尖峰较多）
    # mock_mgr.register(
    #     "SCHEDULE-CAR-PRICE",
    #     station_id="station_D",
    #     provider=StaticMockProvider([{
    #         "stationId": "{station_id}", "sendTime": "2026-02-25 00:00:00",
    #         "peakElectricFee": 1.2, "flatElectricFee": 0.8,
    #         "sharpElectricFee": 1.5, "ebbElectricFee": 0.35,
    #         "peakServerFee": 0.1, "flatServerFee": 0.0,
    #         "sharpServerFee": 0.2, "ebbServerFee": 0.0,
    #         **{f"feeNo{i}": "03" for i in range(1, 9)},    # 0-4点谷
    #         **{f"feeNo{i}": "01" for i in range(9, 23)},   # 4-11点峰
    #         **{f"feeNo{i}": "00" for i in range(23, 31)},  # 11-15点尖
    #         **{f"feeNo{i}": "01" for i in range(31, 43)},  # 15-21点峰
    #         **{f"feeNo{i}": "03" for i in range(43, 49)},  # 21-24点谷
    #     }]),
    # )
    # # station_D / station_C 其余 topic 用全局预设兜底
    # apply_default_mocks(mock_mgr, topics=[
    #     "SCHEDULE-STATION-PARAM", "SCHEDULE-DEVICE-STORAGE",
    #     "SCHEDULE-DEVICE-HOST-DCDC", "SCHEDULE-DEVICE-HOST-ACDC",
    # ])

    # ================================================================
    # 场景 F 全局预设 + 对特定 topic 精确覆盖
    #   适合：大多数 topic 用默认预设，个别 topic 需要精细控制。
    # ================================================================
    # apply_default_mocks(mock_mgr)  # 先全量注册预设
    #
    # # 用随机值覆盖电表（每次推理看到不同功率）
    # mock_mgr.register(
    #     "SCHEDULE-DEVICE-METER",
    #     provider=RandomMockProvider(
    #         template={"stationId": "{station_id}", "activePower": None,
    #                   "reactivePower": None, "totalActiveEnergy": None},
    #         ranges={"activePower": (-500.0, 500.0),
    #                 "reactivePower": (0.0, 60.0),
    #                 "totalActiveEnergy": (5000.0, 20000.0)},
    #     ),
    # )
    #
    # # 用序列覆盖订单，模拟 SOC 从 20 → 50 → 80 循环变化
    # mock_mgr.register(
    #     "SCHEDULE-CAR-ORDER",
    #     provider=SequenceMockProvider([
    #         {"stationId": "{station_id}", "gunNo": "01", "hostCode": "MOCK_HOST_001",
    #          "soc": 20.0, "outputPower": 200.0, "batteryNominalTotalCapacity": 80.0,
    #          "transactionSerialNo": "MOCK_ORDER_001",
    #          "startChargeTime": "2026-02-25 06:00:00", "endChargeTime": "2070-01-01 00:00:00",
    #          "terminalRequireElectric": 80.0, "terminalRequireVoltage": 750.0,
    #          "terminalMaxOutElectric": 200.0, "carProducerCode": "0000000000000000"},
    #         {"stationId": "{station_id}", "gunNo": "01", "hostCode": "MOCK_HOST_001",
    #          "soc": 50.0, "outputPower": 160.0, "batteryNominalTotalCapacity": 80.0,
    #          "transactionSerialNo": "MOCK_ORDER_001",
    #          "startChargeTime": "2026-02-25 06:00:00", "endChargeTime": "2070-01-01 00:00:00",
    #          "terminalRequireElectric": 80.0, "terminalRequireVoltage": 750.0,
    #          "terminalMaxOutElectric": 200.0, "carProducerCode": "0000000000000000"},
    #         {"stationId": "{station_id}", "gunNo": "01", "hostCode": "MOCK_HOST_001",
    #          "soc": 80.0, "outputPower": 80.0, "batteryNominalTotalCapacity": 80.0,
    #          "transactionSerialNo": "MOCK_ORDER_001",
    #          "startChargeTime": "2026-02-25 06:00:00", "endChargeTime": "2070-01-01 00:00:00",
    #          "terminalRequireElectric": 80.0, "terminalRequireVoltage": 750.0,
    #          "terminalMaxOutElectric": 200.0, "carProducerCode": "0000000000000000"},
    #     ], loop=True),
    # )
    #
    # # 用 lambda 覆盖储能，每次注入时动态计算 SOC（例：随时间线性上升）
    # mock_mgr.register(
    #     "SCHEDULE-DEVICE-STORAGE",
    #     provider=MultiDeviceMockProvider([
    #         CallableMockProvider(lambda sid, _: {
    #             "stationId": sid, "storageCode": "MOCK_STORAGE_001",
    #             "hostCode": "MOCK_HOST_001",
    #             "batteryGroupSoc": min(100, int((time.time() % 3600) / 36)),  # 0-100循环
    #             "batteryGroupSoh": 95, "batteryGroupElectric": 10.0,
    #             "batteryGroupOutPower": 5.0,
    #             "maxTemperature": 35.0, "minTemperature": 10.0,
    #         }),
    #     ]),
    # )

    # ================================================================
    # 场景 G 纯自定义（不用任何预设，完全手写）
    #   适合：只需要 2-3 个 topic，数据内容需要精细控制。
    #   启动命令：MOCK_DATA=1 MOCK_FORCE=1 MOCK_STATIONS=station_001 python main.py
    # ================================================================
    # mock_mgr.register(
    #     "SCHEDULE-STATION-PARAM",
    #     provider=StaticMockProvider({
    #         "stationId": "{station_id}", "stationName": "测试场站",
    #         "gridCapacity": 500.0, "gunNum": 8, "powerNum": 4,
    #         "hostCode": ["HOST_001", "HOST_002"],
    #         "stationLat": 31.23, "stationLng": 121.47,
    #     }),
    # )
    # mock_mgr.register(
    #     "SCHEDULE-DEVICE-STORAGE",
    #     provider=MultiDeviceMockProvider([
    #         StaticMockProvider({
    #             "stationId": "{station_id}", "storageCode": "STORAGE_001",
    #             "hostCode": "HOST_001", "batteryGroupSoc": 85,
    #             "batteryGroupSoh": 97, "batteryGroupElectric": 12.0,
    #             "batteryGroupOutPower": 6.0,
    #             "maxTemperature": 32.0, "minTemperature": 18.0,
    #         }),
    #     ]),
    # )
    # mock_mgr.register(
    #     "SCHEDULE-CAR-PRICE",
    #     provider=StaticMockProvider([{
    #         "stationId": "{station_id}", "sendTime": "2026-02-25 00:00:00",
    #         "peakElectricFee": 0.85, "flatElectricFee": 0.60,
    #         "sharpElectricFee": 1.10, "ebbElectricFee": 0.32,
    #         "peakServerFee": 0.0, "flatServerFee": 0.0,
    #         "sharpServerFee": 0.0, "ebbServerFee": 0.0,
    #         **{f"feeNo{i}": ("03" if i <= 16 or i > 44
    #                          else "01" if i <= 22 or 28 < i <= 40
    #                          else "02") for i in range(1, 49)},
    #     }]),
    # )

    logging.info(
        f"[MockData] 已注册 {len(mock_mgr.get_registry_info())} 条虚拟数据规则，"
        f"topics: {[r['topic'] for r in mock_mgr.get_registry_info()]}"
    )
    return mock_mgr


async def mock_auto_inject_loop(mock_mgr, service, stop_event, interval: float = 3.0):
    """
    虚拟数据后台补全任务（职责已收窄）。

    新场站的首次补全由 make_station_init_hook 在 _station_worker 启动前同步完成，
    彻底消除时间窗口竞争，本任务只负责以下两种情况：

    - 非 force 模式：启动后 1s 做一次兜底扫描（覆盖钩子注册前已存在的场站），
      完成后退出，不再轮询。
    - force 模式：持续按 interval 周期覆盖被 Kafka 新消息刷掉的 topic。

    Args:
        mock_mgr: MockDataManager 实例
        service: AsyncDataAnalysisService 实例
        stop_event: asyncio.Event，触发后退出循环
        interval: force 模式刷新周期（秒），默认 3 秒
    """
    if not MOCK_FORCE:
        # 非 force 模式：只做一次兜底扫描后退出
        logging.info("[MockData] 非 force 模式，1s 后执行一次兜底扫描")
        try:
            await asyncio.wait_for(asyncio.shield(stop_event.wait()), timeout=1.0)
            return  # stop_event 已触发，直接退出
        except asyncio.TimeoutError:
            pass
        try:
            for station_id in list(service.dispatcher.data_cache.keys()):
                results = mock_mgr.inject_all(station_id, force=False)
                injected = {t for t, ok in results.items() if ok}
                if injected:
                    logging.info(f"[MockData] 兜底补全 station={station_id}: {injected}")
        except Exception as exc:
            logging.warning(f"[MockData] 兜底扫描异常: {exc}", exc_info=True)
        logging.info("[MockData] 兜底扫描完成，后台任务退出")
        return

    # force 模式：持续覆盖 Kafka 刷入的数据
    logging.info(f"[MockData] force 模式后台刷新已启动，周期 {interval}s")
    while not stop_event.is_set():
        try:
            for station_id in list(service.dispatcher.data_cache.keys()):
                results = mock_mgr.inject_all(station_id, force=True)
                injected = {t for t, ok in results.items() if ok}
                if injected:
                    logging.debug(
                        f"[MockData] force 刷新 station={station_id}: {injected}"
                    )
        except Exception as exc:
            logging.warning(f"[MockData] force 刷新异常: {exc}", exc_info=True)

        try:
            await asyncio.wait_for(
                asyncio.shield(stop_event.wait()), timeout=interval
            )
            break  # stop_event 已触发
        except asyncio.TimeoutError:
            pass  # 正常超时，继续下一轮


def mock_startup_inject(mock_mgr):
    """
    服务启动后立即触发的一次性注入（对已知场站主动写入 dispatcher）。

    此函数与 build_mock_manager 配合使用，后者只完成"注册（WHAT）"，
    本函数决定"对哪些场站、哪些 topic 立即注入（WHEN + WHO）"。

    ┌────────────────────────────────────────────────────────────────┐
    │  触发时机对比                                                   │
    │  mock_startup_inject   ← 启动时对已知/指定场站主动写入          │
    │  make_station_init_hook← Kafka 发现新场站时自动触发（零窗口）   │
    │  mock_auto_inject_loop ← force 模式持续刷新 / 兜底扫描         │
    └────────────────────────────────────────────────────────────────┘

    根据 build_mock_manager 中启用的场景，取消对应注入块的注释即可。

    Args:
        mock_mgr: 已完成注册的 MockDataManager 实例
    """
    # ================================================================
    # 对应场景 A / B：全局注册，无需指定场站，依赖钩子和兜底任务自动注入。
    #   → 此处不需要任何调用，新场站由 make_station_init_hook 自动处理。
    # ================================================================
    # pass

    # ================================================================
    # 对应场景 C / D：精确注入指定场站（立即写入，不等 Kafka 消息）
    #   → 适合场站 ID 已知且需要在服务启动瞬间就有数据的情况。
    # ================================================================
    # target_stations = ["station_C", "station_D"]  # ← 改为你的场站 ID
    # for sid in target_stations:
    #     results = mock_mgr.inject_all(sid, force=MOCK_FORCE)
    #     ok = [t for t, v in results.items() if v]
    #     logging.info(f"[MockData] 启动注入 {sid}: {ok}")

    # ================================================================
    # 对应场景 C（双向精确）：只注入指定 topic，不注入其他
    # ================================================================
    inject_map = {
        "CN00151807": ["SCHEDULE-CAR-PRICE"],
        "CN00198099": ["SCHEDULE-CAR-PRICE"],
        "CN00235802": ["SCHEDULE-CAR-PRICE"],
    }
    for sid, topics in inject_map.items():
        for topic in topics:
            ok = mock_mgr.inject(sid, topic, force=MOCK_FORCE)
            logging.info(f"[MockData] 启动注入 {sid}/{topic}: {ok}")

    # ================================================================
    # 对应场景 E / F / G：全局注册后立即对所有已知场站做一次全量注入
    #   → 适合纯本地测试，dispatcher 启动时场站已通过其他途径加载。
    #   ⚠️ 若场站尚未出现在 dispatcher.data_cache，此处无效，
    #      需依赖 make_station_init_hook 在场站首次出现时自动补全。
    # ================================================================
    # results = mock_mgr.inject_all_stations(force=MOCK_FORCE)
    # for sid, topics_result in results.items():
    #     ok = [t for t, v in topics_result.items() if v]
    #     if ok:
    #         logging.info(f"[MockData] 启动全量注入 {sid}: {ok}")


def make_station_init_hook(mock_mgr):
    """
    生成场站初始化钩子。

    返回的协程会在 _station_worker 启动前被 _create_station_task 调用，
    确保虚拟数据在 worker 第一次 get_module_input 之前已就位，
    彻底消除轮询补全的时间窗口竞争。

    Args:
        mock_mgr: MockDataManager 实例

    Returns:
        async callable(station_id: str)
    """
    async def _hook(station_id: str):
        results = mock_mgr.inject_all(station_id, force=MOCK_FORCE)
        injected = {t for t, ok in results.items() if ok}
        if injected:
            logging.info(
                f"[MockData] 初始化钩子已触发 station={station_id}，"
                f"注入 {len(injected)} 个 topic: {injected}"
            )
        else:
            logging.debug(f"[MockData] 初始化钩子 station={station_id}，无需注入")

    return _hook
