#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:10:10
project: data_analysis
filename: main.py
version: 1.0
"""


# # ===================== 使用示例 =====================
# from data_analysis.service import DataAnalysisService


# def my_model_predict(module_input):
#     # 这里写模型推理逻辑
#     return {"result": 123}  # 示例输出


# def my_callback(station_id, module_input):
#     # module_input为本模型所需结构化输入（含窗口补全/插值、依赖聚合）
#     result = my_model_predict(module_input)
#     return result  # 返回值会自动上传到Kafka


# if __name__ == "__main__":
#     # 指定本模型名，自动处理依赖、窗口、上传输出
#     service = DataAnalysisService(module_name="load_prediction")
#     service.start(callback=my_callback)
#     # ...业务主循环...
#     # 可随时获取健康状态
#     print(service.get_service_status())
#     print(service.get_station_status())
#     # 支持配置热更新
#     # service.reload_config()
#     service.stop()


# ===================== 异步示例 =====================
from __future__ import annotations

import asyncio
import logging
import sys
import time
from pathlib import Path

MODULE_NAME = "load_prediction"

if __package__ in (None, ""):
    # Allow running via `python d_a/main.py` by adding the project root.
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from d_a.analysis_service import AsyncDataAnalysisService
    from d_a.kafka_client import AsyncKafkaProducerClient
    from d_a.config import KAFKA_CONFIG, MODULE_OUTPUT_TOPICS
else:
    from .analysis_service import AsyncDataAnalysisService
    from .kafka_client import AsyncKafkaProducerClient
    from .config import KAFKA_CONFIG, MODULE_OUTPUT_TOPICS


async def my_model_predict(module_input):
    # 这里写模型推理逻辑
    await asyncio.sleep(0.01)  # 模拟异步推理
    return {"result": 123}


async def my_callback(station_id, module_input):
    # module_input为本模型所需结构化输入（含窗口补全/插值、依赖聚合）
    result = await my_model_predict(module_input)
    return result  # 返回值将交由result_handler处理


async def main():
    output_topic = MODULE_OUTPUT_TOPICS.get(MODULE_NAME)
    if not output_topic:
        raise RuntimeError(f"未配置模块 {MODULE_NAME} 的输出topic")

    producer = AsyncKafkaProducerClient(KAFKA_CONFIG)
    await producer.start()

    service = AsyncDataAnalysisService(module_name=MODULE_NAME)

    async def result_handler(station_id, module_input, result):
        if result is None:
            return
        payload = {
            "station_id": station_id,
            "module": MODULE_NAME,
            "result": result,
            "timestamp": time.time(),
        }
        try:
            await producer.send(output_topic, payload)
        except Exception as exc:  # noqa: BLE001
            logging.error("Kafka上传失败", exc_info=exc)

    await service.start(callback=my_callback, result_handler=result_handler)
    try:
        while True:  # 模拟主循环
            print(service.get_station_status())
            await asyncio.sleep(1)
    finally:
        await service.stop()
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
