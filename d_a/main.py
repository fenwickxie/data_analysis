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
import sys
from pathlib import Path

if __package__ in (None, ""):
    # Allow running via `python d_a/main.py` by adding the project root.
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    from d_a.analysis_service import AsyncDataAnalysisService
else:
    from .analysis_service import AsyncDataAnalysisService


async def my_model_predict(module_input):
    # 这里写模型推理逻辑
    await asyncio.sleep(0.01)  # 模拟异步推理
    return {"result": 123}


async def my_callback(station_id, module_input):
    # module_input为本模型所需结构化输入（含窗口补全/插值、依赖聚合）
    result = await my_model_predict(module_input)
    return result  # 返回值会自动上传到Kafka


async def main():
    service = AsyncDataAnalysisService(module_name="load_prediction")
    await service.start(callback=my_callback)
    try:
        for _ in range(10):  # 模拟主循环10次
            print(service.get_station_status())
            await asyncio.sleep(1)
    finally:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())
