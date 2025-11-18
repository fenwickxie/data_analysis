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

#
from __future__ import annotations

import asyncio
import logging
import signal
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


# 模型封装入口范例（异步）
async def my_model_predict(module_input):
    print(module_input)
    return {"result": 123}


async def my_callback(station_id, module_input):
    """
    单场站处理回调

    Args:
        station_id: 场站ID
        module_input: 模块输入数据

    Returns:
        dict: 必须包含 'station_id' 字段的结果字典
        None: 处理失败
    """
    try:
        result = await my_model_predict(module_input)
        # 在结果中添加 station_id
        result["station_id"] = station_id
        return result
    except Exception as e:
        logging.error(f"场站 {station_id} 处理失败: {e}")
        return None  # 返回None表示失败，不会阻塞批次上传


async def main():
    output_topic = MODULE_OUTPUT_TOPICS.get(MODULE_NAME)
    if not output_topic:
        raise RuntimeError(f"未配置模块 {MODULE_NAME} 的输出topic")

    producer = AsyncKafkaProducerClient(KAFKA_CONFIG)
    await producer.start()

    service = AsyncDataAnalysisService(module_name=MODULE_NAME)

    async def batch_upload_handler(batch_id, results_list):
        """
        批次上传回调

        Args:
            batch_id: 批次ID
            results_list: 成功场站的结果列表，每个元素是包含 station_id 的字典
                [
                    {"station_id": "S001", "result": 123, ...},
                    {"station_id": "S002", "result": 456, ...},
                    ...
                ]
        """
        if not results_list:
            logging.warning(f"批次 {batch_id} 没有成功的结果，跳过上传")
            return

        payload = {
            "batch_id": batch_id,
            "module": MODULE_NAME,
            "timestamp": time.time(),
            "stations_count": len(results_list),
            "results": results_list,  # 所有场站的结果列表
        }

        try:
            await producer.send(output_topic, payload)
            logging.info(f"批次上传成功: {batch_id}, {len(results_list)} 个场站")
        except Exception as exc:
            logging.error(f"批次上传失败: {batch_id}", exc_info=exc)

    await service.start(callback=my_callback, batch_upload_handler=batch_upload_handler)

    try:
        print("服务已启动，按 Ctrl+C 停止...")
        while True:
            status = service.get_station_status()
            print(status)
            # 使用多个短暂的 sleep 以提高响应性
            for _ in range(10):
                await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError) as e:
        print(f"\n程序被用户中断 ({type(e).__name__})，正在清理资源...")
        logging.info(f"收到停止信号: {type(e).__name__}，开始停机...")
    finally:
        print("正在停止服务...")
        # 停止服务（会自动提交所有pending offsets）
        await service.stop()
        print("正在停止生产者...")
        await producer.stop()
        logging.info("服务已停止")
        print("清理完成")


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\n捕获到键盘中断")
    finally:
        print("关闭事件循环...")
        loop.close()
        print("程序已退出")
