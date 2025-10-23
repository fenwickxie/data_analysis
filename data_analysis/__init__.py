#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:09:25
project: data_analysis
filename: __init__.py
version: 1.0
"""

import logging
import traceback

# 统一异常定义
class DataAnalysisError(Exception):
    pass


class KafkaConnectionError(DataAnalysisError):
    pass


class DispatcherError(DataAnalysisError):
    pass


# 统一错误处理器
def handle_error(e, context=None, recover=None):
    msg = f"[统一错误处理] {type(e).__name__}: {e}\n{traceback.format_exc()}"
    if context:
        msg += f"\n[上下文] {context}"
    logging.error(msg)
    if recover:
        try:
            recover()
        except Exception as e2:
            logging.error(
                f"[恢复失败] {type(e2).__name__}: {e2}\n{traceback.format_exc()}"
            )


# 日志支持多级别和文件输出
LOG_FORMAT = "[%(asctime)s] %(levelname)s %(name)s %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("data_analysis.log", encoding="utf-8"),
    ],
)

# 延迟导入避免循环依赖
from .analysis_service import DataAnalysisService, AsyncDataAnalysisService

__all__ = [
    'DataAnalysisService',
    'AsyncDataAnalysisService',
    'DataAnalysisError',
    'KafkaConnectionError',
    'DispatcherError',
    'handle_error',
]
