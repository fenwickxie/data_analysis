#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Common exceptions and error-handling utilities for the data_analysis package.
"""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
import traceback
from typing import Callable, Optional, Any

LOG_FORMAT = "[%(asctime)s] %(levelname)s %(name)s %(message)s"
_LOGGER_CONFIGURED = False


def _ensure_logging() -> None:
    """
    Configure root logging once with stream + rotating file handlers.
    
    日志配置从 config.yaml 读取：
    - log_file: 日志文件路径
    - max_bytes: 单个日志文件最大大小
    - backup_count: 保留的备份文件数量
    """
    global _LOGGER_CONFIGURED
    if _LOGGER_CONFIGURED:
        return
    
    # 从配置加载日志设置
    try:
        from .config import LOGGING_CONFIG
        log_file = LOGGING_CONFIG['log_file']
        max_bytes = LOGGING_CONFIG['max_bytes']
        backup_count = LOGGING_CONFIG['backup_count']
    except (ImportError, KeyError):
        # 配置加载失败时使用默认值
        log_file = "data_analysis.log"
        max_bytes = 10 * 1024 * 1024  # 10MB
        backup_count = 5
    
    handler_stream = logging.StreamHandler()
    # 使用 RotatingFileHandler
    handler_file = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8"
    )
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        handlers=[handler_stream, handler_file],
    )
    _LOGGER_CONFIGURED = True


_ensure_logging()


class DataAnalysisError(Exception):
    """Base exception for data analysis service errors."""


class KafkaConnectionError(DataAnalysisError):
    """Kafka connectivity errors."""


class DispatcherError(DataAnalysisError):
    """Dispatcher-related failures."""


def handle_error(
    exc: BaseException,
    *,
    context: Optional[str] = None,
    recover: Optional[Callable[[], Any]] = None,
) -> None:
    """Log the error with optional recovery callback."""
    _ensure_logging()
    message = f"[统一错误处理] {type(exc).__name__}: {exc}\n{traceback.format_exc()}"
    if context:
        message += f"\n[上下文] {context}"
    logging.error(message)
    if recover is None:
        return
    try:
        recover()
    except Exception as recover_exc:  # pragma: no cover - defensive logging
        logging.error(
            "[恢复失败] %s: %s\n%s",
            type(recover_exc).__name__,
            recover_exc,
            traceback.format_exc(),
        )


__all__ = [
    "DataAnalysisError",
    "KafkaConnectionError",
    "DispatcherError",
    "handle_error",
]
