#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Common exceptions and error-handling utilities for the data_analysis package.
"""

from __future__ import annotations

import logging
import traceback
from typing import Callable, Optional, Any

LOG_FORMAT = "[%(asctime)s] %(levelname)s %(name)s %(message)s"
_LOGGER_CONFIGURED = False


def _ensure_logging() -> None:
    """Configure root logging once with stream + file handlers."""
    global _LOGGER_CONFIGURED
    if _LOGGER_CONFIGURED:
        return
    handler_stream = logging.StreamHandler()
    handler_file = logging.FileHandler("data_analysis.log", encoding="utf-8")
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
