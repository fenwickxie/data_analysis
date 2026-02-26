#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:09:25
project: data_analysis
filename: __init__.py
version: 1.0
"""

__version__ = "2.0.0"
__author__ = "xie.fangyu"
__email__ = "xie.fangyu@byd.com"
__url__ = "https://devops.byd.com/ETDCDISD/chargex-flagship-dispatch/data_analysis.git"
__description__ = "Data Analysis Service"

from .analysis_service import DataAnalysisService, AsyncDataAnalysisService
from .errors import (
    DataAnalysisError,
    KafkaConnectionError,
    DispatcherError,
    handle_error,
)

# ---- 测试工具（仅用于测试环境，勿在生产环境中使用）----
from .mock import (
    MockDataManager,
    MockProvider,
    StaticMockProvider,
    RandomMockProvider,
    CallableMockProvider,
    SequenceMockProvider,
    MultiDeviceMockProvider,
)
from .mock import (
    apply_default_mocks,
    build_preset_provider,
    list_preset_topics,
)

__all__ = [
    # 核心服务
    "DataAnalysisService",
    "AsyncDataAnalysisService",
    # 异常与错误处理
    "DataAnalysisError",
    "KafkaConnectionError",
    "DispatcherError",
    "handle_error",
    # 测试工具 - 虚拟数据注入
    "MockDataManager",
    "MockProvider",
    "StaticMockProvider",
    "RandomMockProvider",
    "CallableMockProvider",
    "SequenceMockProvider",
    "MultiDeviceMockProvider",
    "apply_default_mocks",
    "build_preset_provider",
    "list_preset_topics",
]
