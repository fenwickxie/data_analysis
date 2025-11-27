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

__all__ = [
    "DataAnalysisService",
    "AsyncDataAnalysisService",
    "DataAnalysisError",
    "KafkaConnectionError",
    "DispatcherError",
    "handle_error",
]
