#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-28 00:00:00
project: data_analysis
filename: model_output.py
version: 1.0
"""

from typing import Optional

from ..parser_base import ParserBase


class ModelOutputParser(ParserBase):
    """薄包装模型输出，用于将Kafka中其它模块的推理结果直接注入依赖模块。"""

    def __init__(self, module_name: Optional[str] = None):
        self.module_name = module_name or "unknown"

    def parse(self, raw_data):
        """保持原始结构，必要时增加来源标记。"""
        if raw_data is None:
            return {}
        if isinstance(raw_data, dict):
            # 返回浅拷贝，避免共享引用被下游修改
            return dict(raw_data)
        return {
            "value": raw_data,
            "source_module": self.module_name,
        }
