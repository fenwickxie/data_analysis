#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:10:17
project: data_analysis
filename: parser_base.py
version: 1.0
"""

# 解析器基类
from abc import ABC, abstractmethod

class ParserBase(ABC):
    @abstractmethod
    def parse(self, raw_data):
        """
        解析原始数据，返回结构化数据
        :param raw_data: dict
        :return: dict
        """
        pass
