#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:10:17
project: data_analysis
filename: parser_base.py
version: 1.0
"""

from typing import cast

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
    
    def parse_window(self, window_data):
        """
        解析窗口数据（时序数据列表）
        子类可以重写此方法实现自定义的窗口数据处理逻辑，如：
        - 枪号对齐
        - 数据插值
        - 特征工程
        
        默认实现：简单地对每条数据调用parse()，返回列表
        
        Args:
            window_data: 窗口内的原始数据列表 [raw_data1, raw_data2, ...]
            
        Returns:
            dict: 处理后的窗口数据，格式为 {field: [value1, value2, ...], ...}
        """
        # 默认实现：简单拼接
        result = {}
        
        for raw_data in window_data:
            parsed = self.parse(raw_data)
            
            # 类型检查：确保parsed是字典类型
            if not parsed:
                continue
            
            # 使用cast明确类型
            for key, value in cast(dict, parsed).items():
                if key not in result:
                    result[key] = []
                result[key].append(value)
        
        return result


class ConfigBasedParser(ParserBase):
    """
    基于配置文件自动解析的通用解析器
    根据 config.TOPIC_DETAIL 中的 fields 配置自动提取字段
    """
    
    def __init__(self, topic_name, config_module=None):
        """
        初始化解析器
        
        Args:
            topic_name: Topic名称，对应config.TOPIC_DETAIL的key
            config_module: 配置模块，默认为None时会导入d_a.config
        """
        self.topic_name = topic_name
        
        if config_module is None:
            from . import config
            config_module = config
        
        self.config = config_module
        
        # 获取该topic的字段配置
        topic_config = self.config.TOPIC_DETAIL.get(topic_name, {})
        self.fields = topic_config.get('fields', [])
        
        if not self.fields:
            raise ValueError(f"Topic '{topic_name}' 未在config.TOPIC_DETAIL中配置fields")
    
    def parse(self, raw_data):
        """
        根据配置的fields自动提取字段
        
        Args:
            raw_data: Kafka消息原始数据（JSON已解析为dict）
            
        Returns:
            dict: 提取的结构化数据，key为camelCase字段名
        """
        if not raw_data:
            return None
        
        parsed_data = {}
        for field in self.fields:
            # 直接使用配置中的字段名（camelCase），保持与Kafka消息一致
            parsed_data[field] = raw_data.get(field)
        
        return parsed_data
    
    def parse_window(self, window_data):
        """
        ConfigBasedParser 的窗口解析实现
        
        对于大多数配置型数据（window_size=1），只需要最新的数据
        对于时序型数据（window_size>1），使用基类的默认实现
        
        Args:
            window_data: 窗口内的原始数据列表
            
        Returns:
            dict: 解析后的数据
        """
        if not window_data:
            return {}
        
        # 如果窗口大小为1，直接返回最新数据的解析结果
        if len(window_data) == 1:
            return self.parse(window_data[0]) or {}
        
        # 窗口大小>1时，使用基类的默认实现（合并为列表）
        return super().parse_window(window_data)
