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
