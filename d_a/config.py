#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-10-16 11:09:43
project: data_analysis
filename: config.py
version: 2.0
description: 配置加载器 - 从YAML文件加载配置
"""

import os
import yaml
from pathlib import Path


def load_config(config_path=None):
    """
    从YAML文件加载配置
    
    查找优先级:
    1. 显式指定的config_path参数
    2. 环境变量 DATA_ANALYSIS_CONFIG_PATH
    3. 当前工作目录下的config.yaml
    4. 包自带的默认配置（开发环境）
    
    Args:
        config_path: 配置文件路径（可选）
        
    Returns:
        dict: 配置字典
        
    Raises:
        FileNotFoundError: 找不到配置文件时抛出
    """
    if config_path is None:
        # 优先级1: 检查环境变量
        config_path = os.environ.get('DATA_ANALYSIS_CONFIG_PATH')
        
        if config_path is None:
            # 优先级2: 检查当前工作目录（适用于wheel安装场景）
            cwd_config = Path.cwd() / 'config.yaml'
            if cwd_config.exists():
                config_path = cwd_config
            else:
                # 优先级3: 使用包自带的默认配置（开发环境）
                package_dir = Path(__file__).parent.parent
                package_config = package_dir / 'config.yaml'
                if package_config.exists():
                    config_path = package_config
                else:
                    raise FileNotFoundError(
                        f"找不到配置文件。请在以下位置之一提供config.yaml:\n"
                        f"  1. 通过参数指定: load_config('path/to/config.yaml')\n"
                        f"  2. 设置环境变量: DATA_ANALYSIS_CONFIG_PATH\n"
                        f"  3. 当前工作目录: {cwd_config}\n"
                        f"  4. 包目录: {package_config}"
                    )
    
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"配置文件不存在: {config_path}")
    
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    return config


def _build_module_output_topics(config):
    """根据配置动态构建模块输出topic的完整配置"""
    module_dependencies = config['module_dependencies']
    module_output_topics = config['module_output']['topics']
    module_output_window_size = config['module_output']['window_size']
    topic_detail = config['topic_detail']
    
    # 为每个模块输出topic添加到topic_detail
    for module_name, output_topic in module_output_topics.items():
        consumers = [
            consumer
            for consumer, deps in module_dependencies.items()
            if module_name in deps
        ]
        topic_detail[output_topic] = {
            'fields': [],
            'frequency': '模型输出',
            'modules': consumers,
            'window_size': module_output_window_size,
        }


def _build_topic_mappings(config):
    """构建topic和模块之间的映射关系"""
    topic_detail = config['topic_detail']
    module_output_topics = config['module_output']['topics']
    
    # topic到模块的多对多映射
    topic_to_modules = {topic: v['modules'] for topic, v in topic_detail.items()}
    
    # 模块到topic的多对多映射
    module_to_topics = {}
    for topic, v in topic_detail.items():
        for m in v['modules']:
            module_to_topics.setdefault(m, []).append(topic)
    
    # 确保所有模块在映射中至少有空列表,便于后续查找
    for module_name in module_output_topics.keys():
        module_to_topics.setdefault(module_name, [])
    
    return topic_to_modules, module_to_topics


# 加载配置
_config = load_config()

# 构建模块输出topic配置
_build_module_output_topics(_config)

# 构建映射关系
TOPIC_TO_MODULES, MODULE_TO_TOPICS = _build_topic_mappings(_config)

# 导出配置变量（保持向后兼容）
KAFKA_CONFIG = _config['kafka']
OFFSET_COMMIT_CONFIG = _config['offset_commit']
TOPIC_DETAIL = _config['topic_detail']
MODULE_DEPENDENCIES = _config['module_dependencies']
MODULE_OUTPUT_TOPIC_PREFIX = _config['module_output']['topic_prefix']
MODULE_OUTPUT_TOPICS = _config['module_output']['topics']
MODULE_OUTPUT_WINDOW_SIZE = _config['module_output']['window_size']
MODULE_NAME = _config.get('module_name', 'load_prediction')  # 默认为load_prediction

# 日志配置
_logging_config = _config.get('logging', {})
LOGGING_CONFIG = {
    'log_file': _logging_config.get('log_file', 'data_analysis.log'),
    'max_bytes': _logging_config.get('max_bytes', 10 * 1024 * 1024),  # 默认10MB
    'backup_count': _logging_config.get('backup_count', 5)  # 默认5个备份
}
