"""
测试辅助工具包
包含重构后的测试基础类和工具
"""

from tests.fixtures.data_generator_base import (
    WindowConfig,
    WindowDataBuilder,
    DataGenerationStrategy,
    DataGeneratorFactory,
)

from tests.fixtures.test_utils import (
    TopicTesterTemplate,
    AsyncTopicTester,
    SyncTopicTester,
    TestReportFormatter,
)

__all__ = [
    # 数据生成
    'WindowConfig',
    'WindowDataBuilder',
    'DataGenerationStrategy',
    'DataGeneratorFactory',
    
    # 测试工具
    'TopicTesterTemplate',
    'AsyncTopicTester',
    'SyncTopicTester',
    'TestReportFormatter',
]
