#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-11-24
project: data_analysis
filename: run_tests.py
version: 2.0
description: 重构后的测试运行脚本
"""

import sys
from pathlib import Path

# 确保项目根目录在Python路径中
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import asyncio
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s - %(message)s"
)


class TestRunner:
    """测试运行器（封装测试逻辑）"""
    
    def __init__(self, args):
        self.args = args
    
    async def run_connectivity_test(self):
        """运行连接性测试"""
        from tests.test_kafka_consume_v2 import test_kafka_connectivity
        
        print("运行Kafka连接性测试...")
        result = await test_kafka_connectivity()
        
        if result:
            print("\n✓ Kafka连接成功！")
            return 0
        else:
            print("\n✗ Kafka连接失败！")
            return 1
    
    async def run_quick_test(self):
        """运行快速测试"""
        from tests.test_kafka_consume_v2 import quick_test
        
        print(f"运行快速测试（超时{self.args.timeout}秒）...")
        await quick_test(timeout=self.args.timeout)
        return 0
    
    async def run_consume_test(self):
        """运行完整消费测试"""
        from tests.test_kafka_consume_v2 import main as consume_main
        
        print("运行Kafka消费完整测试...")
        await consume_main()
        return 0
    
    async def run_module_test(self):
        """运行模块测试"""
        from tests.test_kafka_consume_v2 import test_module_topics
        
        print(f"测试模块: {self.args.module}")
        await test_module_topics(self.args.module)
        return 0
    
    async def run_produce_test(self):
        """运行生产者测试"""
        from tests.test_mock_producer_v2 import MockProducer
        
        print(f"运行模拟生产者（{self.args.duration}秒）...")
        
        producer = MockProducer()
        try:
            await producer.start()
            await producer.run_continuous(
                duration_seconds=self.args.duration,
                interval_seconds=self.args.interval
            )
        except KeyboardInterrupt:
            print("\n用户中断")
        except Exception as e:
            print(f"\n错误: {e}")
            return 1
        finally:
            await producer.stop()
        
        return 0
    
    async def run(self):
        """执行测试"""
        test_map = {
            'connectivity': self.run_connectivity_test,
            'quick': self.run_quick_test,
            'consume': self.run_consume_test,
            'module': self.run_module_test,
            'produce': self.run_produce_test,
        }
        
        handler = test_map.get(self.args.test_type)
        if not handler:
            print(f"未知的测试类型: {self.args.test_type}")
            return 1
        
        return await handler()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='Kafka测试工具集',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python run_tests.py connectivity                        # 测试连接
  python run_tests.py quick --timeout 15                  # 快速测试
  python run_tests.py consume                             # 完整测试
  python run_tests.py module --module load_prediction     # 模块测试
  python run_tests.py produce --duration 300 --interval 10 # 生产数据
        """
    )
    
    parser.add_argument(
        'test_type',
        choices=['connectivity', 'quick', 'consume', 'module', 'produce'],
        help='测试类型'
    )
    
    parser.add_argument(
        '--module',
        default='load_prediction',
        help='指定测试的模块名（用于module测试，默认: load_prediction）'
    )
    
    parser.add_argument(
        '--duration',
        type=int,
        default=60,
        help='运行时长（秒，用于produce测试，默认: 60）'
    )
    
    parser.add_argument(
        '--timeout',
        type=int,
        default=20,
        help='每个topic的超时时间（秒，用于consume测试，默认: 20）'
    )
    
    parser.add_argument(
        '--interval',
        type=int,
        default=10,
        help='发送间隔（秒，用于produce测试，默认: 10）'
    )
    
    args = parser.parse_args()
    
    runner = TestRunner(args)
    exit_code = asyncio.run(runner.run())
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
