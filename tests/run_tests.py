#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
便捷的测试运行脚本
可以从任何位置运行测试而无需担心导入路径问题
"""

import sys
from pathlib import Path

# 确保项目根目录在Python路径中
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import asyncio
import argparse


def main():
    parser = argparse.ArgumentParser(description='运行Kafka相关测试')
    parser.add_argument(
        'test_type',
        choices=['consume', 'produce', 'quick', 'module', 'connectivity'],
        help='测试类型: consume(消费测试), produce(生产测试), quick(快速测试), module(模块测试), connectivity(连接测试)'
    )
    parser.add_argument(
        '--module',
        default='load_prediction',
        help='指定测试的模块名（仅用于module测试）'
    )
    parser.add_argument(
        '--duration',
        type=int,
        default=60,
        help='测试运行时长（秒）'
    )
    parser.add_argument(
        '--timeout',
        type=int,
        default=20,
        help='每个topic的超时时间（秒）'
    )
    
    args = parser.parse_args()
    
    if args.test_type == 'consume':
        from tests.test_kafka_consume import main as consume_main
        print("运行Kafka消费完整测试...")
        asyncio.run(consume_main())
        
    elif args.test_type == 'produce':
        from tests.test_mock_producer import main as produce_main
        print(f"运行模拟生产者（{args.duration}秒）...")
        asyncio.run(produce_main())
        
    elif args.test_type == 'quick':
        from tests.test_kafka_consume import quick_test
        print(f"运行快速测试（超时{args.timeout}秒）...")
        asyncio.run(quick_test(timeout=args.timeout))
        
    elif args.test_type == 'module':
        from tests.test_kafka_consume import test_module_topics
        print(f"测试模块: {args.module}")
        asyncio.run(test_module_topics(args.module))
        
    elif args.test_type == 'connectivity':
        from tests.test_kafka_consume import test_kafka_connectivity
        print("测试Kafka连接性...")
        result = asyncio.run(test_kafka_connectivity())
        if result:
            print("\n✓ Kafka连接成功！")
        else:
            print("\n✗ Kafka连接失败！")
            sys.exit(1)


if __name__ == "__main__":
    main()
