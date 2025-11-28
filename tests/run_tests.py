#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
author: xie.fangyu
date: 2025-11-24
project: data_analysis
filename: run_tests.py
version: 2.0
description: 测试运行脚本
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

import pytest
from d_a.config import KAFKA_CONFIG, TOPIC_TO_MODULES
from d_a.kafka_client import AsyncKafkaConsumerClient

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
        print("运行Kafka连接性测试...")
        consumer = AsyncKafkaConsumerClient(
            list(TOPIC_TO_MODULES.keys()),
            KAFKA_CONFIG,
            []
        )
        try:
            await consumer.start()
            topics = consumer.topics
            print(f"\n✓ Kafka连接成功！发现 {len(topics)} 个 topics。")
            return 0
        except Exception as e:
            print(f"\n✗ Kafka连接失败: {e}")
            return 1
        finally:
            if consumer.consumer:
                await consumer.stop()

    def run_pytest(self):
        """使用 Pytest 运行测试"""
        pytest_args = []
        if self.args.module:
            # 查找匹配模块名的测试文件
            module_path = project_root / "tests" / f"test_{self.args.module}.py"
            if module_path.exists():
                pytest_args.append(str(module_path))
            else:
                print(f"错误: 找不到模块 '{self.args.module}' 对应的测试文件 '{module_path}'")
                return 1
        elif self.args.all:
            pytest_args.append(str(project_root / "tests"))
        
        if not pytest_args:
            print("错误: 没有指定要运行的测试。请使用 --all 或 --module。")
            return 1
        
        # 添加报告生成参数
        if self.args.report:
            report_dir = project_root / "test_reports"
            report_dir.mkdir(exist_ok=True)
            
            # HTML报告
            html_report = report_dir / "test_report.html"
            pytest_args.extend([
                f"--html={html_report}",
                "--self-contained-html"
            ])
            
            # JUnit XML报告
            xml_report = report_dir / "test_report.xml"
            pytest_args.append(f"--junitxml={xml_report}")
            
            print(f"测试报告将生成到: {report_dir}")
        
        # 添加覆盖率报告参数
        if self.args.coverage:
            report_dir = project_root / "test_reports"
            report_dir.mkdir(exist_ok=True)
            cov_html_dir = report_dir / "htmlcov"
            
            pytest_args.extend([
                "--cov=d_a",
                f"--cov-report=html:{cov_html_dir}",
                "--cov-report=term"
            ])
            
            print(f"覆盖率报告将生成到: {cov_html_dir}")
            
        print(f"运行 pytest 命令: pytest {' '.join(pytest_args)}")
        result = pytest.main(pytest_args)
        
        # 打印报告位置
        if self.args.report or self.args.coverage:
            print("\n" + "="*80)
            print("测试报告已生成:")
            if self.args.report:
                print(f"  • HTML报告: {report_dir / 'test_report.html'}")
                print(f"  • XML报告:  {report_dir / 'test_report.xml'}")
            if self.args.coverage:
                print(f"  • 覆盖率报告: {report_dir / 'htmlcov' / 'index.html'}")
            print("="*80)
        
        return result

    async def run(self):
        """执行测试"""
        if self.args.test_type == 'connectivity':
            return await self.run_connectivity_test()
        elif self.args.test_type == 'test':
            # pytest不需要在async中运行,直接返回
            return self.run_pytest()
        else:
            print(f"未知的测试类型: {self.args.test_type}")
            return 1


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='Data Analysis 测试工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python tests/run_tests.py connectivity                    # 测试Kafka连接
  python tests/run_tests.py test --all                      # 运行所有测试
  python tests/run_tests.py test --all --report             # 运行测试并生成HTML+XML报告
  python tests/run_tests.py test --all --coverage           # 运行测试并生成覆盖率报告
  python tests/run_tests.py test --all --report --coverage  # 生成所有报告
  python tests/run_tests.py test --module service           # 运行service模块的测试
        """
    )
    
    parser.add_argument(
        'test_type',
        choices=['connectivity', 'test'],
        help='要执行的操作类型'
    )
    
    parser.add_argument(
        '--all',
        action='store_true',
        help='运行所有测试 (用于 "test" 类型)'
    )
    
    parser.add_argument(
        '--module',
        help='指定要测试的模块名 (例如: service, dispatcher)'
    )
    
    parser.add_argument(
        '--report',
        action='store_true',
        help='生成测试报告 (HTML和JUnit XML格式)'
    )
    
    parser.add_argument(
        '--coverage',
        action='store_true',
        help='生成代码覆盖率报告'
    )
    
    args = parser.parse_args()
    
    if args.test_type == 'test' and not args.all and not args.module:
        parser.error("--all 或 --module 参数在 'test' 类型下必须至少指定一个。")

    runner = TestRunner(args)
    # 如果是connectivity测试,使用asyncio.run; 如果是pytest,直接运行
    if args.test_type == 'connectivity':
        exit_code = asyncio.run(runner.run())
    else:
        exit_code = runner.run_pytest()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
