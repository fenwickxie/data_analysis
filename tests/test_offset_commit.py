#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Offset提交功能快速测试

验证手动offset提交机制是否正常工作
"""

import asyncio
import logging
import time
from unittest.mock import Mock, AsyncMock, MagicMock
from collections import namedtuple

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Mock消息对象
MockMessage = namedtuple('MockMessage', ['topic', 'partition', 'offset', 'value'])


async def test_batch_commit():
    """测试批次触发提交"""
    print("\n=== 测试1: 批次触发提交 ===")
    
    from d_a import AsyncDataAnalysisService
    
    # 配置: 批次大小为5，时间间隔很长（不会触发）
    test_config = {
        'commit_interval_seconds': 9999,
        'commit_batch_size': 5,
        'max_commit_retries': 3,
        'commit_retry_delay': 0.1,
    }
    
    service = AsyncDataAnalysisService(
        offset_commit_config=test_config
    )
    
    # Mock consumer
    service.consumer.commit_offsets = AsyncMock(return_value=True)
    
    # 创建5条测试消息
    messages = [
        MockMessage('test-topic', 0, i, b'{"stationId": "S001", "data": "test"}')
        for i in range(5)
    ]
    
    # 处理消息
    for msg in messages:
        await service._process_and_track_message(msg)
    
    # 检查pending_offsets
    assert len(service._pending_offsets) == 5, f"应该有5个pending offsets，实际: {len(service._pending_offsets)}"
    
    # 触发提交（因为达到批次大小）
    if await service._should_commit_offsets():
        async with service._commit_lock:
            await service._commit_pending_offsets()
    
    # 验证提交
    assert len(service._pending_offsets) == 0, "提交后pending_offsets应该为空"
    assert service._processed_count == 0, "提交后processed_count应该重置为0"
    
    print("✅ 批次触发提交测试通过")


async def test_time_based_commit():
    """测试定时触发提交"""
    print("\n=== 测试2: 定时触发提交 ===")
    
    from d_a import AsyncDataAnalysisService
    
    # 配置: 批次大小很大（不会触发），时间间隔2秒
    test_config = {
        'commit_interval_seconds': 2.0,
        'commit_batch_size': 9999,
        'max_commit_retries': 3,
        'commit_retry_delay': 0.1,
    }
    
    service = AsyncDataAnalysisService(
        offset_commit_config=test_config
    )
    
    # Mock consumer
    service.consumer.commit_offsets = AsyncMock(return_value=True)
    
    # 处理3条消息
    messages = [
        MockMessage('test-topic', 0, i, b'{"stationId": "S001", "data": "test"}')
        for i in range(3)
    ]
    
    for msg in messages:
        await service._process_and_track_message(msg)
    
    assert len(service._pending_offsets) == 3, "应该有3个pending offsets"
    
    # 立即检查，不应触发提交
    should_commit_1 = await service._should_commit_offsets()
    assert not should_commit_1, "未达到时间间隔，不应触发提交"
    
    # 等待超过时间间隔
    await asyncio.sleep(2.5)
    
    # 再次检查，应该触发提交
    should_commit_2 = await service._should_commit_offsets()
    assert should_commit_2, "达到时间间隔，应该触发提交"
    
    # 执行提交
    async with service._commit_lock:
        await service._commit_pending_offsets()
    
    assert len(service._pending_offsets) == 0, "提交后pending_offsets应该为空"
    
    print("✅ 定时触发提交测试通过")


async def test_commit_retry():
    """测试提交重试机制"""
    print("\n=== 测试3: 提交重试机制 ===")
    
    from d_a import AsyncDataAnalysisService
    
    test_config = {
        'commit_interval_seconds': 5.0,
        'commit_batch_size': 10,
        'max_commit_retries': 3,
        'commit_retry_delay': 0.1,
    }
    
    service = AsyncDataAnalysisService(
        offset_commit_config=test_config
    )
    
    # Mock consumer - 前2次失败，第3次成功
    call_count = {'count': 0}
    
    async def mock_commit(offsets=None):
        call_count['count'] += 1
        if call_count['count'] < 3:
            return False  # 失败
        return True  # 成功
    
    service.consumer.commit_offsets = mock_commit
    
    # 添加一些pending offsets
    service._pending_offsets[('test-topic', 0)] = 100
    service._pending_offsets[('test-topic', 1)] = 200
    
    # 尝试提交（会重试）
    async with service._commit_lock:
        await service._commit_pending_offsets()
    
    assert call_count['count'] == 3, f"应该重试3次，实际: {call_count['count']}"
    assert len(service._pending_offsets) == 0, "第3次成功后应该清空"
    
    print("✅ 提交重试机制测试通过")


async def test_partial_failure():
    """测试部分消息失败"""
    print("\n=== 测试4: 部分消息失败处理 ===")
    
    from d_a import AsyncDataAnalysisService
    
    service = AsyncDataAnalysisService()
    
    # Mock dispatcher - 第2条和第4条消息失败
    fail_indices = {1, 3}
    call_index = {'count': 0}
    
    def mock_update(station_id, topic, value):
        idx = call_index['count']
        call_index['count'] += 1
        if idx in fail_indices:
            raise Exception("模拟处理失败")
    
    service.dispatcher.update_topic_data = mock_update
    
    # 创建5条消息
    messages = [
        MockMessage('test-topic', 0, i, b'{"stationId": "S001", "data": "test"}')
        for i in range(5)
    ]
    
    # 处理所有消息
    for msg in messages:
        await service._process_and_track_message(msg)
    
    # 只有3条成功（0, 2, 4）
    assert len(service._pending_offsets) == 3, f"应该有3个成功的offset，实际: {len(service._pending_offsets)}"
    
    print("✅ 部分消息失败处理测试通过")


async def test_thread_safety():
    """测试线程安全（并发提交）"""
    print("\n=== 测试5: 线程安全 ===")
    
    from d_a import AsyncDataAnalysisService
    
    service = AsyncDataAnalysisService()
    service.consumer.commit_offsets = AsyncMock(return_value=True)
    
    # 添加一些pending offsets
    for i in range(100):
        service._pending_offsets[('test-topic', i % 3)] = i
    
    # 并发执行多个提交
    async def commit_task():
        async with service._commit_lock:
            await service._commit_pending_offsets()
    
    # 启动10个并发任务
    tasks = [asyncio.create_task(commit_task()) for _ in range(10)]
    await asyncio.gather(*tasks)
    
    # 验证: 由于锁保护，应该只清空一次
    assert len(service._pending_offsets) == 0, "应该清空pending_offsets"
    
    print("✅ 线程安全测试通过")


async def main():
    """运行所有测试"""
    print("=" * 60)
    print("开始测试Offset提交功能")
    print("=" * 60)
    
    try:
        await test_batch_commit()
        await test_time_based_commit()
        await test_commit_retry()
        await test_partial_failure()
        await test_thread_safety()
        
        print("\n" + "=" * 60)
        print("✅ 所有测试通过！")
        print("=" * 60)
        
    except AssertionError as e:
        print("\n" + "=" * 60)
        print(f"❌ 测试失败: {e}")
        print("=" * 60)
        raise
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"❌ 测试异常: {e}")
        print("=" * 60)
        raise


if __name__ == '__main__':
    asyncio.run(main())
