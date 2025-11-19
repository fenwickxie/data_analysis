#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
验证多消费者模式的 group_id 修复
测试脚本：检查每个 topic 是否使用了独立的 group_id
"""

import asyncio
import logging
from d_a.kafka_client import AsyncKafkaConsumerClient
from d_a.config import KAFKA_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(name)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


async def verify_multi_consumer_group_ids():
    """验证多消费者模式下每个 topic 是否有独立的 group_id"""
    
    topics = [
        "SCHEDULE-STATION-PARAM",
        "SCHEDULE-CAR-ORDER",
        "SCHEDULE-CAR-PRICE",
        "SCHEDULE-DEVICE-HOST-DCDC",
        "SCHEDULE-DEVICE-STORAGE",
        "MODULE-OUTPUT-PV_PREDICTION",
    ]
    
    print("\n" + "="*80)
    print("多消费者模式 Group ID 验证")
    print("="*80)
    
    # 创建消费者客户端
    consumer_client = AsyncKafkaConsumerClient(
        topics=topics,
        config=KAFKA_CONFIG,
    )
    
    try:
        # 启动消费者
        print("\n[1] 启动消费者...")
        await consumer_client.start()
        
        # 获取所有消费者
        print("\n[2] 检查消费者配置...")
        topic_consumers = consumer_client.get_consumer()
        
        if not isinstance(topic_consumers, dict):
            print("❌ 错误：未使用多消费者模式")
            return False
        
        print(f"\n[3] 共创建了 {len(topic_consumers)} 个独立消费者\n")
        
        # 验证每个 topic 的 group_id
        base_group_id = KAFKA_CONFIG.get('consumer', {}).get('group_id', 'unknown')
        all_correct = True
        
        for topic, consumer in topic_consumers.items():
            # 获取消费者的 group_id
            # type: ignore - AIOKafkaConsumer has _group_id attribute
            actual_group_id = consumer._group_id  # type: ignore
            expected_group_id = f"{base_group_id}-{topic}"
            
            status = "✅" if actual_group_id == expected_group_id else "❌"
            
            print(f"{status} Topic: {topic}")
            print(f"   Expected group_id: {expected_group_id}")
            print(f"   Actual group_id:   {actual_group_id}")
            print()
            
            if actual_group_id != expected_group_id:
                all_correct = False
        
        # 验证结果
        print("="*80)
        if all_correct:
            print("✅ 验证通过：所有 topic 都使用了独立的 group_id")
            print("   格式：{base_group_id}-{topic}")
        else:
            print("❌ 验证失败：存在使用相同 group_id 的消费者")
        print("="*80)
        
        # 测试消息拉取
        print("\n[4] 测试消息拉取（5秒）...")
        batch = await consumer_client.getmany(timeout_ms=5000)
        print(f"   拉取到 {len(batch)} 条消息")
        
        # 检查是否有 offset 越界错误的日志
        print("\n[5] 请检查上方日志，确认没有以下错误：")
        print("   - 'Fetch offset ** is out of range'")
        print("   - 'OffsetOutOfRangeError'")
        
        return all_correct
        
    except Exception as e:
        print(f"\n❌ 验证失败：{e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # 停止消费者
        print("\n[6] 停止消费者...")
        await consumer_client.stop()
        print("✅ 消费者已停止")


async def test_single_consumer_mode():
    """测试单消费者模式（对照组）"""
    
    print("\n" + "="*80)
    print("单消费者模式测试（对照组）")
    print("="*80)
    
    # 修改配置，禁用多消费者模式
    config = KAFKA_CONFIG.copy()
    config['consumer']['multi_consumer_mode'] = False
    
    consumer_client = AsyncKafkaConsumerClient(
        topics=["SCHEDULE-STATION-PARAM"],
        config=config,
    )
    
    try:
        await consumer_client.start()
        
        consumer = consumer_client.get_consumer()
        if isinstance(consumer, dict):
            print("❌ 错误：应该是单消费者模式")
            return False
        
        print(f"✅ 单消费者模式正常")
        if hasattr(consumer, '_group_id'):
            print(f"   Group ID: {consumer._group_id}")  # type: ignore
        else:
            print(f"   Group ID: (无法获取)")
        
        # 测试拉取
        batch = await consumer_client.getmany(timeout_ms=2000)
        print(f"   拉取到 {len(batch)} 条消息")
        
        return True
        
    finally:
        await consumer_client.stop()


async def main():
    """主函数"""
    
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    多消费者模式 Group ID 修复验证工具                        ║
║                                                                              ║
║  目的：验证每个 topic 是否使用了独立的 group_id                              ║
║  修复：topic_kwargs["group_id"] = f"{base_group_id}-{topic}"                ║
║                                                                              ║
║  预期结果：                                                                  ║
║  1. 每个 topic 有独立的 group_id                                            ║
║  2. 不再出现 "Fetch offset is out of range" 错误                           ║
║  3. 所有 topic 可以并发消费                                                 ║
╚══════════════════════════════════════════════════════════════════════════════╝
    """)
    
    # 测试1：多消费者模式
    result1 = await verify_multi_consumer_group_ids()
    
    await asyncio.sleep(2)
    
    # 测试2：单消费者模式（对照）
    result2 = await test_single_consumer_mode()
    
    # 总结
    print("\n" + "="*80)
    print("测试总结")
    print("="*80)
    print(f"多消费者模式: {'✅ 通过' if result1 else '❌ 失败'}")
    print(f"单消费者模式: {'✅ 通过' if result2 else '❌ 失败'}")
    print("="*80)
    
    if result1 and result2:
        print("\n🎉 所有测试通过！修复生效。")
        print("\n建议：")
        print("1. 观察生产环境日志，确认不再出现 offset 越界错误")
        print("2. 使用 Kafka 管理工具检查消费者组列表")
        print("3. 监控各个 topic 的消费延迟")
    else:
        print("\n⚠️  部分测试失败，请检查配置和代码。")


if __name__ == "__main__":
    asyncio.run(main())
