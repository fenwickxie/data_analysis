#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
多消费者模式拉取诊断工具
用于诊断为什么某些 topic 没有拉取到数据
"""

import asyncio
import logging
from collections import defaultdict
from d_a.kafka_client import AsyncKafkaConsumerClient
from d_a.config import KAFKA_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


async def diagnose_multi_consumer_fetch():
    """诊断多消费者拉取情况"""
    
    topics = [
        "SCHEDULE-STATION-PARAM",
        "SCHEDULE-CAR-ORDER",
        "SCHEDULE-CAR-PRICE",
        "SCHEDULE-DEVICE-HOST-DCDC",
        "SCHEDULE-DEVICE-STORAGE",
        "MODULE-OUTPUT-PV_PREDICTION",
    ]
    
    print("\n" + "="*80)
    print("多消费者模式拉取诊断")
    print("="*80)
    
    consumer_client = AsyncKafkaConsumerClient(
        topics=topics,
        config=KAFKA_CONFIG,
    )
    
    try:
        print("\n[1] 启动消费者...")
        await consumer_client.start()
        print("✅ 消费者启动成功")
        
        # 等待一下，让消费者完全初始化
        await asyncio.sleep(2)
        
        # 进行多次拉取测试
        print("\n[2] 开始拉取测试（共进行 5 轮，每轮间隔 2 秒）")
        print("-" * 80)
        
        topic_total_count = defaultdict(int)
        
        for round_num in range(1, 6):
            print(f"\n第 {round_num} 轮拉取（timeout=3000ms）...")
            
            batch = await consumer_client.getmany(timeout_ms=3000)
            
            # 统计每个 topic 的消息数
            topic_count = defaultdict(int)
            for msg in batch:
                topic_count[msg.topic] += 1
                topic_total_count[msg.topic] += 1
            
            # 显示本轮统计
            if batch:
                print(f"  拉取到 {len(batch)} 条消息：")
                for topic in topics:
                    count = topic_count[topic]
                    status = "✅" if count > 0 else "⚠️"
                    print(f"    {status} {topic}: {count} 条")
            else:
                print(f"  ⚠️  本轮未拉取到任何消息")
                for topic in topics:
                    print(f"    ⚠️ {topic}: 0 条")
            
            if round_num < 5:
                print("  等待 2 秒...")
                await asyncio.sleep(2)
        
        # 总结
        print("\n" + "="*80)
        print("拉取总结（5 轮总计）")
        print("="*80)
        
        total_messages = sum(topic_total_count.values())
        print(f"\n总消息数: {total_messages} 条\n")
        
        for topic in topics:
            count = topic_total_count[topic]
            if count > 0:
                print(f"✅ {topic}: {count} 条")
            else:
                print(f"❌ {topic}: 0 条  ← 该 topic 在 5 轮拉取中都没有数据")
        
        # 分析
        print("\n" + "="*80)
        print("诊断分析")
        print("="*80)
        
        empty_topics = [t for t in topics if topic_total_count[t] == 0]
        
        if not empty_topics:
            print("\n✅ 所有 topic 都有数据，拉取正常！")
        else:
            print(f"\n⚠️  以下 {len(empty_topics)} 个 topic 没有拉取到数据：")
            for topic in empty_topics:
                print(f"  - {topic}")
            
            print("\n可能的原因：")
            print("  1. 该 topic 确实没有新消息（生产者未发送数据）")
            print("  2. 已经消费到最新位置，没有积压")
            print("  3. 生产者的发送频率很低，需要等待更长时间")
            print("  4. fetch_max_wait_ms 配置太短（当前: 500ms）")
            
            print("\n建议的排查步骤：")
            print("  1. 检查生产者是否在发送数据到这些 topic")
            print("  2. 使用 Kafka 命令行工具检查 topic 的 LAG：")
            print("     kafka-consumer-groups.sh --bootstrap-server 10.8.4.40:35888 \\")
            print(f"       --group {KAFKA_CONFIG['consumer']['group_id']} --describe")
            print("  3. 如果 LAG > 0 但拉不到数据，尝试：")
            print("     - 增加 timeout_ms（如 5000ms）")
            print("     - 删除 fetch_max_wait_ms 配置")
            print("     - 增加 max_poll_records")
        
        # 配置建议
        print("\n" + "="*80)
        print("当前配置")
        print("="*80)
        consumer_cfg = KAFKA_CONFIG.get('consumer', {})
        print(f"  group_id: {consumer_cfg.get('group_id')}")
        print(f"  max_poll_records: {consumer_cfg.get('max_poll_records')}")
        print(f"  fetch_max_wait_ms: {consumer_cfg.get('fetch_max_wait_ms')}")
        print(f"  fetch_min_bytes: {consumer_cfg.get('fetch_min_bytes')}")
        print(f"  auto_offset_reset: {consumer_cfg.get('auto_offset_reset')}")
        
        if empty_topics and consumer_cfg.get('fetch_max_wait_ms') == 500:
            print("\n💡 优化建议：")
            print("  fetch_max_wait_ms=500ms 可能太短，建议：")
            print("  - 删除此配置（使用 Kafka 默认值）")
            print("  - 或增加到 1000-2000ms")
        
    except Exception as e:
        print(f"\n❌ 诊断失败: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        print("\n[3] 停止消费者...")
        await consumer_client.stop()
        print("✅ 诊断完成")


async def check_consumer_lag():
    """检查消费者 LAG（需要 kafka-python 支持）"""
    print("\n" + "="*80)
    print("消费者 LAG 检查")
    print("="*80)
    print("\n请手动运行以下命令检查 LAG：\n")
    
    group_id = KAFKA_CONFIG['consumer']['group_id']
    bootstrap = KAFKA_CONFIG['bootstrap_servers'][0]
    
    print(f"kafka-consumer-groups.sh --bootstrap-server {bootstrap} \\")
    print(f"  --group {group_id} --describe")
    print("\n输出示例：")
    print("TOPIC                    PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG")
    print("SCHEDULE-CAR-ORDER       0          12850          12850           0    ← LAG=0 表示没有积压")
    print("SCHEDULE-STATION-PARAM   0          5000           5100            100  ← LAG=100 表示有积压")
    print("\n如果 LAG=0，说明该 topic 没有新消息，返回空是正常的")
    print("如果 LAG>0，但拉不到数据，需要优化配置")


async def main():
    print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║              多消费者模式拉取诊断工具                                        ║
║                                                                              ║
║  目的：诊断为什么某些 topic 没有拉取到数据                                   ║
║                                                                              ║
║  测试方法：                                                                  ║
║  1. 进行 5 轮拉取测试，每轮 timeout=3000ms                                  ║
║  2. 统计每个 topic 拉取的消息数                                             ║
║  3. 分析哪些 topic 没有数据                                                 ║
║  4. 提供诊断建议                                                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
    """)
    
    await diagnose_multi_consumer_fetch()
    await check_consumer_lag()
    
    print("\n" + "="*80)
    print("诊断报告已生成")
    print("="*80)
    print("\n详细文档：docs/MULTI_CONSUMER_EMPTY_FETCH_DIAGNOSIS.md")


if __name__ == "__main__":
    asyncio.run(main())
