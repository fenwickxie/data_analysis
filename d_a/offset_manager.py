#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Offset管理器,负责Kafka offset的跟踪和提交
"""

import logging
import time
from aiokafka import TopicPartition, OffsetAndMetadata


class OffsetManager:
    """Offset管理器,用于异步服务的offset提交"""
    
    def __init__(self, consumer, config):
        """
        初始化Offset管理器
        
        Args:
            consumer: AsyncKafkaConsumerClient实例
            config: offset配置字典,包含：
                - commit_interval_seconds: 提交间隔（秒）
                - commit_batch_size: 批次大小
                - max_commit_retries: 最大重试次数
                - commit_retry_delay: 重试延迟（秒）
        """
        self.consumer = consumer
        self.config = config
        self._pending_offsets = {}  # {(topic, partition): offset}
        self._processed_count = 0
        self._last_commit_time = time.time()
    
    def track_message(self, msg):
        """
        跟踪消息的offset
        
        Args:
            msg: Kafka消息对象
        """
        tp_key = (msg.topic, msg.partition)
        self._pending_offsets[tp_key] = msg.offset + 1
        self._processed_count += 1
    
    def should_commit(self):
        """
        判断是否应该提交offset
        
        Returns:
            bool: 是否应该提交
        """
        if not self._pending_offsets:
            return False
        
        commit_interval = self.config.get('commit_interval_seconds', 5.0)
        commit_batch_size = self.config.get('commit_batch_size', 100)
        
        # 检查是否达到批次大小
        if self._processed_count >= commit_batch_size:
            return True
        
        # 检查是否达到时间间隔
        if time.time() - self._last_commit_time >= commit_interval:
            return True
        
        return False
    
    async def commit(self):
        """
        提交所有待处理的offset
        
        Returns:
            bool: 是否成功提交
        """
        if not self._pending_offsets:
            return True
        
        try:
            # 构造aiokafka需要的offset字典
            offsets_dict = {}
            for (topic, partition), offset in self._pending_offsets.items():
                tp = TopicPartition(topic, partition)
                offsets_dict[tp] = OffsetAndMetadata(offset, "")
            
            # 带重试的提交
            success = await self._commit_with_retry(offsets_dict)
            
            if success:
                # 提交成功,清空待处理offset和计数器
                committed_count = len(self._pending_offsets)
                processed = self._processed_count
                self._pending_offsets.clear()
                self._processed_count = 0
                self._last_commit_time = time.time()
                logging.info(f"成功提交 {committed_count} 个分区的offset,共处理 {processed} 条消息")
                return True
            else:
                # 提交失败,保留待处理offset供下次重试
                logging.warning(f"Offset提交失败,保留 {len(self._pending_offsets)} 个待处理offset")
                return False
        
        except Exception as exc:
            logging.error(f"提交pending offsets时发生异常: {exc}")
            # 异常时也保留offset供下次重试
            return False
    
    async def _commit_with_retry(self, offsets_dict):
        """
        带重试机制的offset提交
        
        Args:
            offsets_dict: TopicPartition到OffsetAndMetadata的字典
            
        Returns:
            bool: 提交是否成功
        """
        max_retries = self.config.get('max_commit_retries', 3)
        retry_delay = self.config.get('commit_retry_delay', 1.0)
        
        for attempt in range(max_retries):
            try:
                success = await self.consumer.commit_offsets(offsets_dict)
                if success:
                    logging.debug(f"成功提交offset: {len(offsets_dict)} 个分区")
                    return True
                else:
                    logging.warning(f"提交offset失败,尝试 {attempt + 1}/{max_retries}")
            except Exception as exc:
                logging.error(f"提交offset异常,尝试 {attempt + 1}/{max_retries}: {exc}")
            
            if attempt < max_retries - 1:
                import asyncio
                await asyncio.sleep(retry_delay)
        
        logging.error("达到最大重试次数,offset提交失败")
        return False
