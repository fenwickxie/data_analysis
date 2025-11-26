#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
批次结果聚合器
负责收集同一批数据中多个场站的输出结果,并在超时或全部完成后统一上传
"""

import asyncio
import logging
import time
from typing import Dict, List, Any, Optional, Callable
from collections import defaultdict


class BatchResultAggregator:
    """
    批次结果聚合器
    
    功能：
    1. 为每批数据创建独立的结果收集器
    2. 收集各场站的输出结果（允许部分失败）
    3. 超时或全部完成后触发批次上传
    4. 自动清理过期批次
    """
    
    def __init__(
        self,
        batch_timeout: float = 5.0,
        cleanup_interval: float = 60.0
    ):
        """
        Args:
            batch_timeout: 批次超时时间（秒）,超时后即使部分场站未完成也会上传
            cleanup_interval: 清理过期批次的间隔（秒）
        """
        self.batch_timeout = batch_timeout
        self.cleanup_interval = cleanup_interval
        
        # 批次数据: {batch_id: BatchCollector}
        self._batches: Dict[str, 'BatchCollector'] = {}
        self._lock = asyncio.Lock()
        
    async def create_batch(
        self,
        batch_id: str,
        expected_stations: List[str],
        upload_callback: Callable[[str, List[Dict]], Any]
    ) -> 'BatchCollector':
        """
        创建新批次
        
        Args:
            batch_id: 批次ID（通常是消息的时间戳或其他唯一标识）
            expected_stations: 期望的场站列表
            upload_callback: 上传回调函数 (batch_id, results_list) -> Any
            
        Returns:
            BatchCollector: 批次收集器
        """
        async with self._lock:
            if batch_id in self._batches:
                logging.warning(f"批次 {batch_id} 已存在,返回现有批次")
                return self._batches[batch_id]
            
            collector = BatchCollector(
                batch_id=batch_id,
                expected_stations=expected_stations,
                upload_callback=upload_callback,
                timeout=self.batch_timeout
            )
            self._batches[batch_id] = collector
            
            # 启动批次超时监控
            asyncio.create_task(self._monitor_batch(batch_id, collector))
            
            logging.info(f"创建批次 {batch_id},期望场站数: {len(expected_stations)}")
            return collector
    
    async def _monitor_batch(self, batch_id: str, collector: 'BatchCollector'):
        """
        监控批次,超时后触发上传
        """
        try:
            # 等待批次完成或超时
            await asyncio.wait_for(
                collector.wait_completion(),
                timeout=self.batch_timeout
            )
        except asyncio.TimeoutError:
            # 超时：强制完成批次
            logging.warning(
                f"批次 {batch_id} 超时 ({self.batch_timeout}秒),"
                f"已完成 {collector.completed_count}/{collector.expected_count} 个场站"
            )
            await collector.force_complete()
        finally:
            # 清理批次
            async with self._lock:
                if batch_id in self._batches:
                    del self._batches[batch_id]
    
    async def get_or_create_batch(
        self,
        batch_id: str,
        expected_stations: List[str],
        upload_callback: Callable[[str, List[Dict]], Any]
    ) -> 'BatchCollector':
        """
        获取或创建批次
        """
        async with self._lock:
            if batch_id in self._batches:
                return self._batches[batch_id]
        
        return await self.create_batch(batch_id, expected_stations, upload_callback)
    
    async def cleanup_expired(self, max_age: float = 300.0):
        """
        清理过期批次（超过max_age秒未完成的）
        
        Args:
            max_age: 最大存活时间（秒）
        """
        async with self._lock:
            current_time = time.time()
            expired = []
            
            for batch_id, collector in list(self._batches.items()):
                if current_time - collector.created_at > max_age:
                    expired.append(batch_id)
                    logging.warning(f"清理过期批次 {batch_id}")
            
            for batch_id in expired:
                collector = self._batches[batch_id]
                await collector.force_complete()
                del self._batches[batch_id]
            
            if expired:
                logging.info(f"清理了 {len(expired)} 个过期批次")


class BatchCollector:
    """
    单个批次的结果收集器
    """
    
    def __init__(
        self,
        batch_id: str,
        expected_stations: List[str],
        upload_callback: Callable[[str, List[Dict]], Any],
        timeout: float = 5.0
    ):
        self.batch_id = batch_id
        self.expected_stations = set(expected_stations)
        self.expected_count = len(expected_stations)
        self.upload_callback = upload_callback
        self.timeout = timeout
        self.created_at = time.time()
        
        # 结果存储: {station_id: result_dict}
        self._results: Dict[str, Optional[Dict]] = {}
        self._completed = asyncio.Event()
        self._uploaded = False
        self._lock = asyncio.Lock()
        
    async def add_result(self, station_id: str, result: Optional[Dict]):
        """
        添加场站结果
        
        Args:
            station_id: 场站ID
            result: 场站输出结果（可以是None,表示该场站处理失败）
        """
        async with self._lock:
            if self._uploaded:
                logging.warning(f"批次 {self.batch_id} 已上传,忽略场站 {station_id} 的结果")
                return
            
            self._results[station_id] = result
            logging.debug(
                f"批次 {self.batch_id} 收到场站 {station_id} 结果,"
                f"进度: {len(self._results)}/{self.expected_count}"
            )
            
            # 检查是否所有期望的场站都已完成
            if len(self._results) >= self.expected_count:
                logging.info(f"批次 {self.batch_id} 所有场站已完成,触发上传")
                asyncio.create_task(self._do_upload())
    
    @property
    def completed_count(self) -> int:
        """已完成的场站数量"""
        return len(self._results)
    
    async def wait_completion(self):
        """等待批次完成"""
        await self._completed.wait()
    
    async def force_complete(self):
        """强制完成批次（超时调用）"""
        async with self._lock:
            if not self._uploaded:
                logging.info(
                    f"强制完成批次 {self.batch_id},"
                    f"已完成 {len(self._results)}/{self.expected_count} 个场站"
                )
                await self._do_upload()
    
    async def _do_upload(self):
        """执行上传"""
        if self._uploaded:
            return
        
        self._uploaded = True
        
        # 构建结果列表（过滤掉None）
        results_list = [
            result for result in self._results.values()
            if result is not None
        ]
        
        success_count = len(results_list)
        failed_count = self.expected_count - len(self._results)
        none_count = len(self._results) - success_count
        
        logging.info(
            f"批次 {self.batch_id} 上传结果: "
            f"成功={success_count}, 失败={failed_count}, 无输出={none_count}"
        )
        
        try:
            # 调用上传回调
            if self.upload_callback:
                result = self.upload_callback(self.batch_id, results_list)
                # 如果是协程,await它
                if asyncio.iscoroutine(result):
                    await result
        except Exception as exc:
            logging.error(f"批次 {self.batch_id} 上传失败: {exc}", exc_info=True)
        finally:
            # 标记完成
            self._completed.set()
