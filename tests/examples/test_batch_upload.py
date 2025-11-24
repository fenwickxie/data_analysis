#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
批次结果聚合功能测试示例
"""

import asyncio
import logging
from d_a.analysis_service import AsyncDataAnalysisService

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


async def process_station(station_id, module_input):
    """
    单场站处理回调
    
    模拟场景：
    - 场站ID末尾是偶数：处理成功
    - 场站ID末尾是奇数：处理失败
    """
    try:
        # 模拟处理耗时
        await asyncio.sleep(0.5)
        
        # 模拟部分失败
        if int(station_id[-1]) % 2 == 1:
            raise ValueError(f"场站 {station_id} 模拟失败")
        
        # 返回结果
        result = {
            "station_id": station_id,
            "status": "success",
            "data": module_input.get("_data_quality", {}),
        }
        
        logging.info(f"场站 {station_id} 处理成功")
        return result
        
    except Exception as e:
        logging.error(f"场站 {station_id} 处理失败: {e}")
        return None


async def batch_upload_handler(batch_id, results_list):
    """
    批次上传回调
    
    Args:
        batch_id: 批次ID
        results_list: 成功的场站结果列表
    """
    logging.info("=" * 80)
    logging.info(f"批次上传: {batch_id}")
    logging.info(f"成功场站数: {len(results_list)}")
    
    if results_list:
        logging.info("成功的场站:")
        for result in results_list:
            logging.info(f"  - {result.get('station_id')}: {result.get('status')}")
    else:
        logging.warning("没有成功的场站结果")
    
    logging.info("=" * 80)
    
    # 这里可以添加实际的上传逻辑
    # await kafka_producer.send("OUTPUT_TOPIC", value=results_list)


async def main():
    """主函数"""
    
    # 创建服务
    service = AsyncDataAnalysisService(
        module_name="test_module",
        topics=["SCHEDULE-STATION-REALTIME-DATA"],
    )
    
    # 启动服务
    await service.start(
        callback=process_station,
        batch_upload_handler=batch_upload_handler
    )
    
    logging.info("服务已启动，等待消息...")
    logging.info("提示：")
    logging.info("  1. 偶数编号场站会处理成功")
    logging.info("  2. 奇数编号场站会处理失败")
    logging.info("  3. 批次会在5秒后或所有场站完成后上传")
    
    # 运行一段时间
    try:
        await asyncio.sleep(3600)  # 运行1小时
    except KeyboardInterrupt:
        logging.info("收到停止信号")
    
    # 停止服务
    await service.stop()
    logging.info("服务已停止")


if __name__ == "__main__":
    asyncio.run(main())
