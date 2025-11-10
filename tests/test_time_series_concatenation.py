#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
时序数据窗口拼接测试

验证dispatcher.get_module_input()返回的数据格式
"""

import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from d_a.dispatcher import DataDispatcher
from d_a.config import TOPIC_DETAIL
import json


def test_time_series_concatenation():
    """测试时序数据拼接功能"""
    
    print("=" * 60)
    print("测试时序数据窗口拼接（枪号对齐）")
    print("=" * 60)
    
    # 1. 初始化dispatcher
    dispatcher = DataDispatcher(data_expire_seconds=600)
    
    station_id = "1881540927845654529"
    topic = "SCHEDULE-STATION-REALTIME-DATA"
    
    # 2. 模拟添加3条时序数据（枪号不同）
    print("\n步骤1: 添加3条时序数据到窗口（枪号不同）")
    
    test_data = [
        {
            "stationId": station_id,
            "hostCode": "52000000000088",
            "sendTime": "2025-11-04 09:00:00",
            "gunPower": {
                "gunNo": ["01", "05", "02"],  # 3个枪
                "outputPowerPerGunAvg": [2.73, 34.29, 88.48],
                "outputPowerPerGunMax": [7.0, 100.8, 135.2]
            }
        },
        {
            "stationId": station_id,
            "hostCode": "52000000000088",
            "sendTime": "2025-11-04 10:00:00",
            "gunPower": {
                "gunNo": ["02", "01"],  # 2个枪（缺少05）
                "outputPowerPerGunAvg": [44.17, 2.27],
                "outputPowerPerGunMax": [79.2, 196.8]
            }
        },
        {
            "stationId": station_id,
            "hostCode": "52000000000088",
            "sendTime": "2025-11-04 11:00:00",
            "gunPower": {
                "gunNo": ["01", "05", "03"],  # 3个枪（有新枪03，缺少02）
                "outputPowerPerGunAvg": [56.42, 45.8, 12.5],
                "outputPowerPerGunMax": [60.1, 50.2, 15.0]
            }
        }
    ]
    
    for i, data in enumerate(test_data):
        dispatcher.update_topic_data(station_id, topic, data)
        print(f"  添加数据 {i+1}: sendTime={data['sendTime']}, "
              f"枪号={data['gunPower']['gunNo']}")
    
    # 3. 获取模块输入
    print("\n步骤2: 获取模块输入数据（应该已对齐）")
    module_input = dispatcher.get_module_input(station_id, "load_prediction")
    
    if module_input is None:
        print("❌ 获取模块输入失败")
        return False
    
    # 4. 验证数据格式
    print("\n步骤3: 验证数据格式")
    
    # 验证必需字段
    required_fields = ['stationId', 'sendTime', 'gunNo', 
                      'outputPowerPerGunAvg', 'outputPowerPerGunMax', 'hostCode']
    
    for field in required_fields:
        if field not in module_input:
            print(f"❌ 缺少字段: {field}")
            return False
        print(f"  ✓ 字段 '{field}' 存在")
    
    # 5. 验证数据结构
    print("\n步骤4: 验证枪号对齐")
    
    # 验证stationId
    assert module_input['stationId'] == station_id, "stationId不匹配"
    print(f"  ✓ stationId: {module_input['stationId']}")
    
    # 验证列表长度
    window_size = len(module_input['sendTime'])
    print(f"  ✓ 窗口大小: {window_size}")
    assert window_size == 3, f"窗口大小应为3，实际为{window_size}"
    
    # 验证gunNo应该在所有时间点保持一致
    print(f"\n  gunNo (统一顺序): {module_input['gunNo']}")
    
    # gunNo现在是一维列表（统一的枪号顺序）
    unified_gun_nos = module_input['gunNo']
    print(f"  ✓ 统一的枪号顺序: {unified_gun_nos}")
    
    # 验证所有时间点的枪数量一致
    gun_count = len(unified_gun_nos)
    for i in range(window_size):
        avg_count = len(module_input['outputPowerPerGunAvg'][i]) if i < len(module_input['outputPowerPerGunAvg']) else 0
        max_count = len(module_input['outputPowerPerGunMax'][i]) if i < len(module_input['outputPowerPerGunMax']) else 0
        
        assert avg_count == gun_count, f"时间点{i+1} avg枪数({avg_count})应等于统一枪数({gun_count})"
        assert max_count == gun_count, f"时间点{i+1} max枪数({max_count})应等于统一枪数({gun_count})"
        
        print(f"  ✓ 时间点 {i+1} ({module_input['sendTime'][i]}): "
              f"枪数={avg_count}, 对齐一致")
    
    # 6. 显示对齐后的数据格式
    print("\n步骤5: 对齐后的数据格式")
    print("-" * 60)
    print(f"stationId: {module_input['stationId']}")
    print(f"\nsendTime: {module_input['sendTime']}")
    print(f"\ngunNo (统一顺序): {module_input['gunNo']}")
    
    print("\noutputPowerPerGunAvg (对齐后，缺失值已填充):")
    for i, powers in enumerate(module_input['outputPowerPerGunAvg']):
        print(f"  时间点 {i+1}: {powers}")
        # 显示每个枪的值
        for j, gun in enumerate(module_input['gunNo']):
            print(f"    枪 {gun}: {powers[j]}")
    
    print("\noutputPowerPerGunMax (对齐后，缺失值已填充):")
    for i, powers in enumerate(module_input['outputPowerPerGunMax']):
        print(f"  时间点 {i+1}: {powers}")
        for j, gun in enumerate(module_input['gunNo']):
            print(f"    枪 {gun}: {powers[j]}")
    
    # 7. 验证数据对齐和填充
    print("\n步骤6: 验证数据对齐和填充")
    
    # 预期的枪号顺序（按首次出现顺序）
    expected_gun_order = ['01', '05', '02', '03']
    
    print(f"  预期枪号顺序: {expected_gun_order}")
    print(f"  实际枪号顺序: {module_input['gunNo']}")
    
    # 验证每个时间点的数据长度
    for i in range(window_size):
        avg_len = len(module_input['outputPowerPerGunAvg'][i])
        max_len = len(module_input['outputPowerPerGunMax'][i])
        gun_count = len(module_input['gunNo'])
        
        assert avg_len == gun_count, f"时间点{i+1} avg长度({avg_len})应等于枪数量({gun_count})"
        assert max_len == gun_count, f"时间点{i+1} max长度({max_len})应等于枪数量({gun_count})"
        print(f"  ✓ 时间点 {i+1}: 所有字段长度={gun_count}（对齐成功）")
    
    # 8. 显示完整JSON
    print("\n步骤7: 完整的模块输入数据 (JSON格式)")
    print("-" * 60)
    print(json.dumps(module_input, indent=2, ensure_ascii=False))
    
    print("\n" + "=" * 60)
    print("✅ 枪号对齐测试通过！")
    print("=" * 60)
    
    return True


def test_empty_window():
    """测试空窗口情况"""
    
    print("\n\n" + "=" * 60)
    print("测试空窗口情况")
    print("=" * 60)
    
    dispatcher = DataDispatcher()
    station_id = "empty_station"
    
    # 不添加任何数据，直接获取
    module_input = dispatcher.get_module_input(station_id, "load_prediction")
    
    if module_input is None:
        print("✅ 空窗口返回None（符合预期）")
        return True
    
    # 如果返回数据，验证是空列表
    print(f"module_input: {module_input}")
    
    if 'sendTime' in module_input:
        assert len(module_input['sendTime']) == 0, "空窗口应该返回空列表"
        print("✅ 空窗口返回空列表（符合预期）")
        return True
    
    return False


def test_single_data_point():
    """测试单条数据（窗口大小为1）"""
    
    print("\n\n" + "=" * 60)
    print("测试单条数据")
    print("=" * 60)
    
    dispatcher = DataDispatcher()
    station_id = "single_station"
    topic = "SCHEDULE-STATION-REALTIME-DATA"
    
    # 只添加1条数据
    data = {
        "stationId": station_id,
        "hostCode": "52000000000088",
        "sendTime": "2025-11-04 09:00:00",
        "gunPower": {
            "gunNo": ["01", "05", "02"],
            "outputPowerPerGunAvg": [2.73, 34.29, 88.48],
            "outputPowerPerGunMax": [7.0, 100.8, 135.2]
        }
    }
    
    dispatcher.update_topic_data(station_id, topic, data)
    module_input = dispatcher.get_module_input(station_id, "load_prediction")
    
    if module_input is None:
        print("❌ 获取失败")
        return False
    
    # 验证
    assert len(module_input['sendTime']) == 1, "应该只有1条数据"
    assert len(module_input['gunNo']) == 1, "应该只有1个时间点的枪数据"
    assert len(module_input['gunNo'][0]) == 3, "第1个时间点应该有3个枪"
    
    print("✅ 单条数据测试通过")
    print(f"  sendTime: {module_input['sendTime']}")
    print(f"  gunNo: {module_input['gunNo']}")
    
    return True


def main():
    """运行所有测试"""
    
    try:
        # 测试1: 基本时序数据拼接
        success1 = test_time_series_concatenation()
        
        # 测试2: 空窗口
        success2 = test_empty_window()
        
        # 测试3: 单条数据
        success3 = test_single_data_point()
        
        if success1 and success2 and success3:
            print("\n\n" + "=" * 60)
            print("🎉 所有测试全部通过！")
            print("=" * 60)
            return 0
        else:
            print("\n\n" + "=" * 60)
            print("❌ 部分测试失败")
            print("=" * 60)
            return 1
            
    except Exception as e:
        print("\n\n" + "=" * 60)
        print(f"❌ 测试过程中发生异常: {e}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    exit(main())
