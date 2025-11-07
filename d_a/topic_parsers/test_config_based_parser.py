#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试基于配置的解析器功能

运行方式：
    python test_config_based_parser.py
"""

import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from d_a.parser_base import ConfigBasedParser
from d_a.topic_parsers.station_param import StationParamParser
from d_a.topic_parsers.car_order import CarOrderParser
from d_a import config


def test_config_based_parser():
    """测试通用的 ConfigBasedParser"""
    print("=" * 60)
    print("测试 ConfigBasedParser 基础功能")
    print("=" * 60)
    print()
    
    # 测试 SCHEDULE-STATION-PARAM
    print("1. 测试 SCHEDULE-STATION-PARAM 解析器")
    parser = ConfigBasedParser(topic_name='SCHEDULE-STATION-PARAM')
    
    # 模拟 Kafka 消息
    raw_data = {
        'stationId': 'station001',
        'stationLng': 116.4074,
        'stationLat': 39.9042,
        'gunNum': 10,
        'gridCapacity': 500,
        'meterId': 'meter001',
        'powerNum': 5,
        'normaClap': 1000,
        'totalPower': 5000,
        'hostCode': 'host001',
        'extra_field': 'should_be_ignored'  # 不在配置中的字段
    }
    
    parsed = parser.parse(raw_data)
    
    print(f"   配置的字段: {config.TOPIC_DETAIL['SCHEDULE-STATION-PARAM']['fields']}")
    print(f"   解析结果: {parsed}")
    print()
    
    # 验证
    assert parsed['stationId'] == 'station001', "stationId 解析错误"
    assert parsed['gunNum'] == 10, "gunNum 解析错误"
    assert 'extra_field' not in parsed, "不应包含未配置的字段"
    print("   ✓ 解析正确")
    print()


def test_station_param_parser():
    """测试 StationParamParser"""
    print("=" * 60)
    print("测试 StationParamParser")
    print("=" * 60)
    print()
    
    parser = StationParamParser()
    
    raw_data = {
        'stationId': 'station002',
        'stationLng': 121.4737,
        'stationLat': 31.2304,
        'gunNum': 20,
        'gridCapacity': 1000,
        'meterId': 'meter002',
        'powerNum': 10,
        'normaClap': 2000,
        'totalPower': 10000,
        'hostCode': 'host002'
    }
    
    parsed = parser.parse(raw_data)
    
    print(f"   原始数据: {raw_data}")
    print(f"   解析结果: {parsed}")
    print()
    
    # 验证所有配置的字段都被提取
    expected_fields = config.TOPIC_DETAIL['SCHEDULE-STATION-PARAM']['fields']
    for field in expected_fields:
        assert field in parsed, f"缺失字段: {field}"
    
    print("   ✓ 所有字段都正确提取")
    print()


def test_car_order_parser():
    """测试 CarOrderParser"""
    print("=" * 60)
    print("测试 CarOrderParser")
    print("=" * 60)
    print()
    
    parser = CarOrderParser()
    
    raw_data = {
        'stationId': 'station001',
        'transactionSerialNo': 'order001',
        'hostCode': 'host001',
        'gunNo': 1,
        'terminalMaxOutElectric': 250,
        'startChargeTime': '2025-11-07 10:00:00',
        'endChargeTime': '2025-11-07 11:30:00',
        'beginSOC': 20,
        'soc': 80,
        'terminalRequireVoltage': 400,
        'terminalRequireElectric': 200,
        'outputPower': 60,
        'carProducerCode': 'TESLA',
        'batteryNominalTotalCapacity': 75
    }
    
    parsed = parser.parse(raw_data)
    
    print(f"   原始数据: {raw_data}")
    print(f"   解析结果: {parsed}")
    print()
    
    # 验证所有配置的字段都被提取
    expected_fields = config.TOPIC_DETAIL['SCHEDULE-CAR-ORDER']['fields']
    for field in expected_fields:
        assert field in parsed, f"缺失字段: {field}"
    
    print("   ✓ 所有字段都正确提取")
    print()


def test_null_data():
    """测试空数据和缺失字段处理"""
    print("=" * 60)
    print("测试空数据和缺失字段处理")
    print("=" * 60)
    print()
    
    parser = StationParamParser()
    
    # 测试 None
    print("1. 测试 None 输入")
    result = parser.parse(None)
    print(f"   结果: {result}")
    assert result is None, "None 输入应返回 None"
    print("   ✓ 正确处理 None")
    print()
    
    # 测试空字典
    print("2. 测试空字典输入")
    result = parser.parse({})
    print(f"   结果: {result}")
    assert isinstance(result, dict), "空字典应返回字典"
    print("   ✓ 正确处理空字典")
    print()
    
    # 测试部分字段缺失
    print("3. 测试部分字段缺失")
    partial_data = {
        'stationId': 'station003',
        'gunNum': 5
        # 其他字段缺失
    }
    result = parser.parse(partial_data)
    print(f"   原始数据: {partial_data}")
    print(f"   解析结果: {result}")
    
    assert result['stationId'] == 'station003', "stationId 应正确提取"
    assert result['gunNum'] == 5, "gunNum 应正确提取"
    assert result['stationLng'] is None, "缺失字段应为 None"
    print("   ✓ 正确处理缺失字段（设为 None）")
    print()


def test_all_topics():
    """测试所有配置的 topic"""
    print("=" * 60)
    print("测试所有 Topic 配置")
    print("=" * 60)
    print()
    
    success_count = 0
    fail_count = 0
    
    for topic_name in config.TOPIC_DETAIL.keys():
        try:
            parser = ConfigBasedParser(topic_name=topic_name)
            fields = parser.fields
            print(f"✓ {topic_name}: {len(fields)} 个字段")
            success_count += 1
        except Exception as e:
            print(f"✗ {topic_name}: {e}")
            fail_count += 1
    
    print()
    print(f"成功: {success_count}, 失败: {fail_count}")
    print()


def main():
    """运行所有测试"""
    try:
        test_config_based_parser()
        test_station_param_parser()
        test_car_order_parser()
        test_null_data()
        test_all_topics()
        
        print("=" * 60)
        print("所有测试通过！✓")
        print("=" * 60)
        
    except AssertionError as e:
        print()
        print("=" * 60)
        print(f"测试失败：{e}")
        print("=" * 60)
        sys.exit(1)
    
    except Exception as e:
        print()
        print("=" * 60)
        print(f"运行错误：{e}")
        import traceback
        traceback.print_exc()
        print("=" * 60)
        sys.exit(1)


if __name__ == '__main__':
    main()
