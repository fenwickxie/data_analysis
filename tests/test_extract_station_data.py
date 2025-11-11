#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试 extract_station_data 方法对各种数据格式的处理
"""

import sys
from pathlib import Path

# 添加项目根目录到 sys.path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from d_a.service_base import ServiceBase


def test_schedule_station_param():
    """测试 SCHEDULE-STATION-PARAM 格式（直接列表）"""
    print("\n=== 测试 SCHEDULE-STATION-PARAM ===")
    data = [
        {'gridCapacity': 0.0, 'gunNum': 2, 'stationId': '1053405539402387456'},
        {'gridCapacity': 0.0, 'gunNum': 0, 'stationId': '1068663126427308032'}
    ]
    
    result = ServiceBase.extract_station_data('SCHEDULE-STATION-PARAM', data)
    print(f"输入: {len(data)} 个场站")
    print(f"输出: {len(result)} 个场站")
    for station_id, station_data in result:
        print(f"  - {station_id}: {station_data.get('gunNum')} 个枪")
    
    assert len(result) == 2
    assert result[0][0] == '1053405539402387456'
    assert result[1][0] == '1068663126427308032'
    print("✅ 测试通过")


def test_schedule_station_realtime_data():
    """测试 SCHEDULE-STATION-REALTIME-DATA 格式（realTimeData）"""
    print("\n=== 测试 SCHEDULE-STATION-REALTIME-DATA ===")
    data = {
        'realTimeData': [
            {'stationId': '1881540927845654529', 'hostCode': '52000000000088'},
            {'stationId': '1173980655843938304', 'hostCode': '52000000000631'},
            {'stationId': '1179430837217792000', 'hostCode': '52000000000632'}
        ]
    }
    
    result = ServiceBase.extract_station_data('SCHEDULE-STATION-REALTIME-DATA', data)
    print(f"输入: {len(data['realTimeData'])} 个场站")
    print(f"输出: {len(result)} 个场站")
    for station_id, station_data in result:
        print(f"  - {station_id}: {station_data.get('hostCode')}")
    
    assert len(result) == 3
    assert result[0][0] == '1881540927845654529'
    assert result[1][0] == '1173980655843938304'
    assert result[2][0] == '1179430837217792000'
    print("✅ 测试通过")


def test_schedule_car_price():
    """测试 SCHEDULE-CAR-PRICE 格式（fee）"""
    print("\n=== 测试 SCHEDULE-CAR-PRICE ===")
    data = {
        'fee': [
            {'stationId': '1077316458003959808', 'dialNo': '2100', 'flatElectricFee': 0.7},
            {'stationId': '1077316458003959808', 'dialNo': '2101', 'flatElectricFee': 0.8}
        ]
    }
    
    result = ServiceBase.extract_station_data('SCHEDULE-CAR-PRICE', data)
    print(f"输入: {len(data['fee'])} 条费率")
    print(f"输出: {len(result)} 条费率")
    for station_id, station_data in result:
        print(f"  - {station_id}: {station_data.get('dialNo')}, 电费={station_data.get('flatElectricFee')}")
    
    assert len(result) == 2
    assert result[0][0] == '1077316458003959808'
    assert result[1][0] == '1077316458003959808'
    print("✅ 测试通过")


def test_schedule_environment_calendar():
    """测试 SCHEDULE-ENVIRONMENT-CALENDAR 格式（全局数据）"""
    print("\n=== 测试 SCHEDULE-ENVIRONMENT-CALENDAR ===")
    data = {
        'calendar': [
            {'date': '2025-11-11', 'isHoliday': 0, 'isWeekend': 0},
            {'date': '2025-11-12', 'isHoliday': 0, 'isWeekend': 0}
        ]
    }
    
    result = ServiceBase.extract_station_data('SCHEDULE-ENVIRONMENT-CALENDAR', data)
    print(f"输入: {len(data['calendar'])} 天日历")
    print(f"输出: {len(result)} 条记录（全局数据）")
    for station_id, station_data in result:
        print(f"  - {station_id}: {len(station_data.get('calendar', []))} 天")
    
    assert len(result) == 1
    assert result[0][0] == '__global__'
    assert 'calendar' in result[0][1]
    print("✅ 测试通过")


def test_schedule_device_storage():
    """测试 SCHEDULE-DEVICE-STORAGE 格式（单场站字典）"""
    print("\n=== 测试 SCHEDULE-DEVICE-STORAGE ===")
    data = {
        'stationId': '1881540927845654529',
        'batteryGroupElectric': 11.2,
        'batteryGroupSoc': 12,
        'hostCode': '42000000001900'
    }
    
    result = ServiceBase.extract_station_data('SCHEDULE-DEVICE-STORAGE', data)
    print(f"输入: 1 个场站的储能数据")
    print(f"输出: {len(result)} 个场站")
    for station_id, station_data in result:
        print(f"  - {station_id}: SOC={station_data.get('batteryGroupSoc')}%")
    
    assert len(result) == 1
    assert result[0][0] == '1881540927845654529'
    assert result[0][1]['batteryGroupSoc'] == 12
    print("✅ 测试通过")


def test_schedule_device_host_dcdc():
    """测试 SCHEDULE-DEVICE-HOST-DCDC 格式（单场站字典）"""
    print("\n=== 测试 SCHEDULE-DEVICE-HOST-DCDC ===")
    data = {
        'dcWorkStatus': ['00', '01', '00'],
        'dcdcPower': [3.0, 15.0, 30.0],
        'hostCode': '42000000001900',
        'stationId': '1881540927845654529'
    }
    
    result = ServiceBase.extract_station_data('SCHEDULE-DEVICE-HOST-DCDC', data)
    print(f"输入: 1 个场站的DCDC数据")
    print(f"输出: {len(result)} 个场站")
    for station_id, station_data in result:
        print(f"  - {station_id}: {len(station_data.get('dcWorkStatus', []))} 个模块")
    
    assert len(result) == 1
    assert result[0][0] == '1881540927845654529'
    assert len(result[0][1]['dcWorkStatus']) == 3
    print("✅ 测试通过")


def test_empty_data():
    """测试空数据"""
    print("\n=== 测试空数据 ===")
    
    # 空列表
    result = ServiceBase.extract_station_data('TEST', [])
    assert len(result) == 0
    print("  - 空列表: ✅")
    
    # 空字典
    result = ServiceBase.extract_station_data('TEST', {})
    assert len(result) == 0
    print("  - 空字典: ✅")
    
    # 无 stationId 的列表
    result = ServiceBase.extract_station_data('TEST', [{'value': 1}, {'value': 2}])
    assert len(result) == 0
    print("  - 无stationId的列表: ✅")
    
    print("✅ 测试通过")


if __name__ == "__main__":
    print("=" * 60)
    print("测试 extract_station_data 方法")
    print("=" * 60)
    
    try:
        test_schedule_station_param()
        test_schedule_station_realtime_data()
        test_schedule_car_price()
        test_schedule_environment_calendar()
        test_schedule_device_storage()
        test_schedule_device_host_dcdc()
        test_empty_data()
        
        print("\n" + "=" * 60)
        print("✅ 所有测试通过！")
        print("=" * 60)
    except AssertionError as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()
    except Exception as e:
        print(f"\n❌ 测试出错: {e}")
        import traceback
        traceback.print_exc()
