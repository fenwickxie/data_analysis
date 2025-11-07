#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
批量更新 topic_parsers，使用基于配置的自动解析器

运行方式：
    python update_all_parsers.py
"""

import os
from pathlib import Path

# Topic 名称与文件、类名的映射
PARSER_MAPPING = {
    'SCHEDULE-STATION-PARAM': {
        'file': 'station_param.py',
        'class': 'StationParamParser'
    },
    'SCHEDULE-STATION-REALTIME-DATA': {
        'file': 'station_realtime_data.py',
        'class': 'StationRealtimeDataParser'
    },
    'SCHEDULE-ENVIRONMENT-CALENDAR': {
        'file': 'environment_calendar.py',
        'class': 'EnvironmentCalendarParser'
    },
    'SCHEDULE-DEVICE-METER': {
        'file': 'device_meter.py',
        'class': 'DeviceMeterParser'
    },
    'SCHEDULE-DEVICE-GUN': {
        'file': 'device_gun.py',
        'class': 'DeviceGunParser'
    },
    'SCHEDULE-CAR-ORDER': {
        'file': 'car_order.py',
        'class': 'CarOrderParser'
    },
    'SCHEDULE-CAR-PRICE': {
        'file': 'car_price.py',
        'class': 'CarPriceParser'
    },
    'SCHEDULE-DEVICE-ERROR': {
        'file': 'device_error.py',
        'class': 'DeviceErrorParser'
    },
    'SCHEDULE-DEVICE-STORAGE': {
        'file': 'device_storage.py',
        'class': 'DeviceStorageParser'
    },
}

# ACDC 和 DCDC 特殊处理（都在 device_host.py 中）
HOST_PARSERS = [
    ('SCHEDULE-DEVICE-HOST-DCDC', 'DeviceHostDCDCParser'),
    ('SCHEDULE-DEVICE-HOST-ACDC', 'DeviceHostACDCParser'),
]

def generate_parser_code(topic_name, class_name):
    """生成新的解析器代码"""
    return f'''#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
自动更新：使用基于配置的解析器
字段自动从 config.TOPIC_DETAIL['{topic_name}']['fields'] 读取
"""

from ..parser_base import ConfigBasedParser

class {class_name}(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name='{topic_name}')


# 如果需要自定义解析逻辑，可以覆盖 parse 方法：
# 
# class {class_name}(ConfigBasedParser):
#     def __init__(self):
#         super().__init__(topic_name='{topic_name}')
#     
#     def parse(self, raw_data):
#         # 先调用父类方法获取基础字段
#         parsed_data = super().parse(raw_data)
#         
#         if parsed_data:
#             # 添加自定义处理逻辑
#             # 例如：类型转换、计算衍生字段等
#             pass
#         
#         return parsed_data
'''

def generate_host_parser_code():
    """生成 device_host.py 的代码（包含 DCDC 和 ACDC 两个解析器）"""
    return '''#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
自动更新：使用基于配置的解析器
包含 DCDC 和 ACDC 两个解析器
"""

from ..parser_base import ConfigBasedParser

class DeviceHostDCDCParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name='SCHEDULE-DEVICE-HOST-DCDC')


class DeviceHostACDCParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name='SCHEDULE-DEVICE-HOST-ACDC')
'''

def update_parser_file(filepath, content, backup=True):
    """更新解析器文件"""
    if backup and filepath.exists():
        backup_path = filepath.with_suffix('.py.bak')
        print(f"  备份原文件到: {backup_path}")
        filepath.rename(backup_path)
    
    print(f"  写入新内容到: {filepath}")
    filepath.write_text(content, encoding='utf-8')

def main():
    """主函数"""
    # 获取 topic_parsers 目录
    script_dir = Path(__file__).parent
    
    print("=" * 60)
    print("批量更新 topic_parsers 为基于配置的自动解析器")
    print("=" * 60)
    print()
    
    # 更新标准解析器
    for topic_name, info in PARSER_MAPPING.items():
        filename = info['file']
        class_name = info['class']
        filepath = script_dir / filename
        
        print(f"更新: {filename} ({class_name})")
        print(f"  Topic: {topic_name}")
        
        code = generate_parser_code(topic_name, class_name)
        update_parser_file(filepath, code)
        print(f"  ✓ 完成")
        print()
    
    # 更新 device_host.py（特殊处理）
    print(f"更新: device_host.py (包含 DCDC 和 ACDC)")
    filepath = script_dir / 'device_host.py'
    code = generate_host_parser_code()
    update_parser_file(filepath, code)
    print(f"  ✓ 完成")
    print()
    
    print("=" * 60)
    print("更新完成！")
    print()
    print("注意事项：")
    print("1. 原文件已备份为 .py.bak，如需恢复可以重命名")
    print("2. 请运行测试确保解析功能正常")
    print("3. 如果某些解析器需要自定义逻辑，请参考 UPDATE_GUIDE.md")
    print("=" * 60)

if __name__ == '__main__':
    main()
