#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
设备枪解析器
包含枪号对齐和数据插值逻辑
"""

from ..parser_base import ConfigBasedParser

class DeviceGunParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name='SCHEDULE-DEVICE-GUN')
    
    def parse_window(self, window_data):
        """
        重写窗口解析方法,实现枪号对齐
        
        Args:
            window_data: 窗口内的原始数据列表
            
        Returns:
            dict: 对齐后的数据,格式为：
            {
                'gunNo': ['01', '02', '05', ...],  # 统一的枪号顺序
                'hostCode': [['host1', 'host2', ...], ...],  # 时序列表
                'status': [['charging', 'idle', ...], ...],
                'sendTime': ['2025-11-04 09:00:00', ...],
                ...
            }
        """
        if not window_data:
            return {}
        
        # 第一步：解析所有数据
        parsed_list = []
        for raw_data in window_data:
            parsed = self.parse(raw_data)
            parsed_list.append({
                'sendTime': raw_data.get('sendTime', ''),
                'parsed': parsed
            })
        
        # 第二步：检查是否包含枪号数据
        has_gun_data = any(
            item['parsed'] and 'gunNo' in item['parsed'] 
            for item in parsed_list
        )
        
        if not has_gun_data:
            # 没有枪号数据,使用默认的简单拼接
            return super().parse_window(window_data)
        
        # 第三步：枪号对齐
        aligned_data = self._align_gun_data(parsed_list)
        
        # 第四步：组装返回结果
        result = {
            'sendTime': [item['sendTime'] for item in aligned_data]
        }
        
        # 添加统一的gunNo（所有时间点相同）
        if aligned_data and 'gunNo' in aligned_data[0]['data']:
            result['gunNo'] = aligned_data[0]['data']['gunNo']
        
        # 添加其他字段（时序列表）
        for item in aligned_data:
            for key, value in item['data'].items():
                if key == 'gunNo':
                    continue  # gunNo已经添加过了
                if key not in result:
                    result[key] = []
                result[key].append(value)
        
        return result
    
    def _align_gun_data(self, parsed_list):
        """
        对齐枪号数据,确保所有时间点使用相同的枪号顺序
        
        Args:
            parsed_list: 列表,每个元素为 {'sendTime': str, 'parsed': dict}
        
        Returns:
            对齐后的数据列表,每个元素为 {'sendTime': str, 'data': dict}
        """
        # 收集所有出现过的枪号,保持首次出现的顺序
        all_gun_nos = []
        for item in parsed_list:
            if item['parsed'] and 'gunNo' in item['parsed']:
                gun_nos = item['parsed']['gunNo']
                for gun in gun_nos:
                    if gun not in all_gun_nos:
                        all_gun_nos.append(gun)
        
        if not all_gun_nos:
            return [{'sendTime': item['sendTime'], 'data': item['parsed'] or {}} 
                    for item in parsed_list]
        
        # 对每个时间点的数据进行对齐
        aligned_data = []
        
        for i, item in enumerate(parsed_list):
            aligned_item = {
                'sendTime': item['sendTime'],
                'data': {}
            }
            
            if not item['parsed']:
                # 解析失败,所有字段用默认值填充
                aligned_item['data']['gunNo'] = all_gun_nos
                aligned_item['data']['hostCode'] = [''] * len(all_gun_nos)
                aligned_item['data']['status'] = ['unknown'] * len(all_gun_nos)
                aligned_data.append(aligned_item)
                continue
            
            parsed = item['parsed']
            current_gun_nos = parsed.get('gunNo', [])
            
            # 创建当前时间点的枪号到索引的映射
            gun_index_map = {gun: idx for idx, gun in enumerate(current_gun_nos)}
            
            # 对齐gunNo
            aligned_item['data']['gunNo'] = all_gun_nos
            
            # 对齐其他字段（如hostCode、status等）
            for key, value in parsed.items():
                if key == 'gunNo':
                    continue
                
                if isinstance(value, list) and len(value) == len(current_gun_nos):
                    # 这是与枪号对应的列表数据,需要对齐
                    aligned_values = []
                    
                    for gun in all_gun_nos:
                        if gun in gun_index_map:
                            # 当前时间点有这个枪的数据
                            idx = gun_index_map[gun]
                            aligned_values.append(value[idx])
                        else:
                            # 当前时间点没有这个枪的数据,进行插值
                            interpolated_value = self._interpolate_value(
                                parsed_list, i, gun, key, all_gun_nos
                            )
                            aligned_values.append(interpolated_value)
                    
                    aligned_item['data'][key] = aligned_values
                else:
                    # 非列表数据或长度不匹配,直接使用
                    aligned_item['data'][key] = value
            
            aligned_data.append(aligned_item)
        
        return aligned_data
    
    def _interpolate_value(self, parsed_list, current_idx, gun_no, field, all_gun_nos):
        """
        对缺失的枪号数据进行插值
        
        策略：
        1. 优先使用前向填充或后向填充
        2. 对于字符串类型字段,使用默认值
        
        Args:
            parsed_list: 所有时间点的数据列表
            current_idx: 当前时间点的索引
            gun_no: 需要插值的枪号
            field: 字段名（如'hostCode', 'status'）
            all_gun_nos: 所有枪号列表
        
        Returns:
            插值结果
        """
        prev_value = None
        next_value = None
        
        # 向前查找
        for i in range(current_idx - 1, -1, -1):
            item = parsed_list[i]
            if item['parsed'] and 'gunNo' in item['parsed']:
                gun_nos = item['parsed']['gunNo']
                if gun_no in gun_nos:
                    idx = gun_nos.index(gun_no)
                    values = item['parsed'].get(field, [])
                    if idx < len(values):
                        prev_value = values[idx]
                        break
        
        # 向后查找
        for i in range(current_idx + 1, len(parsed_list)):
            item = parsed_list[i]
            if item['parsed'] and 'gunNo' in item['parsed']:
                gun_nos = item['parsed']['gunNo']
                if gun_no in gun_nos:
                    idx = gun_nos.index(gun_no)
                    values = item['parsed'].get(field, [])
                    if idx < len(values):
                        next_value = values[idx]
                        break
        
        # 根据找到的值进行插值
        if prev_value is not None:
            # 前向填充
            return prev_value
        elif next_value is not None:
            # 后向填充
            return next_value
        else:
            # 使用默认值
            if field == 'hostCode':
                return ''
            elif field == 'status':
                return 'unknown'
            else:
                return ''
