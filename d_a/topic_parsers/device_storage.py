#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
储能设备解析器
处理单个储能设备的数据（不是数组格式）
"""

from ..parser_base import ConfigBasedParser

class DeviceStorageParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name='SCHEDULE-DEVICE-STORAGE')
    
    def parse(self, raw_data):
        """
        解析储能设备数据
        
        Args:
            raw_data: {
                'stationId': '1881540927845654529',
                'storageCode': 'BDDDDD74DAD2CD6B095D',
                'batteryGroupSoc': 12,
                'batteryGroupSoh': 9,
                'batteryGroupElectric': 11.2,
                'batteryGroupOutPower': 4,
                'maxTemperature': 40.0,
                'minTemperature': 5.0,
                'hostCode': '42000000001900'
            }
            
        Returns:
            dict: 提取的字段数据
        """
        if not raw_data:
            return None
        
        parsed_data = {}
        
        # 提取所有字段
        field_mapping = {
            'storageCode': 'storageId',  # storageCode 映射为 storageId
            'batteryGroupSoc': 'storageSOC',
            'batteryGroupSoh': 'storageSOH',
            'batteryGroupElectric': 'storageCurrent',
            'batteryGroupOutPower': 'storagePower',
            'maxTemperature': 'storageTempMax',
            'minTemperature': 'storageTempMin',
            'hostCode': 'hostId',
        }
        
        for raw_field, target_field in field_mapping.items():
            if raw_field in raw_data:
                parsed_data[target_field] = raw_data[raw_field]
        
        return parsed_data if parsed_data else None
    
    def parse_window(self, window_data):
        """
        重写窗口解析方法，实现储能ID对齐
        
        Args:
            window_data: 窗口内的原始数据列表
            
        Returns:
            dict: 对齐后的数据，格式为：
            {
                'storageId': ['storage1', 'storage2', ...],  # 统一的储能ID顺序
                'storagePower': [[100.5, 200.3, ...], ...],  # 时序列表
                'storageSOC': [[80, 75, ...], ...],
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
        
        # 第二步：检查是否包含储能ID数据
        has_storage_data = any(
            item['parsed'] and 'storageId' in item['parsed'] 
            for item in parsed_list
        )
        
        if not has_storage_data:
            # 没有储能ID数据，使用默认的简单拼接
            return super().parse_window(window_data)
        
        # 第三步：储能ID对齐
        aligned_data = self._align_storage_data(parsed_list)
        
        # 第四步：组装返回结果
        result = {
            'sendTime': [item['sendTime'] for item in aligned_data]
        }
        
        # 添加统一的storageId（所有时间点相同）
        if aligned_data and 'storageId' in aligned_data[0]['data']:
            result['storageId'] = aligned_data[0]['data']['storageId']
        
        # 添加其他字段（时序列表）
        for item in aligned_data:
            for key, value in item['data'].items():
                if key == 'storageId':
                    continue  # storageId已经添加过了
                if key not in result:
                    result[key] = []
                result[key].append(value)
        
        return result
    
    def _align_storage_data(self, parsed_list):
        """
        对齐储能ID数据，确保所有时间点使用相同的储能ID顺序
        
        Args:
            parsed_list: 列表，每个元素为 {'sendTime': str, 'parsed': dict}
        
        Returns:
            对齐后的数据列表，每个元素为 {'sendTime': str, 'data': dict}
        """
        # 收集所有出现过的储能ID，保持首次出现的顺序
        all_storage_ids = []
        for item in parsed_list:
            if item['parsed'] and 'storageId' in item['parsed']:
                storage_ids = item['parsed']['storageId']
                for sid in storage_ids:
                    if sid not in all_storage_ids:
                        all_storage_ids.append(sid)
        
        if not all_storage_ids:
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
                # 解析失败，所有字段用0填充
                aligned_item['data']['storageId'] = all_storage_ids
                aligned_item['data']['storagePower'] = [0.0] * len(all_storage_ids)
                aligned_item['data']['storageCurrent'] = [0.0] * len(all_storage_ids)
                aligned_item['data']['storageTempMax'] = [0.0] * len(all_storage_ids)
                aligned_item['data']['storageTempMin'] = [0.0] * len(all_storage_ids)
                aligned_item['data']['storageSOC'] = [0.0] * len(all_storage_ids)
                aligned_item['data']['storageSOH'] = [0.0] * len(all_storage_ids)
                aligned_item['data']['hostId'] = [''] * len(all_storage_ids)
                aligned_data.append(aligned_item)
                continue
            
            parsed = item['parsed']
            current_storage_ids = parsed.get('storageId', [])
            
            # 创建当前时间点的储能ID到索引的映射
            storage_index_map = {sid: idx for idx, sid in enumerate(current_storage_ids)}
            
            # 对齐storageId
            aligned_item['data']['storageId'] = all_storage_ids
            
            # 对齐其他字段（如storagePower、storageSOC等）
            for key, value in parsed.items():
                if key == 'storageId':
                    continue
                
                if isinstance(value, list) and len(value) == len(current_storage_ids):
                    # 这是与储能ID对应的列表数据，需要对齐
                    aligned_values = []
                    
                    for sid in all_storage_ids:
                        if sid in storage_index_map:
                            # 当前时间点有这个储能的数据
                            idx = storage_index_map[sid]
                            aligned_values.append(value[idx])
                        else:
                            # 当前时间点没有这个储能的数据，进行插值
                            interpolated_value = self._interpolate_value(
                                parsed_list, i, sid, key, all_storage_ids
                            )
                            aligned_values.append(interpolated_value)
                    
                    aligned_item['data'][key] = aligned_values
                else:
                    # 非列表数据或长度不匹配，直接使用
                    aligned_item['data'][key] = value
            
            aligned_data.append(aligned_item)
        
        return aligned_data
    
    def _interpolate_value(self, parsed_list, current_idx, storage_id, field, all_storage_ids):
        """
        对缺失的储能ID数据进行插值
        
        策略：
        1. 优先使用线性插值（如果前后都有数据）
        2. 其次使用前向填充或后向填充
        3. 最后使用默认值
        
        Args:
            parsed_list: 所有时间点的数据列表
            current_idx: 当前时间点的索引
            storage_id: 需要插值的储能ID
            field: 字段名（如'storagePower', 'storageSOC'）
            all_storage_ids: 所有储能ID列表
        
        Returns:
            插值结果
        """
        prev_value = None
        next_value = None
        prev_distance = 0
        next_distance = 0
        
        # 向前查找
        for i in range(current_idx - 1, -1, -1):
            item = parsed_list[i]
            if item['parsed'] and 'storageId' in item['parsed']:
                storage_ids = item['parsed']['storageId']
                if storage_id in storage_ids:
                    idx = storage_ids.index(storage_id)
                    values = item['parsed'].get(field, [])
                    if idx < len(values):
                        prev_value = values[idx]
                        prev_distance = current_idx - i
                        break
        
        # 向后查找
        for i in range(current_idx + 1, len(parsed_list)):
            item = parsed_list[i]
            if item['parsed'] and 'storageId' in item['parsed']:
                storage_ids = item['parsed']['storageId']
                if storage_id in storage_ids:
                    idx = storage_ids.index(storage_id)
                    values = item['parsed'].get(field, [])
                    if idx < len(values):
                        next_value = values[idx]
                        next_distance = i - current_idx
                        break
        
        # 根据找到的值进行插值
        if prev_value is not None and next_value is not None:
            # 数值类型：线性插值
            if isinstance(prev_value, (int, float)) and isinstance(next_value, (int, float)):
                total_distance = prev_distance + next_distance
                weight = prev_distance / total_distance
                return prev_value * (1 - weight) + next_value * weight
            else:
                # 非数值类型：前向填充
                return prev_value
        elif prev_value is not None:
            # 前向填充
            return prev_value
        elif next_value is not None:
            # 后向填充
            return next_value
        else:
            # 使用默认值
            if field == 'hostId':
                return ''
            else:
                return 0.0
