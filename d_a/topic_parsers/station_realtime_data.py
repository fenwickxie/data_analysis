#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
充电站实时数据解析器
包含枪号对齐和数据插值逻辑
处理嵌套的 gunPower 结构
支持基于时间的窗口补全（1小时间隔）
"""

from ..parser_base import ConfigBasedParser
from datetime import datetime, timedelta

class StationRealtimeDataParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name='SCHEDULE-STATION-REALTIME-DATA')
    
    def parse(self, raw_data):
        """
        解析实时数据,提取嵌套的 gunPower 字段
        
        Args:
            raw_data: {
                'gunPower': {
                    'gunNo': ['05', '01', '02'],
                    'outputPowerPerGunAvg': [34.29, 2.73, 88.48],
                    'outputPowerPerGunMax': [100.8, 7.0, 135.2]
                },
                'hostCode': '52000000000088',
                'sendTime': '2025-11-04 09:00:00',
                'stationId': '1881540927845654529'
            }
            
        Returns:
            dict: 扁平化后的数据,包含 gunNo, outputPowerPerGunAvg, outputPowerPerGunMax 等
        """
        if not raw_data:
            return None
        
        parsed_data = {}
        
        # 提取 gunPower 对象中的字段
        gun_power = raw_data.get('gunPower', {})
        if gun_power:
            parsed_data['gunNo'] = gun_power.get('gunNo', [])
            parsed_data['outputPowerPerGunAvg'] = gun_power.get('outputPowerPerGunAvg', [])
            parsed_data['outputPowerPerGunMax'] = gun_power.get('outputPowerPerGunMax', [])
        
        # 提取其他顶层字段
        for field in ['outputPowerPerStationMax', 'outputPowerPerStationAvg']:
            if field in raw_data:
                parsed_data[field] = raw_data[field]
        
        return parsed_data if parsed_data else None
    
    def parse_window(self, window_data):
        """
        重写窗口解析方法,实现枪号对齐和时间窗口补全
        
        功能：
        1. 按时间补全缺失的时间点（1小时间隔）
        2. 枪号对齐（确保所有时间点使用相同的枪号顺序）
        3. 缺失数据填充0
        
        Args:
            window_data: 窗口内的原始数据列表（可能不完整）
            
        Returns:
            dict: 对齐后的数据,格式为：
            {
                'gunNo': ['01', '02', '05', ...],  # 统一的枪号顺序
                'outputPowerPerGunAvg': [[2.73, 88.48, 34.29], [2.27, 44.17, 0], ...],
                'sendTime': ['2025-11-04 09:00:00', '2025-11-04 10:00:00', ...],
                ...
            }
        """
        if not window_data:
            return {}
        
        # 第一步：补全时间窗口（根据 window_size）
        complete_window_data = self._fill_time_window(window_data)
        
        # 第二步：解析所有数据
        parsed_list = []
        for raw_data in complete_window_data:
            parsed = self.parse(raw_data) if raw_data else None
            parsed_list.append({
                'sendTime': raw_data.get('sendTime', '') if raw_data else '',
                'parsed': parsed
            })
        
        # 第三步：检查是否包含枪号数据
        has_gun_data = any(
            item['parsed'] and 'gunNo' in item['parsed'] 
            for item in parsed_list
        )
        
        if not has_gun_data:
            # 没有枪号数据,使用默认的简单拼接
            return super().parse_window(window_data)
        
        # 第四步：枪号对齐
        aligned_data = self._align_gun_data(parsed_list)
        
        # 第五步：组装返回结果
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
    
    def _fill_time_window(self, window_data):
        """
        根据 sendTime 补全时间窗口,填充缺失的时间点
        
        规则：
        - 时间间隔：1小时
        - 结束时间：最晚的 sendTime（最新数据）
        - 窗口大小：从 config.TOPIC_DETAIL 读取（默认7天=168小时）
        - 方向：从最晚时间往前推（获取过去N小时的历史数据）
        - 缺失时间点：填充占位符（后续会被填充为0）
        
        Args:
            window_data: 原始窗口数据列表
            
        Returns:
            list: 补全后的窗口数据列表（长度等于 window_size,按时间升序）
        """
        if not window_data:
            return []
        
        # 获取窗口大小配置
        from ..config import TOPIC_DETAIL
        window_size = TOPIC_DETAIL.get(self.topic_name, {}).get('window_size', 168)  # 默认7天=168小时
        
        # 提取所有时间戳并排序
        time_data_map = {}  # {sendTime: raw_data}
        for item in window_data:
            send_time = item.get('sendTime')
            if send_time:
                time_data_map[send_time] = item
        
        if not time_data_map:
            return []
        
        # 获取最晚时间作为结束点（最新数据）
        sorted_times = sorted(time_data_map.keys())
        end_time_str = sorted_times[-1]  # 最晚时间
        
        try:
            # 解析结束时间
            end_time = datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            # 时间格式不正确,返回原始数据
            return window_data
        
        # 计算起始时间（往前推 window_size-1 小时）
        start_time = end_time - timedelta(hours=window_size - 1)
        
        # 生成完整的时间序列（1小时间隔,从旧到新）
        complete_data = []
        for i in range(window_size):
            current_time = start_time + timedelta(hours=i)
            current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
            
            # 检查该时间点是否有数据
            if current_time_str in time_data_map:
                complete_data.append(time_data_map[current_time_str])
            else:
                # 缺失数据,填充占位符（包含 sendTime,数据为空）
                complete_data.append({
                    'sendTime': current_time_str,
                    'stationId': window_data[0].get('stationId', ''),  # 使用第一条数据的场站ID
                    # 其他字段留空,后续填充0
                })
        
        return complete_data
    
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
                # 解析失败,所有字段用0填充
                aligned_item['data']['gunNo'] = all_gun_nos
                aligned_item['data']['outputPowerPerGunAvg'] = [0.0] * len(all_gun_nos)
                aligned_item['data']['outputPowerPerGunMax'] = [0.0] * len(all_gun_nos)
                aligned_data.append(aligned_item)
                continue
            
            parsed = item['parsed']
            current_gun_nos = parsed.get('gunNo', [])
            
            # 创建当前时间点的枪号到索引的映射
            gun_index_map = {gun: idx for idx, gun in enumerate(current_gun_nos)}
            
            # 对齐gunNo
            aligned_item['data']['gunNo'] = all_gun_nos
            
            # 对齐其他字段（如outputPowerPerGunAvg、outputPowerPerGunMax等）
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
        对缺失的枪号数据进行填充
        
        填充策略：
        - 直接填充 0（简单清晰,避免插值引入误差）
        
        如需其他填充策略（如前向填充、线性插值等）,可修改此方法。
        
        Args:
            parsed_list: 所有时间点的数据列表
            current_idx: 当前时间点的索引
            gun_no: 需要填充的枪号
            field: 字段名（如'outputPowerPerGunAvg'）
            all_gun_nos: 所有枪号列表
        
        Returns:
            填充值（浮点数）
        """
        # 直接填充 0
        return 0.0
        
        # === 可选的其他填充策略（已注释） ===
        
        # 策略1：前向填充（使用最近的历史值）
        # for i in range(current_idx - 1, -1, -1):
        #     item = parsed_list[i]
        #     if item['parsed'] and 'gunNo' in item['parsed']:
        #         gun_nos = item['parsed']['gunNo']
        #         if gun_no in gun_nos:
        #             idx = gun_nos.index(gun_no)
        #             values = item['parsed'].get(field, [])
        #             if idx < len(values):
        #                 return values[idx]
        # return 0.0
        
        # 策略2：线性插值（使用前后值进行插值）
        # prev_value = None
        # next_value = None
        # prev_distance = 0
        # next_distance = 0
        # 
        # # 向前查找
        # for i in range(current_idx - 1, -1, -1):
        #     item = parsed_list[i]
        #     if item['parsed'] and 'gunNo' in item['parsed']:
        #         gun_nos = item['parsed']['gunNo']
        #         if gun_no in gun_nos:
        #             idx = gun_nos.index(gun_no)
        #             values = item['parsed'].get(field, [])
        #             if idx < len(values):
        #                 prev_value = values[idx]
        #                 prev_distance = current_idx - i
        #                 break
        # 
        # # 向后查找
        # for i in range(current_idx + 1, len(parsed_list)):
        #     item = parsed_list[i]
        #     if item['parsed'] and 'gunNo' in item['parsed']:
        #         gun_nos = item['parsed']['gunNo']
        #         if gun_no in gun_nos:
        #             idx = gun_nos.index(gun_no)
        #             values = item['parsed'].get(field, [])
        #             if idx < len(values):
        #                 next_value = values[idx]
        #                 next_distance = i - current_idx
        #                 break
        # 
        # # 线性插值
        # if prev_value is not None and next_value is not None:
        #     total_distance = prev_distance + next_distance
        #     weight = prev_distance / total_distance
        #     return prev_value * (1 - weight) + next_value * weight
        # elif prev_value is not None:
        #     return prev_value
        # elif next_value is not None:
        #     return next_value
        # 
        # return 0.0

