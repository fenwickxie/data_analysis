# Topic Parsers 优化指南

## 优化方案说明

为了避免重复配置，统一使用 `config.py` 中的 `TOPIC_DETAIL['fields']` 配置来自动解析字段。

## 新的解析器实现方式

### 1. 使用 ConfigBasedParser（推荐）

最简单的方式，只需要指定 topic 名称：

```python
from ..parser_base import ConfigBasedParser

class StationParamParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name='SCHEDULE-STATION-PARAM')
```

**优点**：
- ✅ 零重复配置，字段自动从 config.py 读取
- ✅ 统一维护，修改字段只需在 config.py 中修改一处
- ✅ 字段名保持 camelCase，与 Kafka 消息格式一致
- ✅ 自动处理空值和缺失字段

### 2. 自定义扩展（需要额外处理时）

如果需要字段转换、计算衍生字段等自定义逻辑：

```python
from ..parser_base import ConfigBasedParser

class StationParamParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name='SCHEDULE-STATION-PARAM')
    
    def parse(self, raw_data):
        # 先调用父类方法获取基础字段
        parsed_data = super().parse(raw_data)
        
        if parsed_data:
            # 添加自定义处理逻辑
            # 例如：类型转换
            if parsed_data.get('gunNum'):
                parsed_data['gunNum'] = int(parsed_data['gunNum'])
            
            # 例如：计算衍生字段
            parsed_data['totalCapacity'] = (
                parsed_data.get('gridCapacity', 0) + 
                parsed_data.get('storageCapacity', 0)
            )
            
            # 例如：字段重命名（如果确实需要）
            parsed_data['latitude'] = parsed_data.pop('stationLat', None)
            parsed_data['longitude'] = parsed_data.pop('stationLng', None)
        
        return parsed_data
```

## 批量更新步骤

### 方式一：手动逐个更新

对于每个 topic_parser 文件，按照以下模板更新：

```python
from ..parser_base import ConfigBasedParser

class YourParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name='YOUR-TOPIC-NAME')
```

### 方式二：使用批量更新脚本

运行提供的 `update_all_parsers.py` 脚本自动更新所有解析器。

## Topic 与 Parser 映射表

| Topic 名称 | Parser 文件 | Parser 类名 |
|-----------|------------|------------|
| SCHEDULE-STATION-PARAM | station_param.py | StationParamParser |
| SCHEDULE-STATION-REALTIME-DATA | station_realtime_data.py | StationRealtimeDataParser |
| SCHEDULE-ENVIRONMENT-CALENDAR | environment_calendar.py | EnvironmentCalendarParser |
| SCHEDULE-DEVICE-METER | device_meter.py | DeviceMeterParser |
| SCHEDULE-DEVICE-GUN | device_gun.py | DeviceGunParser |
| SCHEDULE-CAR-ORDER | car_order.py | CarOrderParser |
| SCHEDULE-CAR-PRICE | car_price.py | CarPriceParser |
| SCHEDULE-DEVICE-ERROR | device_error.py | DeviceErrorParser |
| SCHEDULE-DEVICE-HOST-DCDC | device_host.py | DeviceHostDCDCParser |
| SCHEDULE-DEVICE-HOST-ACDC | device_host.py | DeviceHostACDCParser |
| SCHEDULE-DEVICE-STORAGE | device_storage.py | DeviceStorageParser |
| MODULE-OUTPUT-* | model_output.py | ModelOutputParser |

## 注意事项

1. **字段名格式**：统一使用 camelCase（如 `stationId`），与 Kafka 消息保持一致
2. **config.py 配置**：确保 `TOPIC_DETAIL` 中配置了完整的 `fields` 列表
3. **向后兼容**：旧的手动解析方式仍然可用，可以逐步迁移
4. **测试验证**：更新后务必测试解析结果是否正确

## 示例：完整的更新前后对比

### 更新前（手动配置）

```python
from ..parser_base import ParserBase

class StationParamParser(ParserBase):
    def parse(self, raw_data):
        return {
            'station_id': raw_data.get('stationId'),  # 字段名不一致
            'lat': raw_data.get('lat'),
            'lng': raw_data.get('lng'),
            # ... 重复配置每个字段
        }
```

**问题**：
- ❌ 字段需要手动配置，容易遗漏
- ❌ 字段名不一致（config 是 camelCase，parser 用 snake_case）
- ❌ 修改字段需要在多处同步修改

### 更新后（自动配置）

```python
from ..parser_base import ConfigBasedParser

class StationParamParser(ConfigBasedParser):
    def __init__(self):
        super().__init__(topic_name='SCHEDULE-STATION-PARAM')
```

**优点**：
- ✅ 自动从 config.py 读取字段，无需手动配置
- ✅ 字段名统一使用 camelCase
- ✅ 只需在 config.py 一处维护字段配置

## 测试方法

```python
from d_a.topic_parsers.station_param import StationParamParser

# 创建解析器实例
parser = StationParamParser()

# 模拟 Kafka 消息数据
raw_data = {
    'stationId': 'station001',
    'stationLng': 116.4074,
    'stationLat': 39.9042,
    'gunNum': 10,
    'gridCapacity': 500,
    # ... 其他字段
}

# 解析数据
parsed = parser.parse(raw_data)
print(parsed)

# 验证字段是否正确提取
assert parsed['stationId'] == 'station001'
assert parsed['gunNum'] == 10
```

## 后续优化建议

1. **添加字段验证**：在 ConfigBasedParser 中添加必填字段检查
2. **类型转换**：支持配置中指定字段类型，自动转换
3. **默认值支持**：为缺失字段提供默认值配置
4. **字段映射**：支持配置中定义字段映射关系（如果 Kafka 字段名与期望输出不同）
