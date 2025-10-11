from ..parser_base import ParserBase

class DeviceMeterParser(ParserBase):
    def parse(self, raw_data):
        # 解析SCHEDULE-DEVICE-METER
        return {
            'meter_id': raw_data.get('meter_id'),
            'current_power': raw_data.get('current_power'),
            'rated_power_limit': raw_data.get('rated_power_limit'),
        }
