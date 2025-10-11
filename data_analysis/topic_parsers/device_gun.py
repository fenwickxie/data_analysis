from ..parser_base import ParserBase

class DeviceGunParser(ParserBase):
    def parse(self, raw_data):
        # 解析SCHEDULE-DEVICE-GUN
        return {
            'host_id': raw_data.get('host_id'),
            'gun_id': raw_data.get('gun_id'),
            'gun_status': raw_data.get('gun_status'),
        }
