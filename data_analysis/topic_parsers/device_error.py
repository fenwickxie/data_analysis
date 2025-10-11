from ..parser_base import ParserBase

class DeviceErrorParser(ParserBase):
    def parse(self, raw_data):
        # 解析SCHEDULE-DEVICE-ERROR
        return {
            'station_id': raw_data.get('station_id'),
            'host_error': raw_data.get('host_error'),
            'ac_error': raw_data.get('ac_error'),
            'dc_error': raw_data.get('dc_error'),
            'terminal_error': raw_data.get('terminal_error'),
            'storage_error': raw_data.get('storage_error'),
        }
