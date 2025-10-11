from ..parser_base import ParserBase

class DeviceHostParser(ParserBase):
    def parse(self, raw_data):
        # 解析SCHEDULE-DEVICE-HOST
        return {
            'host_id': raw_data.get('host_id'),
            'acdc_status': raw_data.get('acdc_status'),
            'dcdc_input_power': raw_data.get('dcdc_input_power'),
            'acdc_input_power': raw_data.get('acdc_input_power'),
        }
