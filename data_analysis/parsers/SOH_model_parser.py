from ..parser_base import ParserBase

class SOHModelParser(ParserBase):
    def parse(self, raw_data):
        # 解析SOH模块数据
        return {
            'SOH': raw_data.get('SOH'),
            'SOH_history': raw_data.get('SOH_history'),
            'timestamp': raw_data.get('timestamp'),
        }
