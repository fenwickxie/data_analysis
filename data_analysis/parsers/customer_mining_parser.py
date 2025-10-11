from ..parser_base import ParserBase

class CustomerMiningParser(ParserBase):
    def parse(self, raw_data):
        # 解析客户挖掘模块数据
        return {
            'customer_info': raw_data.get('customer_info'),
            'timestamp': raw_data.get('timestamp'),
        }
