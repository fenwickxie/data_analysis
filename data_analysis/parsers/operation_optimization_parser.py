from ..parser_base import ParserBase

class OperationOptimizationParser(ParserBase):
    def parse(self, raw_data):
        # 解析运行优化及配置模块数据
        return {
            'storage_power': raw_data.get('storage_power'),
            'charge_power': raw_data.get('charge_power'),
            'timestamp': raw_data.get('timestamp'),
        }
