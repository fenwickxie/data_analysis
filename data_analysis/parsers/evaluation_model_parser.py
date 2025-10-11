from ..parser_base import ParserBase

class EvaluationModelParser(ParserBase):
    def parse(self, raw_data):
        # 解析评价模块数据
        return {
            'power_gap': raw_data.get('power_gap'),
            'station_cost': raw_data.get('station_cost'),
            'timestamp': raw_data.get('timestamp'),
        }
