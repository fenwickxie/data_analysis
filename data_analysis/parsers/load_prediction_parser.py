from ..parser_base import ParserBase

class LoadPredictionParser(ParserBase):
    def parse(self, raw_data):
        # 解析负载预测数据
        return {
            'load_forecast': raw_data.get('load_forecast'),
            'timestamp': raw_data.get('timestamp'),
        }
