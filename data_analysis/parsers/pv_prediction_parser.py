from ..parser_base import ParserBase

class PvPredictionParser(ParserBase):
    def parse(self, raw_data):
        # 解析光伏预测数据
        return {
            'pv_forecast': raw_data.get('pv_forecast'),
            'pv_actual': raw_data.get('pv_actual'),
            'timestamp': raw_data.get('timestamp'),
        }
