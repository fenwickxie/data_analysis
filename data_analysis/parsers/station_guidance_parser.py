from ..parser_base import ParserBase

class StationGuidanceParser(ParserBase):
    def parse(self, raw_data):
        # 解析场站指引数据，依赖负载预测和评价模块
        return {
            'guidance_info': raw_data.get('guidance_info'),
            'load_forecast': raw_data.get('load_forecast'),
            'power_gap': raw_data.get('power_gap'),
            'station_cost': raw_data.get('station_cost'),
            'timestamp': raw_data.get('timestamp'),
        }
