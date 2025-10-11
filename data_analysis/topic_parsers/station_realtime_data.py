from ..parser_base import ParserBase

class StationRealtimeDataParser(ParserBase):
    def parse(self, raw_data):
        # 解析SCHEDULE-STATION-REALTIME-DATA
        return {
            'station_id': raw_data.get('station_id'),
            'gun_id': raw_data.get('gun_id'),
            'history_curve_gun_avg': raw_data.get('history_curve_gun_avg'),
            'history_curve_gun_max': raw_data.get('history_curve_gun_max'),
            'history_curve_station_avg': raw_data.get('history_curve_station_avg'),
            'history_curve_station_max': raw_data.get('history_curve_station_max'),
        }
