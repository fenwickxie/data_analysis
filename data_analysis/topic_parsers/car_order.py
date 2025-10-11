from ..parser_base import ParserBase

class CarOrderParser(ParserBase):
    def parse(self, raw_data):
        # 解析SCHEDULE-CAR-ORDER
        return {
            'station_id': raw_data.get('station_id'),
            'order_id': raw_data.get('order_id'),
            'charger_id': raw_data.get('charger_id'),
            'gun_id': raw_data.get('gun_id'),
            'charger_rated_current': raw_data.get('charger_rated_current'),
            'start_time': raw_data.get('start_time'),
            'end_time': raw_data.get('end_time'),
            'start_SOC': raw_data.get('start_SOC'),
            'current_SOC': raw_data.get('current_SOC'),
            'demand_voltage': raw_data.get('demand_voltage'),
            'demand_current': raw_data.get('demand_current'),
            'mileage': raw_data.get('mileage'),
            'car_model': raw_data.get('car_model'),
            'battery_capacity': raw_data.get('battery_capacity'),
        }
