from ..parser_base import ParserBase

class EnvironmentCalendarParser(ParserBase):
    def parse(self, raw_data):
        # 解析SCHEDULE-ENVIRONMENT-CALENDAR
        return {
            'workday_code': raw_data.get('workday_code'),
            'holiday_code': raw_data.get('holiday_code'),
        }
