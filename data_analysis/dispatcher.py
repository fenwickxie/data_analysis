# 数据分发与依赖处理
from .config import MODULE_DEPENDENCIES
from .parsers import *

import threading
import time
from .config import TOPIC_TO_MODULES, MODULE_TO_TOPICS
from .topic_parsers import *

TOPIC_PARSER_MAP = {
    'SCHEDULE-STATION-PARAM': StationParamParser(),
    'SCHEDULE-STATION-REALTIME-DATA': StationRealtimeDataParser(),
    'SCHEDULE-ENVIRONMENT-CALENDAR': EnvironmentCalendarParser(),
    'SCHEDULE-DEVICE-METER': DeviceMeterParser(),
    'SCHEDULE-DEVICE-GUN': DeviceGunParser(),
    'SCHEDULE-CAR-ORDER': CarOrderParser(),
    'SCHEDULE-CAR-PRICE': CarPriceParser(),
    'SCHEDULE-DEVICE-ERROR': DeviceErrorParser(),
    'SCHEDULE-DEVICE-HOST': DeviceHostParser(),
    'SCHEDULE-DEVICE-STORAGE': DeviceStorageParser(),
}

class DataDispatcher:
    def __init__(self, data_expire_seconds=600):
        # 各模块解析器注册
        self.parsers = {
            'electricity_price': ElectricityPriceParser(),
            'load_prediction': LoadPredictionParser(),
            'pv_prediction': PvPredictionParser(),
            'thermal_management': ThermalManagementParser(),
            'station_guidance': StationGuidanceParser(),
            'evaluation_model': EvaluationModelParser(),
            'SOH_model': SOHModelParser(),
            'operation_optimization': OperationOptimizationParser(),
            'customer_mining': CustomerMiningParser(),
        }
        # 多场站缓存结构: {station_id: {topic: (data, ts)}}
        self.data_cache = {}
        self.data_expire_seconds = data_expire_seconds
        self.lock = threading.Lock()

    def update_topic_data(self, station_id, topic, raw_data):
        # 更新指定场站、topic的数据
        with self.lock:
            if station_id not in self.data_cache:
                self.data_cache[station_id] = {k: (None, 0) for k in TOPIC_PARSER_MAP.keys()}
            self.data_cache[station_id][topic] = (raw_data, time.time())

    def get_module_input(self, station_id, module):
        # 整合该场站所有topic数据，组装为模块输入
        with self.lock:
            if station_id not in self.data_cache:
                return None
            input_data = {}
            for topic in MODULE_TO_TOPICS.get(module, []):
                topic_data = self.data_cache[station_id].get(topic, (None, 0))[0]
                if topic_data:
                    parsed = TOPIC_PARSER_MAP[topic].parse(topic_data)
                    input_data.update(parsed)
            # 依赖其他模块输出
            deps = MODULE_DEPENDENCIES.get(module, [])
            for dep in deps:
                dep_input = self.get_module_input(station_id, dep)
                if dep_input:
                    input_data.update(dep_input)
            return self.parsers[module].parse(input_data)

    def get_all_outputs(self, station_id):
        # 获取指定场站所有模块的输出
        with self.lock:
            if station_id not in self.data_cache:
                return {}
            return {m: self.get_module_input(station_id, m) for m in self.parsers.keys()}

    def clean_expired(self):
        # 清理过期数据，防止内存泄漏
        now = time.time()
        with self.lock:
            expired_stations = []
            for station_id, topic_map in self.data_cache.items():
                for t, (data, ts) in topic_map.items():
                    if data and now - ts > self.data_expire_seconds:
                        topic_map[t] = (None, 0)
                # 如果所有topic都无数据则移除场站
                if all(v[0] is None for v in topic_map.values()):
                    expired_stations.append(station_id)
            for sid in expired_stations:
                del self.data_cache[sid]
