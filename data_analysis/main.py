import time
import threading
from data_analysis.config import KAFKA_CONFIG, TOPIC_TO_MODULES
from data_analysis.kafka_client import KafkaConsumerClient
from data_analysis.dispatcher import DataDispatcher

ALL_TOPICS = list(TOPIC_TO_MODULES.keys())

def station_worker(station_id, dispatcher, stop_event):
    """每个场站独立线程，定期输出各模块数据，可扩展为推送/写库/上传Kafka等"""
    while not stop_event.is_set():
        outputs = dispatcher.get_all_outputs(station_id)
        # 这里可根据需要输出、推送或存储outputs
        # print(f"[场站{station_id}] 最新数据: {outputs}")
        time.sleep(2)

def main():
    dispatcher = DataDispatcher()
    consumer = KafkaConsumerClient(ALL_TOPICS, KAFKA_CONFIG)
    print('数据解析模块启动，监听Kafka...')
    station_threads = {}
    stop_events = {}
    try:
        while True:
            msg_pack = consumer.poll(timeout_ms=1000)
            for tp, msgs in msg_pack.items():
                topic = tp.topic
                for msg in msgs:
                    # station_id需从msg.value中提取，假设为msg.value['station_id']
                    station_id = msg.value.get('station_id') or msg.value.get('host_id') or msg.value.get('meter_id')
                    if not station_id:
                        continue
                    dispatcher.update_topic_data(station_id, topic, msg.value)
                    print(f'更新场站{station_id} topic {topic} 数据: {msg.value}')
                    # 启动新场站线程
                    if station_id not in station_threads:
                        stop_event = threading.Event()
                        stop_events[station_id] = stop_event
                        t = threading.Thread(target=station_worker, args=(station_id, dispatcher, stop_event), daemon=True)
                        t.start()
                        station_threads[station_id] = t
            # 定期清理过期数据，防止内存泄漏
            dispatcher.clean_expired()
            time.sleep(1)
    except KeyboardInterrupt:
        print('退出数据解析模块...')
    finally:
        consumer.close()
        for e in stop_events.values():
            e.set()
