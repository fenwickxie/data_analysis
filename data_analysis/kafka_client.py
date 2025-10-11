# Kafka消费客户端封装
from kafka import KafkaConsumer
import json

class KafkaConsumerClient:
    def __init__(self, topics, config):
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=config['bootstrap_servers'],
            group_id=config['group_id'],
            auto_offset_reset=config.get('auto_offset_reset', 'latest'),
            enable_auto_commit=config.get('enable_auto_commit', True),
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

    def poll(self, timeout_ms=1000):
        return self.consumer.poll(timeout_ms=timeout_ms)

    def close(self):
        self.consumer.close()
