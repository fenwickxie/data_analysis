from kafka import KafkaProducer
import json

class KafkaProducerClient:
    def __init__(self, config):
        self.producer = KafkaProducer(
            bootstrap_servers=config['bootstrap_servers'],
            value_serializer=lambda m: json.dumps(m, ensure_ascii=False).encode('utf-8')
        )

    def send(self, topic, value):
        self.producer.send(topic, value=value)
        self.producer.flush()

    def close(self):
        self.producer.close()
