import json
from confluent_kafka import Producer

class BaseProducer:
    def __init__(self, topic, bootstrap_servers="localhost:9092"):
        self.topic = topic
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers
        })

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"Delivery failed: {err}")
        else:
            print(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

    def send(self, data: dict):
        self.producer.produce(
            self.topic,
            value=json.dumps(data),
            callback=self.delivery_report
        )
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()