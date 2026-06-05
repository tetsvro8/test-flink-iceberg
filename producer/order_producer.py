import json
import os
import random
import time
from datetime import datetime

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

fake = Faker()

bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")


def create_producer():
    for attempt in range(10):
        try:
            return KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
        except NoBrokersAvailable:
            print(f"Kafka not ready, retrying ({attempt + 1}/10)...")
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka after 10 attempts")


producer = create_producer()
order_id = 1

print(f"Connected to Kafka at {bootstrap_servers}. Producing events...")

while True:
    event = {
        "order_id": order_id,
        "user_id": random.randint(1, 1000),
        "product_id": random.randint(1, 100),
        "amount": round(random.uniform(100.0, 50000.0), 2),
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    producer.send("order_events", event)
    print(f"sent: {event}")
    order_id += 1
    time.sleep(0.2)
