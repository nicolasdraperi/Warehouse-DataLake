# -*- coding: utf-8 -*-
import sys
from kafka import KafkaConsumer

if len(sys.argv) != 2:
    print("Usage: python kafka_consumer.py <topic>")
    sys.exit(1)

topic = sys.argv[1]

consumer = KafkaConsumer(
    topic,
    bootstrap_servers="localhost:29092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: x.decode("utf-8")
)

print(f"En écoute sur le topic '{topic}'...\n")

for message in consumer:
    print(f"Message reçu : {message.value}")
