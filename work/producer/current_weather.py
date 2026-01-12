# -*- coding: utf-8 -*-
import sys
import time
import json
import requests
from kafka import KafkaProducer

if len(sys.argv) != 3:
    print("Usage: python current_weather.py <latitude> <longitude>")
    sys.exit(1)

latitude = sys.argv[1]
longitude = sys.argv[2]

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "weather_stream"

print(f"Streaming météo pour ({latitude}, {longitude}) vers Kafka...\n")

while True:
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={latitude}&longitude={longitude}"
        "&current_weather=true"
    )

    response = requests.get(url)
    data = response.json()

    if "current_weather" in data:
        weather = data["current_weather"]

        message = {
            "latitude": latitude,
            "longitude": longitude,
            "temperature": weather["temperature"],
            "windspeed": weather["windspeed"],
            "winddirection": weather["winddirection"],
            "time": weather["time"]
        }

        producer.send(topic, message)
        print(f"Envoyé : {message}")

    time.sleep(10)  # envoi toutes les 10 secondes
