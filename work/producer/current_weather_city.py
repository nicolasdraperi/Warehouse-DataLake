# -*- coding: utf-8 -*-

import sys
import time
import json
import requests
from kafka import KafkaProducer

if len(sys.argv) != 3:
    print("Usage: python current_weather_city.py <city> <country>")
    sys.exit(1)

city = sys.argv[1]
country = sys.argv[2]

# Geocoding Open-Meteo
geo_url = (
    "https://geocoding-api.open-meteo.com/v1/search"
    f"?name={city}&country={country}&count=1"
)

geo_response = requests.get(geo_url).json()

if "results" not in geo_response:
    print("Ville ou pays non trouvé")
    sys.exit(1)

location = geo_response["results"][0]
latitude = location["latitude"]
longitude = location["longitude"]

print(f"Localisation trouvée : {city}, {country} ({latitude}, {longitude})")

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "weather_stream"

# Boucle streaming météo
while True:
    weather_url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={latitude}&longitude={longitude}"
        "&current_weather=true"
    )

    weather_response = requests.get(weather_url).json()

    if "current_weather" in weather_response:
        weather = weather_response["current_weather"]

        message = {
            "city": city,
            "country": country,
            "latitude": latitude,
            "longitude": longitude,
            "temperature": weather["temperature"],
            "windspeed": weather["windspeed"],
            "winddirection": weather["winddirection"],
            "time": weather["time"]
        }

        producer.send(topic, message)
        print(f"Envoyé : {message}")

    time.sleep(10)
