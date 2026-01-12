import requests
import json
import sys
from kafka import KafkaProducer
from datetime import date

if len(sys.argv) != 3:
    print("Usage: python weather_history_producer.py <city> <country>")
    sys.exit(1)

city = sys.argv[1]
country = sys.argv[2]

# Geocoding
geo_url = f"https://geocoding-api.open-meteo.com/v1/search?name={city}&country={country}&count=1"
geo = requests.get(geo_url).json()["results"][0]
lat, lon = geo["latitude"], geo["longitude"]

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

start_date = "2014-01-01"
end_date = date.today().isoformat()

archive_url = (
    "https://archive-api.open-meteo.com/v1/archive"
    f"?latitude={lat}&longitude={lon}"
    f"&start_date={start_date}&end_date={end_date}"
    "&daily=temperature_2m_max,temperature_2m_min,windspeed_10m_max"
    "&timezone=UTC"
)

data = requests.get(archive_url).json()

for i, day in enumerate(data["daily"]["time"]):
    record = {
        "city": city,
        "country": country,
        "date": day,
        "temp_max": data["daily"]["temperature_2m_max"][i],
        "temp_min": data["daily"]["temperature_2m_min"][i],
        "wind_max": data["daily"]["windspeed_10m_max"][i]
    }

    producer.send("weather_history_raw", record)

producer.flush()
print("Historique météo envoyé dans Kafka")
