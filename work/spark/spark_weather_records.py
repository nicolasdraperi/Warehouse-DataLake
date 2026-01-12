# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, to_json, lit

spark = SparkSession.builder.appName("WeatherRecords").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Lecture HDFS historique
df = spark.read \
    .option("basePath", "hdfs://namenode:9000/hdfs-data/weather_history_raw") \
    .json("hdfs://namenode:9000/hdfs-data/weather_history_raw/*/*/*.json")

# Calcul des records
hottest_row = df.orderBy(col("temp_max").desc()).first()
coldest_row = df.orderBy(col("temp_min").asc()).first()
wind_row = df.orderBy(col("wind_max").desc()).first()

# Construction du DataFrame final
records = spark.createDataFrame([{
    "city": hottest_row["city"],
    "country": hottest_row["country"],
    "hottest_day": {
        "date": hottest_row["date"],
        "value": hottest_row["temp_max"]
    },
    "coldest_day": {
        "date": coldest_row["date"],
        "value": coldest_row["temp_min"]
    },
    "strongest_wind": {
        "date": wind_row["date"],
        "value": wind_row["wind_max"]
    }
}])

# Sortie Kafka
output = records.select(to_json(struct("*")).alias("value"))

output.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "weather_records") \
    .save()

print("✅ Records climatiques envoyés dans Kafka")
