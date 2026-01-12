# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window,
    count, avg, min, max, when
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Spark session
spark = SparkSession.builder \
    .appName("WeatherAggregates") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schéma des données transformées
schema = StructType([
    StructField("latitude", StringType()),
    StructField("longitude", StringType()),
    StructField("temperature", DoubleType()),
    StructField("windspeed", DoubleType()),
    StructField("event_time", TimestampType()),
    StructField("wind_alert_level", StringType()),
    StructField("heat_alert_level", StringType())
])

# Lecture Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_transformed") \
    .option("startingOffsets", "latest") \
    .load()

df = df_kafka.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Colonnes alertes (level_1 / level_2)
df_alerts = df.withColumn(
    "wind_alert",
    when(col("wind_alert_level").isin("level_1", "level_2"), 1).otherwise(0)
).withColumn(
    "heat_alert",
    when(col("heat_alert_level").isin("level_1", "level_2"), 1).otherwise(0)
)

# Sliding window (5 minutes, slide 1 minute)
aggregates = df_alerts \
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute"),
        col("latitude"),
        col("longitude")
    ) \
    .agg(
        count("wind_alert").alias("total_wind_alerts"),
        count("heat_alert").alias("total_heat_alerts"),
        avg("temperature").alias("avg_temperature"),
        min("temperature").alias("min_temperature"),
        max("temperature").alias("max_temperature")
    )

# Sortie console
query = aggregates.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
