# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)

# Spark session
spark = SparkSession.builder \
    .appName("WeatherToHDFS") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schéma des données transformées
schema = StructType([
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("windspeed", DoubleType()),
    StructField("wind_alert_level", StringType()),
    StructField("heat_alert_level", StringType()),
    StructField("event_time", TimestampType())
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

# Écriture HDFS partitionnée
query = df.writeStream \
    .format("json") \
    .partitionBy("country", "city") \
    .option("path", "hdfs://namenode:9000/hdfs-data") \
    .option("checkpointLocation", "hdfs://namenode:9000/hdfs-data/_checkpoint") \
    .outputMode("append") \
    .start()

query.awaitTermination()
