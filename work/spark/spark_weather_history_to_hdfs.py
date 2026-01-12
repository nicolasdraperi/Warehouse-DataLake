# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("WeatherHistoryToHDFS").getOrCreate()

schema = StructType([
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("date", StringType()),
    StructField("temp_max", DoubleType()),
    StructField("temp_min", DoubleType()),
    StructField("wind_max", DoubleType())
])

df_kafka = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_history_raw") \
    .load()

df = df_kafka.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

df.write \
    .mode("append") \
    .partitionBy("country", "city") \
    .json("hdfs://namenode:9000/hdfs-data/weather_history_raw")

print("Historique météo stocké dans HDFS")
