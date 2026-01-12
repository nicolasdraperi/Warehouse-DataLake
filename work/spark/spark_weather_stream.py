# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Spark session
spark = SparkSession.builder \
    .appName("WeatherStreamingAlerts") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schéma Kafka
schema = StructType([
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("windspeed", DoubleType()),
    StructField("winddirection", DoubleType()),
    StructField("time", StringType())
])

# Lecture Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_stream") \
    .option("startingOffsets", "latest") \
    .load()

# Parsing JSON
df_parsed = df_kafka.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Transformations + alertes
df_transformed = df_parsed \
    .withColumn("event_time", to_timestamp(col("time"))) \
    .withColumn(
        "wind_alert_level",
        when(col("windspeed") < 10, "level_0")
        .when((col("windspeed") >= 10) & (col("windspeed") <= 20), "level_1")
        .otherwise("level_2")
    ) \
    .withColumn(
        "heat_alert_level",
        when(col("temperature") < 25, "level_0")
        .when((col("temperature") >= 25) & (col("temperature") <= 35), "level_1")
        .otherwise("level_2")
    )

# Sortie Kafka
df_output = df_transformed.selectExpr(
    """
    to_json(
        struct(
            city,
            country,
            latitude,
            longitude,
            temperature,
            windspeed,
            event_time,
            wind_alert_level,
            heat_alert_level
        )
    ) AS value
    """
)

# Écriture Kafka
query = df_output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "weather_transformed") \
    .option("checkpointLocation", "/tmp/weather_checkpoint") \
    .start()

query.awaitTermination()
