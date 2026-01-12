# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, month, when, abs,
    lit, struct, to_json, year
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, TimestampType
)

spark = SparkSession.builder \
    .appName("WeatherAnomalyDetection") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# SCHÉMAS
# --------------------------------------------------

# Données temps réel
realtime_schema = StructType([
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("temperature", DoubleType()),
    StructField("windspeed", DoubleType()),
    StructField("wind_alert_level", StringType()),
    StructField("heat_alert_level", StringType()),
    StructField("event_time", TimestampType())
])

# Profils saisonniers enrichis
seasonal = spark.read \
    .option("basePath", "hdfs://namenode:9000/hdfs-data/seasonal_profile_enriched/all_years") \
    .json("hdfs://namenode:9000/hdfs-data/seasonal_profile_enriched/all_years/*/*/*.json")


seasonal = seasonal.select(
    col("country"),
    col("city"),
    col("month"),
    col("avg_temperature").alias("temp_mean"),
    col("avg_wind_speed").alias("wind_mean"),
    col("wind_std"),
    col("alert_probability")
)

# --------------------------------------------------
# LECTURE STREAMING KAFKA
# --------------------------------------------------

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "weather_transformed") \
    .load()

df_rt = df_kafka.select(
    from_json(col("value").cast("string"), realtime_schema).alias("data")
).select("data.*")

df_rt = df_rt.withColumn("month", month(col("event_time")))

# --------------------------------------------------
# JOIN BATCH vs SPEED
# --------------------------------------------------

joined = df_rt.join(
    seasonal,
    on=["country", "city", "month"],
    how="left"
)

# --------------------------------------------------
# LOGIQUE D’ANOMALIE
# --------------------------------------------------

# Température
joined = joined.withColumn(
    "temp_anomaly",
    abs(col("temperature") - col("temp_mean")) > 5
)

# Vent (2 écarts-types)
joined = joined.withColumn(
    "wind_anomaly",
    col("windspeed") > (col("wind_mean") + 2 * col("wind_std"))
)

# Alertes (écart de proba)
joined = joined.withColumn(
    "alert_anomaly",
    (col("wind_alert_level") != "level_0") &
    (col("alert_probability") < 0.2)
)

# Fusion anomalies
joined = joined.withColumn(
    "is_anomaly",
    col("temp_anomaly") | col("wind_anomaly") | col("alert_anomaly")
)

joined = joined.withColumn(
    "anomaly_type",
    when(col("temp_anomaly"), "temperature")
    .when(col("wind_anomaly"), "wind")
    .when(col("alert_anomaly"), "alert")
    .otherwise("none")
)

# --------------------------------------------------
# FORMAT SORTIE
# --------------------------------------------------

anomalies = joined.filter(col("is_anomaly") == True).select(
    col("city"),
    col("country"),
    col("event_time"),
    col("anomaly_type").alias("variable"),
    when(col("anomaly_type") == "temperature", col("temperature"))
        .when(col("anomaly_type") == "wind", col("windspeed"))
        .otherwise(lit(None)).alias("observed_value"),
    when(col("anomaly_type") == "temperature", col("temp_mean"))
        .when(col("anomaly_type") == "wind", col("wind_mean"))
        .otherwise(col("alert_probability")).alias("expected_value")
)

# --------------------------------------------------
# SORTIE KAFKA
# --------------------------------------------------

kafka_out = anomalies.select(
    to_json(struct("*")).alias("value")
)

kafka_query = kafka_out.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "weather_anomalies") \
    .option("checkpointLocation", "/tmp/weather_anomaly_checkpoint") \
    .start()

# --------------------------------------------------
# SORTIE HDSF
# --------------------------------------------------

hdfs_query = anomalies \
    .withColumn("year", year(col("event_time"))) \
    .withColumn("month", month(col("event_time"))) \
    .writeStream \
    .format("json") \
    .partitionBy("country", "city", "year", "month") \
    .option("path", "hdfs://namenode:9000/hdfs-data/anomalies") \
    .option("checkpointLocation", "/tmp/weather_anomaly_hdfs_checkpoint") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()
