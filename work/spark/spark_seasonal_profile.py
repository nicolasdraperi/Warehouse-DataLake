# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, month, avg, when
)

spark = SparkSession.builder \
    .appName("SeasonalClimateProfile") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# lecture HDFS historique
df = spark.read \
    .option("basePath", "hdfs://namenode:9000/hdfs-data/weather_history_raw") \
    .json("hdfs://namenode:9000/hdfs-data/weather_history_raw/*/*/*.json")

# extraire le mois depuis la date
df = df.withColumn("month", month(col("date")))

# Définir une alerte climatique (exemple)
# règle simple : tep_max >= 25 ou wind_max >= 20
df = df.withColumn(
    "alert",
    when((col("temp_max") >= 25) | (col("wind_max") >= 20), 1).otherwise(0)
)

# Agregations mensuelles
seasonal = df.groupBy("country", "city", "month").agg(
    avg(col("temp_max")).alias("avg_temperature"),
    avg(col("wind_max")).alias("avg_wind_speed"),
    avg(col("alert")).alias("alert_probability")
)

# Ecriture HDFS partitionnée
seasonal.write \
    .mode("overwrite") \
    .partitionBy("country", "city") \
    .json("hdfs://namenode:9000/hdfs-data/seasonal_profile")

print("✅ Profils saisonniers stockés dans HDFS")
