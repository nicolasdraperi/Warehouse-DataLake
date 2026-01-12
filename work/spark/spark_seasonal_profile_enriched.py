# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, stddev, min, max, expr
)

spark = SparkSession.builder \
    .appName("SeasonalProfileValidationEnrichment") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Lecture des profils saisonniers
df = spark.read \
    .option("basePath", "hdfs://namenode:9000/hdfs-data/seasonal_profile") \
    .json("hdfs://namenode:9000/hdfs-data/seasonal_profile/*/*/*.json")

# Colonnes attendues :
# country, city, month, avg_temperature, avg_wind_speed, alert_probability

# --------------------------------------------------
# VALIDATION DES DONNÉES
# --------------------------------------------------

# 2.1 Vérifier 12 mois par ville/pays
months_check = df.groupBy("country", "city").agg(
    count("month").alias("nb_months")
)

invalid_cities = months_check.filter(col("nb_months") < 12)

# (optionnel) afficher les villes incomplètes
invalid_cities.show(truncate=False)

# 2.2 Filtrage des valeurs irréalistes
df_valid = df.filter(
    (col("avg_temperature") >= -50) & (col("avg_temperature") <= 60) &
    (col("avg_wind_speed") >= 0) & (col("avg_wind_speed") <= 60)
)

# --------------------------------------------------
# STATISTIQUES DE DISPERSION
# --------------------------------------------------

stats = df_valid.groupBy("country", "city", "month").agg(
    stddev("avg_temperature").alias("temp_std"),
    stddev("avg_wind_speed").alias("wind_std"),
    min("avg_temperature").alias("temp_min"),
    max("avg_temperature").alias("temp_max"),
    min("avg_wind_speed").alias("wind_min"),
    max("avg_wind_speed").alias("wind_max")
)

# --------------------------------------------------
# ENRICHISSEMENT AVANCÉ (MÉDIANE & QUANTILES)
# --------------------------------------------------

quantiles = df_valid.groupBy("country", "city", "month").agg(
    expr("percentile_approx(avg_temperature, 0.5)").alias("temp_median"),
    expr("percentile_approx(avg_temperature, 0.25)").alias("temp_q25"),
    expr("percentile_approx(avg_temperature, 0.75)").alias("temp_q75"),
    expr("percentile_approx(avg_wind_speed, 0.5)").alias("wind_median"),
    expr("percentile_approx(avg_wind_speed, 0.25)").alias("wind_q25"),
    expr("percentile_approx(avg_wind_speed, 0.75)").alias("wind_q75")
)

# --------------------------------------------------
# MERGE FINAL
# --------------------------------------------------

final_profile = df_valid \
    .join(stats, ["country", "city", "month"]) \
    .join(quantiles, ["country", "city", "month"])

# --------------------------------------------------
# SAUVEGARDE HDFS
# --------------------------------------------------

final_profile.write \
    .mode("overwrite") \
    .partitionBy("country", "city") \
    .json("hdfs://namenode:9000/hdfs-data/seasonal_profile_enriched/all_years")

print("✅ Profils saisonniers validés et enrichis sauvegardés")
