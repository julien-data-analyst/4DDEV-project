"""
spark_transform_taxi.py
=======================
Transformation PySpark BATCH – Taxis jaunes NYC.

Lit les Parquets bruts depuis Minio (raw-taxi),
applique les transformations métier et écrit dans Minio (processed)
partitionné par année/mois.

À lancer depuis un DAG Airflow via SparkSubmitOperator ou BashOperator.
"""

from __future__ import annotations

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType


# ─────────────────────────────────────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_password_change_me")

INPUT_PATH = "s3a://raw-taxi/yellow_tripdata/*/*/*/*.parquet"
OUTPUT_PATH = "s3a://processed/taxi_trips_cleaned"


# ─────────────────────────────────────────────────────────────────────────────
#  Session Spark avec connecteur Minio (S3A)
# ─────────────────────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("taxi-trips-batch-transform")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # Packages Hadoop AWS (inclus dans l'image Spark ou à fournir)
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
#  Transformations métier
# ─────────────────────────────────────────────────────────────────────────────

def transform_taxi(spark: SparkSession, year: int = None, month: int = None):
    """
    Lit et transforme les données de taxis jaunes.

    Args:
        year, month : si fournis, filtre sur une partition précise.
                      Sinon traite toute la plage disponible.
    """
    path = (
        f"s3a://raw-taxi/yellow_tripdata/{year:04d}/{month:02d}/*.parquet"
        if year and month
        else INPUT_PATH
    )

    df = spark.read.parquet(path)

    df_clean = (
        df
        # Renommage pour homogénéité (les colonnes varient selon les années)
        .withColumnRenamed("tpep_pickup_datetime", "pickup_dt")
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_dt")
        .withColumnRenamed("passenger_count", "passengers")
        .withColumnRenamed("trip_distance", "distance_miles")
        .withColumnRenamed("fare_amount", "fare_usd")
        .withColumnRenamed("tip_amount", "tip_usd")
        .withColumnRenamed("total_amount", "total_usd")
        .withColumnRenamed("PULocationID", "pickup_location_id")
        .withColumnRenamed("DOLocationID", "dropoff_location_id")

        # Typage
        .withColumn("passengers", F.col("passengers").cast(IntegerType()))
        .withColumn("distance_miles", F.col("distance_miles").cast(DoubleType()))
        .withColumn("fare_usd", F.col("fare_usd").cast(DoubleType()))
        .withColumn("tip_usd", F.col("tip_usd").cast(DoubleType()))
        .withColumn("total_usd", F.col("total_usd").cast(DoubleType()))

        # Colonnes dérivées
        .withColumn("trip_duration_min",
                    (F.unix_timestamp("dropoff_dt") - F.unix_timestamp("pickup_dt")) / 60)
        .withColumn("speed_mph",
                    F.when(F.col("trip_duration_min") > 0,
                           F.col("distance_miles") / (F.col("trip_duration_min") / 60))
                    .otherwise(None))
        .withColumn("pickup_hour", F.hour("pickup_dt"))
        .withColumn("pickup_dow", F.dayofweek("pickup_dt"))
        .withColumn("pickup_year", F.year("pickup_dt"))
        .withColumn("pickup_month", F.month("pickup_dt"))

        # Filtres qualité
        .filter(F.col("distance_miles") > 0)
        .filter(F.col("fare_usd") > 0)
        .filter(F.col("total_usd") > 0)
        .filter(F.col("passengers").between(1, 8))
        .filter(F.col("trip_duration_min").between(1, 180))
        .filter(F.col("speed_mph") < 200)

        # Colonne de partitionnement
        .withColumn("year", F.col("pickup_year"))
        .withColumn("month", F.col("pickup_month"))
    )

    # Écriture partitionnée
    (
        df_clean
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(OUTPUT_PATH)
    )

    count = df_clean.count()
    print(f"[PySpark] {count:,} trajets transformés → {OUTPUT_PATH}")
    return count

# ─────────────────────────────────────────────────────────────────────────────
#  Point d'entrée
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Arguments optionnels : year month
    year = int(sys.argv[1]) if len(sys.argv) > 1 else None
    month = int(sys.argv[2]) if len(sys.argv) > 2 else None

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        transform_taxi(spark, year=year, month=month)
    finally:
        spark.stop()
