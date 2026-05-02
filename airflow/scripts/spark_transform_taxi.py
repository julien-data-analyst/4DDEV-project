from __future__ import annotations

import os
from pyspark.sql import SparkSession, functions as F


# ─────────────────────────────────────────────
# ENV (une seule lecture)
# ─────────────────────────────────────────────

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("DATALAKE_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("DATALAKE_PASSWORD", "minio_password_change_me")

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

JDBC_URL = "jdbc:postgresql://postgres:5432/data_warehouse"


# ─────────────────────────────────────────────
# SPARK SESSION FACTORY
# ─────────────────────────────────────────────

def create_spark_session():
    return (
        SparkSession.builder
        .appName("taxi-trips-batch-transform")

        # S3 / MINIO
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        # perf
        .config("spark.hadoop.fs.s3a.connection.maximum", "200")

        # jars
        .config("spark.jars", "/opt/airflow/scripts/postgresql-42.7.3.jar")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .getOrCreate()
    )


from dateutil.relativedelta import relativedelta
from datetime import date


def iter_months(year_from, year_to):
    current = date(year_from, 1, 1)
    end = date(year_to, 12, 1)

    while current <= end:
        yield current.year, current.month
        current += relativedelta(months=1)

# ─────────────────────────────────────────────
# CORE TRANSFORM
# ─────────────────────────────────────────────

def build_paths(year_from, year_to):
    return [
        f"s3a://raw-taxi/yellow_tripdata/{y:04d}/{m:02d}/*.parquet"
        for y, m in iter_months(year_from, year_to)
    ]


def run_taxi_transform(year_from: int, year_to: int):
    spark = create_spark_session()

    paths = build_paths(year_from, year_to)

    df = spark.read.parquet(*paths)
    
    df_selected = df.select(
        F.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        F.col("tpep_pickup_datetime").alias("pickup_datetime"),
        "passenger_count",
        (F.col("trip_distance") / 0.621371).alias("trip_distance_km"),
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "tip_amount",
        "tolls_amount",
        "total_amount"
    )

    df_filtered = (
        df_selected
        .filter(F.col("trip_distance_km") > 0)
        .filter(F.col("total_amount") > 0)
        .filter(F.col("passenger_count") > 0)
        .dropna(subset=["dropoff_datetime", "pickup_datetime"])
    )

    # zones depuis MINIO
    df_zone = spark.read.csv(
        "s3a://raw-taxi/zone/taxi_zone_lookup.csv",
        header=True
    )

    df_features = (
        df_filtered
        .withColumn("pickup_dow", F.dayofweek("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn(
            "trip_duration_min",
            (F.unix_timestamp("dropoff_datetime") -
             F.unix_timestamp("pickup_datetime")) / 60
        )
        .withColumn(
            "tranche_trip_distance_km",
            F.when(F.col("trip_distance_km") <= 2, "0-2 km")
             .when(F.col("trip_distance_km") <= 5, "2-5 km")
             .otherwise(">5 km")
        )
        .withColumn(
            "payment_type_str",
            F.when(F.col("payment_type") == 1, "Credit card")
             .when(F.col("payment_type") == 2, "Cash")
             .otherwise("Other")
        )
        .drop("payment_type")
        .withColumn(
            "prct_pourboire",
            F.expr("try_divide(tip_amount, fare_amount)")
        )
    )

    # JOIN PU
    df_joined = (
        df_features
        .join(df_zone, df_features.PULocationID == df_zone.LocationID, "left")
        .drop("LocationID", "PULocationID")
        .withColumnRenamed("Borough", "PUBorough")
        .withColumnRenamed("Zone", "PUZone")
        .withColumnRenamed("service_zone", "PU_service_zone")
    )

    # JOIN DO
    df_joined = (
        df_joined
        .join(df_zone, df_joined.DOLocationID == df_zone.LocationID, "left")
        .drop("LocationID", "DOLocationID")
        .withColumnRenamed("Borough", "DOBorough")
        .withColumnRenamed("Zone", "DOZone")
        .withColumnRenamed("service_zone", "DO_service_zone")
    )

    # WRITE POSTGRES (OVERWRITE TABLE)
    df_joined.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "public.fact_taxi_trips") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .save()

    spark.stop()


if __name__ == "__main__":
    run_taxi_transform(2025, 2026)