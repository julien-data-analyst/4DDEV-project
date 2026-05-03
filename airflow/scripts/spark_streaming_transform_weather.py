"""
spark_streaming_transform_weather.py
=====================================
PySpark Structured Streaming – Surveillance des buckets météo Minio.

Surveille en continu :
  - s3a://raw-weather/weather_events/    (données réelles)
  - s3a://raw-weather-faker/weather-fake/ (données fictives)

Correction appliquée : les JARs hadoop-aws et aws-java-sdk-bundle sont
chargés depuis le volume partagé /opt/spark/scripts/ via spark.jars +
spark.executor.extraClassPath → disponibles sur le driver ET les workers.
Plus de ClassNotFoundException: S3AFileSystem sur les executors.
"""

from __future__ import annotations

import os
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, IntegerType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("DATALAKE_USER",    "minio_admin")
MINIO_SECRET_KEY = os.getenv("DATALAKE_PASSWORD", "minio_password_change_me")

POSTGRES_USER     = os.getenv("POSTGRES_USER",     "dwh_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "dwh_password")
JDBC_URL          = "jdbc:postgresql://postgres:5432/data_warehouse"

PATH_REAL   = "s3a://raw-weather/weather_events/"
PATH_FAKER  = "s3a://raw-weather-faker/weather-fake/"

CHECKPOINT_REAL  = "s3a://processed/checkpoints/weather_real/"
CHECKPOINT_FAKER = "s3a://processed/checkpoints/weather_faker/"

PG_TABLE = "public.dim_weather"

# Chemin des JARs dans le volume partagé (même dossier que les scripts)
JAR_DIR      = "/opt/spark/scripts"
JAR_HADOOP   = f"{JAR_DIR}/hadoop-aws-3.3.4.jar"
JAR_AWS_SDK  = f"{JAR_DIR}/aws-java-sdk-bundle-1.12.262.jar"
JAR_POSTGRES = f"{JAR_DIR}/postgresql-42.7.3.jar"
ALL_JARS     = f"{JAR_HADOOP},{JAR_AWS_SDK},{JAR_POSTGRES}"
EXECUTOR_CP  = f"{JAR_HADOOP}:{JAR_AWS_SDK}:{JAR_POSTGRES}"


# ─────────────────────────────────────────────────────────────────────────────
#  Schéma JSON — aligné sur ce que génère generate_fake_weather
# ─────────────────────────────────────────────────────────────────────────────

WEATHER_SCHEMA = StructType([
    StructField("ingested_at",         StringType(),  True),
    StructField("city_id",             LongType(),    True),
    StructField("city_name",           StringType(),  True),
    StructField("lat",                 DoubleType(),  True),
    StructField("lon",                 DoubleType(),  True),
    StructField("timestamp_unix",      LongType(),    True),
    StructField("weather_main",        StringType(),  True),
    StructField("weather_description", StringType(),  True),
    StructField("temp_celsius",        DoubleType(),  True),
    StructField("humidity_pct",        IntegerType(), True),
    StructField("wind_speed_ms",       DoubleType(),  True),
    StructField("clouds_pct",          IntegerType(), True),
])


# ─────────────────────────────────────────────────────────────────────────────
#  Session Spark
#  Les JARs sont passés via spark-submit (--jars + extraClassPath).
#  On les redéclare ici aussi pour le mode standalone (spark-submit local).
# ─────────────────────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("weather-streaming-minio")

        # ── S3A / Minio ──────────────────────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint",              f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key",            MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",            MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",     "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.maximum",    "200")

        # ── JARs locaux — distribués aux workers via extraClassPath ──────────
        .config("spark.jars",                      ALL_JARS)
        .config("spark.executor.extraClassPath",   EXECUTOR_CP)
        .config("spark.driver.extraClassPath",     EXECUTOR_CP)

        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
#  Transformation
# ─────────────────────────────────────────────────────────────────────────────

def apply_transforms(df, is_fictif: bool):
    """
    Transformations métier + colonne `fictif`.
    On ne sélectionne que les colonnes présentes dans le schéma allégé
    du faker (pas de country, feels_like, etc.).
    """
    return (
        df
        .withColumn(
            "datetime_measure",
            F.from_unixtime(F.col("timestamp_unix")).cast("timestamp")
        )
        .withColumn("measure_dow",  F.dayofweek("datetime_measure"))
        .withColumn("measure_hour", F.hour("datetime_measure"))
        .withColumn("date_measure", F.to_date("datetime_measure"))
        .withColumn("fictif",       F.lit(is_fictif))
        .select(
            "city_id",
            "city_name",
            "lat",
            "lon",
            "datetime_measure",
            "date_measure",
            "measure_dow",
            "measure_hour",
            "weather_main",
            "weather_description",
            "temp_celsius",
            "humidity_pct",
            "wind_speed_ms",
            "clouds_pct",
            "fictif",
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
#  foreachBatch → PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────

def write_to_postgres(batch_df, batch_id: int, is_fictif: bool) -> None:
    count = batch_df.count()

    if count == 0:
        log.info("[batch %d] Vide — rien à écrire (fictif=%s)", batch_id, is_fictif)
        return

    log.info("[batch %d] %d lignes → %s (fictif=%s)", batch_id, count, PG_TABLE, is_fictif)

    transformed = apply_transforms(batch_df, is_fictif=is_fictif)

    (
        transformed.write
        .format("jdbc")
        .option("url",      JDBC_URL)
        .option("dbtable",  PG_TABLE)
        .option("user",     POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver",   "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    log.info("[batch %d] OK — %d lignes insérées.", batch_id, count)


# ─────────────────────────────────────────────────────────────────────────────
#  Stream reader
# ─────────────────────────────────────────────────────────────────────────────

def build_stream(spark: SparkSession, path: str):
    return (
        spark.readStream
        .format("json")
        .schema(WEATHER_SCHEMA)
        .option("maxFilesPerTrigger", 50)
        .load(path)
    )


# ─────────────────────────────────────────────────────────────────────────────
#  Point d'entrée
# ─────────────────────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    log.info("Streaming météo démarré — %s | %s", PATH_REAL, PATH_FAKER)

    stream_real = (
        build_stream(spark, PATH_REAL)
        .writeStream
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, is_fictif=False))
        .option("checkpointLocation", CHECKPOINT_REAL)
        .trigger(availableNow=True)
        .start()
    )

    stream_faker = (
        build_stream(spark, PATH_FAKER)
        .writeStream
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, is_fictif=True))
        .option("checkpointLocation", CHECKPOINT_FAKER)
        .trigger(availableNow=True)
        .start()
    )

    log.info("Attente de completion des deux streams...")
    stream_real.awaitTermination()
    stream_faker.awaitTermination()

    log.info("Streaming terminé proprement.")
    spark.stop()


if __name__ == "__main__":
    main()
