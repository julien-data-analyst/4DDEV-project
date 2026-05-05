"""
spark_streaming_transform_weather.py
=====================================
PySpark Structured Streaming – Surveillance des buckets météo Minio.

Surveille :
  - s3a://raw-weather/weather/         (données réelles)
  - s3a://raw-weather-faker/weather-fake/ (données fictives)

Corrections appliquées :
  - JARs locaux via spark.jars + executor.extraClassPath (pas de --packages)
  - Checkpoints locaux dans /tmp pour éviter le rename atomique S3
  - mkdir explicite avant .start() pour garantir l'existence du répertoire
  - Un seul .appName() dans create_spark_session()
  - Vérification boto3 avant de démarrer chaque stream
"""

from __future__ import annotations

import os
import logging
import pathlib

import boto3
from botocore.client import Config
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

PATH_REAL  = "s3a://raw-weather/weather/*/*/*/*/*.json"
PATH_FAKER = "s3a://raw-weather-faker/weather-fake/2026/*/*.json"

# Checkpoints locaux — évite le problème de rename atomique incompatible S3/Minio
CHECKPOINT_REAL  = "s3a://processed/weather/checkpoints/"
CHECKPOINT_FAKER = "s3a://processed/faker-weather/checkpoints/"

PG_TABLE = "public.dim_weather"

# JARs dans le volume partagé — chargés sur driver ET workers
JAR_DIR      = "/opt/spark/scripts"
JAR_HADOOP   = f"{JAR_DIR}/hadoop-aws-3.3.4.jar"
JAR_AWS_SDK  = f"{JAR_DIR}/aws-java-sdk-bundle-1.12.262.jar"
JAR_POSTGRES = f"{JAR_DIR}/postgresql-42.7.3.jar"
ALL_JARS     = f"{JAR_HADOOP},{JAR_AWS_SDK},{JAR_POSTGRES}"
EXECUTOR_CP  = f"{JAR_HADOOP}:{JAR_AWS_SDK}:{JAR_POSTGRES}"


# ─────────────────────────────────────────────────────────────────────────────
#  Schéma JSON
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
    StructField("humidity_pct",        DoubleType(), True),
    StructField("wind_speed_ms",       DoubleType(),  True)
])


# ─────────────────────────────────────────────────────────────────────────────
#  Session Spark — UN SEUL appName, JARs locaux, pas de --packages
# ─────────────────────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("weather-streaming-minio")                                    # ← un seul appName
        .config("spark.hadoop.fs.s3a.endpoint",               f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.maximum",     "200")
        # JARs locaux distribués aux workers — PAS de spark.jars.packages
        .config("spark.jars",                    ALL_JARS)
        .config("spark.executor.extraClassPath", EXECUTOR_CP)
        .config("spark.driver.extraClassPath",   EXECUTOR_CP)
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
#  Vérification Minio (boto3) avant de démarrer un stream
# ─────────────────────────────────────────────────────────────────────────────

def path_has_data(bucket: str, prefix: str) -> bool:
    """Retourne True si le préfixe contient au moins un objet dans Minio."""
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return resp.get("KeyCount", 0) > 0


# ─────────────────────────────────────────────────────────────────────────────
#  Transformation
# ─────────────────────────────────────────────────────────────────────────────

def apply_transforms(df, is_fictif: bool):
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
            "datetime_measure", "date_measure",
            "measure_dow", "measure_hour",
            "weather_main", "weather_description",
            "temp_celsius", "humidity_pct",
            "wind_speed_ms", "fictif",
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
#  foreachBatch → PostgreSQL
# ─────────────────────────────────────────────────────────────────────────────

def write_to_postgres(batch_df, batch_id: int, is_fictif: bool) -> None:
    count = batch_df.count()

    log.info("[batch %d] %d lignes lues (fictif=%s)", batch_id, count, is_fictif)
    batch_df.printSchema()   # ← affiche le schéma inféré
    batch_df.show(5)         # ← affiche les 5 premières lignes
    if count == 0:
        log.info("[batch %d] Vide — rien à écrire (fictif=%s)", batch_id, is_fictif)
        return

    log.info("[batch %d] %d lignes → %s (fictif=%s)", batch_id, count, PG_TABLE, is_fictif)
    apply_transforms(batch_df, is_fictif=is_fictif).write \
        .format("jdbc") \
        .option("url",      JDBC_URL) \
        .option("dbtable",  PG_TABLE) \
        .option("user",     POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver",   "org.postgresql.Driver") \
        .mode("append") \
        .save()
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
    # Création explicite des répertoires de checkpoint AVANT .start()
    # → évite le "mkdir failed" de Spark quand le dossier n'existe pas
    pathlib.Path(CHECKPOINT_REAL).mkdir(parents=True, exist_ok=True)
    pathlib.Path(CHECKPOINT_FAKER).mkdir(parents=True, exist_ok=True)
    log.info("Répertoires checkpoint créés : %s | %s", CHECKPOINT_REAL, CHECKPOINT_FAKER)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    streams = []

    # ── Stream réel ──────────────────────────────────────────────────────────
    if path_has_data("raw-weather", "weather/"):
        log.info("raw-weather/weather/ → stream démarré")
        streams.append(
            build_stream(spark, PATH_REAL)
            .writeStream
            .foreachBatch(lambda df, bid: write_to_postgres(df, bid, is_fictif=False))
            .option("checkpointLocation", CHECKPOINT_REAL)
            .trigger(availableNow=True)
            .start()
        )
    else:
        log.warning("raw-weather/weather/ vide ou absent — stream réel ignoré")

    # ── Stream faker ─────────────────────────────────────────────────────────
    if path_has_data("raw-weather-faker", "weather-fake/"):
        log.info("raw-weather-faker/weather-fake/ → stream démarré")
        streams.append(
            build_stream(spark, PATH_FAKER)
            .writeStream
            .foreachBatch(lambda df, bid: write_to_postgres(df, bid, is_fictif=True))
            .option("checkpointLocation", CHECKPOINT_FAKER)
            .trigger(availableNow=True)
            .start()
        )
    else:
        log.warning("raw-weather-faker/weather-fake/ vide ou absent — stream faker ignoré")

    if not streams:
        log.warning("Aucun stream démarré — buckets vides.")
        spark.stop()
        return

    log.info("%d stream(s) actif(s) — attente de completion...", len(streams))
    for s in streams:
        s.awaitTermination()

    log.info("Streaming terminé proprement.")
    spark.stop()


if __name__ == "__main__":
    main()