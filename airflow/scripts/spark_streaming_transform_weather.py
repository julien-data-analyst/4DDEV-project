"""
spark_streaming_weather.py
==========================
PySpark Structured Streaming – Surveillance des buckets météo Minio.

Surveille en continu :
  - s3a://raw-weather/weather/*/*/*/*/*.json        (données réelles)
  - s3a://raw-weather-faker/weather-fake/*/*/*.json        (données fictives)

Pour chaque nouveau fichier JSON détecté :
  - Applique les transformations métier
  - Ajoute une colonne booléenne `fictif` (True/False)
  - Écrit en mode append dans PostgreSQL → public.dim_weather

Mode : Spark Structured Streaming avec trigger "availableNow" (micro-batch)
       → compatible avec un lancement périodique depuis Airflow (toutes les heures)
       → ne reste pas bloqué indéfiniment comme un streaming continu pur

Lancement :
  spark-submit --master spark://spark-master:7077 \
    --jars /opt/airflow/scripts/postgresql-42.7.3.jar \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    /opt/spark/scripts/spark_streaming_weather.py
"""

from __future__ import annotations

import os
import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, IntegerType, BooleanType
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("DATALAKE_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("DATALAKE_PASSWORD", "minio_password_change_me")

POSTGRES_USER     = os.getenv("POSTGRES_USER", "dwh_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "dwh_password")
JDBC_URL          = "jdbc:postgresql://postgres:5432/data_warehouse"
JDBC_JAR          = "/opt/airflow/scripts/postgresql-42.7.3.jar"

# Chemins des buckets (Spark surveille les nouveaux fichiers via le checkpoint)
PATH_REAL  = "s3a://raw-weather/weather_events/"
PATH_FAKER = "s3a://raw-weather-faker/weather-fake/"

# Checkpoint Minio – Spark y stocke l'état des offsets pour ne pas re-lire les anciens fichiers
CHECKPOINT_REAL  = "s3a://processed/checkpoints/weather_real/"
CHECKPOINT_FAKER = "s3a://processed/checkpoints/weather_faker/"

PG_TABLE = "public.dim_weather"


# ─────────────────────────────────────────────────────────────────────────────
#  Schéma JSON attendu (correspond à la normalisation de collecte_api_weather.py)
# ─────────────────────────────────────────────────────────────────────────────

WEATHER_SCHEMA = StructType([
    StructField("ingested_at",          StringType(),  True),
    StructField("city_id",              LongType(),    True),
    StructField("city_name",            StringType(),  True),
    StructField("lat",                  DoubleType(),  True),
    StructField("lon",                  DoubleType(),  True),
    StructField("timestamp_unix",       LongType(),    True),
    StructField("weather_main",         StringType(),  True),
    StructField("weather_description",  StringType(),  True),
    StructField("temp_celsius",         DoubleType(),  True),
    StructField("humidity_pct",         IntegerType(), True),
    StructField("wind_speed_ms",        DoubleType(),  True),
])


# ─────────────────────────────────────────────────────────────────────────────
#  Session Spark
# ─────────────────────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("weather-streaming-minio")

        # Connexion Minio S3A
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.maximum", "200")

        # Streaming : détection de nouveaux fichiers S3 (polling toutes les 60s)
        .config("spark.sql.streaming.schemaInference", "true")

        # JDBC driver
        .config("spark.jars", JDBC_JAR)
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
#  Transformation commune
# ─────────────────────────────────────────────────────────────────────────────

def apply_transforms(df, is_fictif: bool):
    """
    Applique les transformations métier et ajoute la colonne `fictif`.
    Sélectionne uniquement les colonnes nécessaires pour dim_weather.
    """
    return (
        df
        # Colonne datetime principale
        .withColumn(
            "datetime_measure",
            F.from_unixtime(F.col("timestamp_unix")).cast("timestamp")
        )

        # Colonnes dérivées temporelles
        .withColumn("measure_dow",   F.dayofweek("datetime_measure"))
        .withColumn("measure_hour",  F.hour("datetime_measure"))
        .withColumn("date_measure",  F.to_date("datetime_measure"))

        # Marqueur fictif/réel
        .withColumn("fictif", F.lit(is_fictif))

        # Sélection finale pour dim_weather
        .select(
            "city_id",
            "city_name",
            "country",
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
            "rain_1h_mm",
            "snow_1h_mm",
            "fictif",
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
#  Écriture batch dans PostgreSQL (appelée par foreachBatch)
# ─────────────────────────────────────────────────────────────────────────────

def write_to_postgres(batch_df, batch_id: int, is_fictif: bool) -> None:
    """
    Callback foreachBatch : écrit chaque micro-batch dans PostgreSQL.
    Ignore les batches vides pour éviter les écritures inutiles.
    """
    count = batch_df.count()

    if count == 0:
        log.info("[batch %d] Batch vide, rien à écrire (fictif=%s)", batch_id, is_fictif)
        return

    log.info("[batch %d] Écriture de %d lignes dans %s (fictif=%s)", batch_id, count, PG_TABLE, is_fictif)

    transformed = apply_transforms(batch_df, is_fictif=is_fictif)

    (
        transformed.write
        .format("jdbc")
        .option("url",      JDBC_URL)
        .option("dbtable",  PG_TABLE)
        .option("user",     POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver",   "org.postgresql.Driver")
        # append : on ne tronque jamais, on accumule
        .mode("append")
        .save()
    )

    log.info("[batch %d] OK – %d lignes insérées.", batch_id, count)


# ─────────────────────────────────────────────────────────────────────────────
#  Stream reader générique
# ─────────────────────────────────────────────────────────────────────────────

def build_stream(spark: SparkSession, path: str):
    """
    Retourne un DataFrame streaming qui surveille un dossier S3A.
    Spark utilise le système de fichiers S3A pour lister les nouveaux objets
    depuis le dernier checkpoint.
    """
    return (
        spark.readStream
        .format("json")
        .schema(WEATHER_SCHEMA)
        # maxFilesPerTrigger : traite au max N fichiers par micro-batch
        # → évite de surcharger Postgres si beaucoup de fichiers arrivent d'un coup
        .option("maxFilesPerTrigger", 50)
        # cleanSource : archive les fichiers traités (optionnel, utile pour le debug)
        # .option("cleanSource", "archive")
        .load(path)
    )


# ─────────────────────────────────────────────────────────────────────────────
#  Point d'entrée
# ─────────────────────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    log.info("Démarrage du streaming météo – buckets : %s | %s", PATH_REAL, PATH_FAKER)

    # ── Stream 1 : données réelles ──────────────────────────────────────────
    stream_real = (
        build_stream(spark, PATH_REAL)
        .writeStream
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, is_fictif=False))
        .option("checkpointLocation", CHECKPOINT_REAL)
        # availableNow : traite tous les fichiers disponibles puis s'arrête proprement
        # → parfait pour un lancement horaire via Airflow (pas de process zombie)
        .trigger(availableNow=True)
        .start()
    )

    # ── Stream 2 : données fictives ─────────────────────────────────────────
    stream_faker = (
        build_stream(spark, PATH_FAKER)
        .writeStream
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid, is_fictif=True))
        .option("checkpointLocation", CHECKPOINT_FAKER)
        .trigger(availableNow=True)
        .start()
    )

    log.info("Les deux streams sont démarrés. En attente de completion...")

    # Attend que les deux streams aient fini de traiter les fichiers disponibles
    stream_real.awaitTermination()
    stream_faker.awaitTermination()

    log.info("Streaming terminé proprement.")
    spark.stop()


if __name__ == "__main__":
    main()