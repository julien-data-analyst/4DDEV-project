"""
spark_streaming_transform_weather_parquet.py
=====================================
PySpark Structured Streaming – Surveillance des buckets météo Minio.

Surveille :
  - s3a://raw-weather/weather/         (données réelles)
  - s3a://raw-weather-faker/weather-fake/ (données fictives)

  Logique :
  - Détecte les nouvelles mesures créées
  - Nettoyage/Transformation des données brutes 
  - Exporte dans le bucket "processed" auquel ils vont être ensuite insérées dans la base de données par un autre DAG
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
import os
import logging

# ─────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("DATALAKE_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("DATALAKE_PASSWORD", "minio_password_change_me")

INPUT_PATH_REAL  = "s3a://raw-weather/weather/*/*/*/*.json"
INPUT_PATH_FAKER = "s3a://raw-weather-faker/weather-fake/*/*/*/*.json"

OUTPUT_PATH = "s3a://processed/weather-prepared/"

CHECKPOINT_REAL  = "s3a://processed/checkpoints/weather-real/"
CHECKPOINT_FAKER = "s3a://processed/checkpoints/weather-faker/"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Schéma JSON
# ─────────────────────────────────────────────
schema = StructType([
    StructField("ingested_at", StringType()),
    StructField("city_id", LongType()),
    StructField("city_name", StringType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("timestamp_unix", LongType()),
    StructField("weather_main", StringType()),
    StructField("weather_description", StringType()),
    StructField("temp_celsius", DoubleType()),
    StructField("humidity_pct", DoubleType()),
    StructField("wind_speed_ms", DoubleType())
])

# ─────────────────────────────────────────────
# Job Spark
# ─────────────────────────────────────────────
def create_spark():
    return (
        SparkSession.builder
        .appName("weather-transform-parquet")
        .config("spark.hadoop.fs.s3a.endpoint",               f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.maximum",     "200")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .getOrCreate()
    )

# ─────────────────────────────────────────────
# Transformation des données
# ─────────────────────────────────────────────
def transform(df, fictif):
    return (
        df
        .withColumn(
            "datetime_measure",
            F.from_unixtime(F.col("timestamp_unix")).cast("timestamp")
        )
        .withColumn("measure_dow",  F.dayofweek("datetime_measure"))
        .withColumn("measure_hour", F.hour("datetime_measure"))
        .withColumn("date_measure", F.to_date("datetime_measure"))
        .withColumn("fictif",       F.lit(fictif))
        .select(
            "datetime_measure", "date_measure",
            "measure_dow", "measure_hour",
            "weather_main", "weather_description",
            "temp_celsius", "humidity_pct",
            "wind_speed_ms", "fictif",
        )
    )

# ─────────────────────────────────────────────
# Lancement des deux streaming
# ─────────────────────────────────────────────
def main():
    spark = create_spark()

    log.info("Commencement des deux streamings")
    real_stream = (
        spark.readStream
        .schema(schema)
        .json(INPUT_PATH_REAL)
    )

    faker_stream = (
        spark.readStream
        .schema(schema)
        .json(INPUT_PATH_FAKER)
    )

    query_real = (
        transform(real_stream, False)
        .writeStream
        .format("parquet")
        .option("path", OUTPUT_PATH + "real/")
        .option("checkpointLocation", CHECKPOINT_REAL)
        .trigger(availableNow=True)
        .start()
    )

    query_faker = (
        transform(faker_stream, True)
        .writeStream
        .format("parquet")
        .option("path", OUTPUT_PATH + "faker/")
        .option("checkpointLocation", CHECKPOINT_FAKER)
        .trigger(availableNow=True)
        .start()
    )

    query_real.awaitTermination(timeout=300)
    query_faker.awaitTermination(timeout=300)

    log.info("Streaming terminé proprement.")
    spark.stop()

if __name__ == "__main__":
    main()