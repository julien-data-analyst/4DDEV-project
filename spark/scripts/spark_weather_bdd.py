"""
spark_weather_bdd.py
=====================================
PySpark Structured Streaming – Surveillance des fichiers parquets transformées de météos, 
ce job va servir principalement à l'insertion dans la base de données
des nouveaux fichiers parquets.

Surveille :
  - s3a://processed/weather-prepared/real         (données réelles)
  - s3a://processed/weather-prepared/fake         (données fictives)

Logique :
  - Détecte les nouvelles mesures transformées
  - Lecture puis chargement directe dans la base de données PostgreSQL pour la table "dim_weather"
"""

from pyspark.sql import SparkSession
import os
from pyspark.sql.types import (
    StructType, StructField,
    TimestampType, DateType,
    IntegerType, StringType,
    DoubleType, BooleanType
)
import logging

# ─────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────

POSTGRES_URL = "jdbc:postgresql://postgres:5432/data_warehouse"

INPUT_PATH = "s3a://processed/weather-prepared/*/*.parquet"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("DATALAKE_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("DATALAKE_PASSWORD", "minio_password_change_me")

POSTGRES_USER     = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PG_TABLE = 'public.dim_weather'
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────

# ─────────────────────────────────────────────
# Schéma JSON
# ─────────────────────────────────────────────

schema_transformed = StructType([
    StructField("datetime_measure", TimestampType(), True),
    StructField("date_measure",     DateType(),      True),
    StructField("measure_dow",      IntegerType(),   True),
    StructField("measure_hour",     IntegerType(),   True),
    StructField("weather_main",     StringType(),    True),
    StructField("weather_description", StringType(), True),
    StructField("temp_celsius",     DoubleType(),    True),
    StructField("humidity_pct",     DoubleType(),    True),
    StructField("wind_speed_ms",    DoubleType(),    True),
    StructField("fictif",           BooleanType(),   True),
])

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

def write_to_postgres(batch_df, batch_id):
    count = batch_df.count()

    log.info("[batch %d] %d lignes lues ", batch_id, count)
    batch_df.printSchema()   # ← affiche le schéma inféré
    batch_df.show(5)         # ← affiche les 5 premières lignes

    if count == 0:
        log.info("[batch %d] Vide — rien à écrire", batch_id)
        return

    # Exemple de déduplication simple côté Spark
    df_clean = batch_df.dropDuplicates([
        "datetime_measure"
    ])


    (
        df_clean.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", PG_TABLE)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

    log.info("[batch %d] OK — %d lignes insérées.", batch_id, count)

def main():
    spark = create_spark()

    log.info("Commencement du streaming pour l'insertion dans la base de données")

    df = (
        spark.readStream
        .format("parquet")
        .schema(schema_transformed)
        .load(INPUT_PATH)
    )

    query = (
        df.writeStream 
        .foreachBatch(lambda df, bid: write_to_postgres(df, bid))
        .option("checkpointLocation", "s3a://processed/checkpoints/postgres/")
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination(timeout=300)
    
    log.info("Streaming météo terminé proprement.")

if __name__ == "__main__":
    main()