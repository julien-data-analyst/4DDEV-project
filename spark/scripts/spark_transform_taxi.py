from __future__ import annotations

import os
from datetime import date
from dateutil.relativedelta import relativedelta

import boto3
from botocore.client import Config
from pyspark.sql import SparkSession, functions as F


# ─────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("DATALAKE_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("DATALAKE_PASSWORD", "minio_password_change_me")

POSTGRES_USER     = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

JDBC_URL = "jdbc:postgresql://postgres:5432/data_warehouse"


# ─────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("taxi-trips-batch-transform")
        .config("spark.hadoop.fs.s3a.endpoint",               f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key",             MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",             MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.maximum",     "200")
        .config("spark.jars",          "/opt/spark/scripts/postgresql-42.7.3.jar")
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .getOrCreate()
    )


# ─────────────────────────────────────────────
# UTILITAIRES
# ─────────────────────────────────────────────

def iter_months(year_from: int, year_to: int):
    """
    Génère (year, month) de year_from/01 jusqu'au mois courant inclus.
    Le plafond est toujours date.today() : on ne génère jamais
    de chemin vers un mois futur qui n'existe pas encore dans Minio.
    """
    today   = date.today().replace(day=1)
    ceiling = min(date(year_to, 12, 1), today)
    current = date(year_from, 1, 1)
    while current <= ceiling:
        yield current.year, current.month
        current += relativedelta(months=1)


def get_existing_paths(year_from: int, year_to: int) -> list[str]:
    """
    Interroge Minio via boto3 et ne retourne que les préfixes
    qui contiennent au moins un fichier Parquet.
    Évite le PATH_NOT_FOUND de Spark sur des mois absents ou futurs.
    """
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    existing = []
    for year, month in iter_months(year_from, year_to):
        prefix = f"yellow_tripdata/{year:04d}/{month:02d}/"
        resp   = s3.list_objects_v2(Bucket="raw-taxi", Prefix=prefix, MaxKeys=1)
        if resp.get("KeyCount", 0) > 0:
            existing.append(f"s3a://raw-taxi/{prefix}*.parquet")
            print(f"[OK]   {prefix} → inclus")
        else:
            print(f"[SKIP] {prefix} → absent dans Minio, ignoré")

    return existing


# ─────────────────────────────────────────────
# TRANSFORMATION
# ─────────────────────────────────────────────

def run_taxi_transform(year_from: int, year_to: int):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ── 1. Résolution des chemins existants ──────────────────────────────────
    paths = get_existing_paths(year_from, year_to)

    if not paths:
        print(
            f"[WARN] Aucun fichier Parquet trouvé dans Minio pour {year_from}→{year_to}. "
            "Lancez d'abord le DAG collecte_taxi_trips_batch."
        )
        spark.stop()
        return

    print(f"[INFO] {len(paths)} mois à traiter.")

    # ── 2. Lecture ───────────────────────────────────────────────────────────
    df = spark.read.parquet(*paths)

    # ── 3. Sélection ────────────────────────────────────────────────────────
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
        "total_amount",
    )

    # ── 4. Filtres qualité ───────────────────────────────────────────────────
    df_filtered = (
        df_selected
        .filter(F.col("trip_distance_km") > 0)
        .filter(F.col("total_amount") > 0)
        .filter(F.col("passenger_count") > 0)
        .dropna(subset=["dropoff_datetime", "pickup_datetime"])
    )

    # ── 5. Zone lookup depuis Minio ──────────────────────────────────────────
    df_zone = spark.read.csv(
        "s3a://raw-taxi/zone/taxi_zone_lookup.csv",
        header=True,
    )

    # ── 6. Features temporelles + métier ────────────────────────────────────
    df_features = (
        df_filtered
        .withColumn("pickup_dow",  F.dayofweek("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn(
            "trip_duration_min",
            (F.unix_timestamp("dropoff_datetime") -
             F.unix_timestamp("pickup_datetime")) / 60,
        )
        .withColumn(
            "tranche_trip_distance_km",
            F.when(F.col("trip_distance_km") <= 2, "0-2 km")
             .when(F.col("trip_distance_km") <= 5, "2-5 km")
             .otherwise(">5 km"),
        )
        .withColumn(
            "payment_type_str",
            F.when(F.col("payment_type") == 1, "Credit card")
             .when(F.col("payment_type") == 2, "Cash")
             .otherwise("Other"),
        )
        .drop("payment_type")
        .withColumn(
            "prct_pourboire",
            F.expr("try_divide(tip_amount, fare_amount)"),
        )
    )

    # ── 7. Jointures zones pickup / dropoff ──────────────────────────────────
    df_joined = (
        df_features
        .join(df_zone, df_features.PULocationID == df_zone.LocationID, "left")
        .drop("LocationID", "PULocationID")
        .withColumnRenamed("Borough",      "PUBorough")
        .withColumnRenamed("Zone",         "PUZone")
        .withColumnRenamed("service_zone", "PU_service_zone")
    )

    df_joined = (
        df_joined
        .join(df_zone, df_joined.DOLocationID == df_zone.LocationID, "left")
        .drop("LocationID", "DOLocationID")
        .withColumnRenamed("Borough",      "DOBorough")
        .withColumnRenamed("Zone",         "DOZone")
        .withColumnRenamed("service_zone", "DO_service_zone")
    )

    # ── 8. Écriture PostgreSQL ───────────────────────────────────────────────
    print(f"[INFO] Écriture dans PostgreSQL → public.fact_taxi_trips")

    (
        df_joined.write
        .format("jdbc")
        .option("url",      JDBC_URL)
        .option("dbtable",  "public.fact_taxi_trips")
        .option("user",     POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver",   "org.postgresql.Driver")
        .mode("overwrite")
        .option("truncate", "true")
        .save()
    )

    print("[INFO] Transformation terminée avec succès.")
    spark.stop()


# ─────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    year_from = int(sys.argv[1]) if len(sys.argv) > 1 else 2025
    year_to   = int(sys.argv[2]) if len(sys.argv) > 2 else date.today().year

    print(f"[START] Transformation taxis {year_from} → {year_to}")
    run_taxi_transform(year_from, year_to)