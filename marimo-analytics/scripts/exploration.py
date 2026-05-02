import marimo

__generated_with = "0.23.4"
app = marimo.App(width="medium")


@app.cell
def _():
    # Chargement des librairies
    import marimo as mo
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, IntegerType
    import pandas as pd
    import os

    return SparkSession, mo, os


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Observation et transformation des données pour les voyages de taxis jaunes
    """)
    return


@app.cell
def _(os):
    # Lecture des dpnnées depuis le MINIO endpoint
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "")
    MINIO_ACCESS_KEY = os.getenv("DATALAKE_USER", "minio_admin")
    MINIO_SECRET_KEY = os.getenv("DATALAKE_PASSWORD", "minio_password_change_me")
    return MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY


@app.cell
def _(MINIO_SECRET_KEY):
    MINIO_SECRET_KEY
    return


@app.cell
def _(MINIO_ACCESS_KEY, MINIO_ENDPOINT, MINIO_SECRET_KEY, SparkSession):
    # Configuration de la session
    spark = (
        SparkSession.builder
        .appName("taxi-trips-batch-transform")

        # ─────────────────────────────────────
        # MINIO / S3A BASIC CONFIG
        # ─────────────────────────────────────
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)

        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # IMPORTANT : évite provider AWS SDK inutile (tu es en MinIO)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )

        # ─────────────────────────────────────
        # CONNECTION POOL / PERF I/O
        # ─────────────────────────────────────
        .config("spark.hadoop.fs.s3a.connection.maximum", "200")
        .config("spark.hadoop.fs.s3a.threads.max", "200")
        .config("spark.hadoop.fs.s3a.threads.core", "50")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")

        # timeouts
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")

        # retries (IMPORTANT: évite retry trop agressif)
        .config("spark.hadoop.fs.s3a.retry.limit", "3")
        .config("spark.hadoop.fs.s3a.retry.interval", "500ms")

        # ─────────────────────────────────────
        # MULTIPART / UPLOAD OPTIMISATION
        # ─────────────────────────────────────
        .config("spark.hadoop.fs.s3a.multipart.size", "64m")
        .config("spark.hadoop.fs.s3a.multipart.threshold", "64m")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")

        # ─────────────────────────────────────
        # PARQUET / READING PERF
        # ─────────────────────────────────────
        .config("spark.sql.files.maxPartitionBytes", "128m")
        .config("spark.sql.files.openCostInBytes", "4m")
        .config("spark.hadoop.fs.s3a.experimental.input.fadvise", "sequential")

        # ─────────────────────────────────────
        # HADOOP AWS LIBS
        # ─────────────────────────────────────
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )

        .getOrCreate()
    )
    # Les dépendances d'AWS (Amazon Web Service) vont nous permettre de facilement nous connecter à Minio contenant les fichiers parquet
    # à lire et transformer
    return (spark,)


@app.cell
def _(MINIO_ACCESS_KEY):
    MINIO_ACCESS_KEY
    return


@app.cell
def _(spark):
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    for entry in conf.iterator():
        k = entry.getKey()
        v = entry.getValue()
        # if "s3a" in k and "s" in v:
        if v == '24h':
            print(k, v)
    return


@app.cell
def _():
    path_recent = (
            "s3a://raw-taxi/yellow_tripdata/2026/01/yellow_tripdata_2026-01.parquet"
        )

    path_tmp_recent = (
        "./scripts/data/yellow_tripdata_2026-01.parquet"
    )
    path_ancien =(
        "s3a://raw-taxi/yellow_tripdata/2009/01/*.parquet"
    )
    return (path_recent,)


@app.cell
def _(spark):
    print(spark.sparkContext.defaultParallelism)
    return


@app.cell
def _(path_recent, spark):
    # Lecture du fichier récent et observation des colonnes et lignes
    df_recent = spark.read.parquet(path_recent)
    return


@app.cell
def _(spark):
    spark.stop()
    return


if __name__ == "__main__":
    app.run()
