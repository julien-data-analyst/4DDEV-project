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

    return F, SparkSession, mo, os


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
    # Configuration de la session (peut prendre quelques minutes à s'initialiser)
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
        .config("spark.jars", "/code/scripts/postgresql-42.7.3.jar")
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

    path_ancien =(
        "s3a://raw-taxi/yellow_tripdata/2020/01/yellow_tripdata_2020-01.parquet"
    )

    path_ancien_v2 = (
        "s3a://raw-taxi/yellow_tripdata/2017/01/yellow_tripdata_2017-01.parquet"
    )
    return path_ancien, path_recent


@app.cell
def _(spark):
    print(spark.sparkContext.defaultParallelism)
    return


@app.cell
def _(path_recent, spark):
    # Lecture du fichier récent et observation des colonnes et lignes
    df_recent = spark.read.parquet(path_recent)
    return (df_recent,)


@app.cell
def _(path_ancien, spark):
    df_ancien = spark.read.parquet(path_ancien)
    return (df_ancien,)


@app.cell
def _(df_recent):
    # Observation du nombre de lignes de données et du schéma de données
    print("Nombre de lignes dans le dataframe : ", df_recent.count())

    print("\nSchéma du dataframe : ")
    df_recent.printSchema()

    print("Observation de la première ligne de données : ")
    print(df_recent.head(1))
    return


@app.cell
def _(df_ancien):
    # Observation du nombre de lignes de données et du schéma de données
    print("Nombre de lignes dans le dataframe : ", df_ancien.count())

    print("\nSchéma du dataframe : ")
    df_ancien.printSchema()

    print("Observation de la première ligne de données : ")
    print(df_ancien.head(1))
    return


@app.cell
def _():
    #spark.stop()
    return


@app.cell
def _():
    # # Lister tous les fichiers Parquet dans S3A
    # files_df = spark.read.format("binaryFile").load(
    #     "s3a://raw-taxi/yellow_tripdata/*/*/*.parquet"
    # )

    # file_paths = [row.path for row in files_df.select("path").distinct().collect()]
    return


@app.cell
def _():
    # schemas = {}

    # for file in file_paths:
    #     try:
    #         df = spark.read.parquet(file)
    #         schemas[file] = df.schema.simpleString()
    #     except Exception as e:
    #         print(f"Erreur sur {file}: {e}")
    return


@app.cell
def _():
    # schemas
    return


@app.cell
def _():
    # schema_sorted = sorted(
    #     schemas.keys()
    # )
    return


@app.cell
def _():
    # schema_sorted
    return


@app.cell
def _():
    # import re

    # def extract_year_month(path: str):
    #     match = re.search(r"(\d{4})-(\d{2})", path)
    #     if match:
    #         return f"{match.group(1)}-{match.group(2)}"
    #     return None
    return


@app.cell
def _():
    # sorted_files = sorted(
    #     schemas.keys(),
    #     key=lambda x: extract_year_month(x)
    # )
    return


@app.cell
def _():
    # sorted_files
    return


@app.cell
def _():
    # def normalize_schema(schema_str):
    #     return ",".join(sorted(schema_str.replace("struct<", "").replace(">", "").split(",")))
    return


@app.cell
def _():
    # schema_changes = {}

    # previous_schema = None

    # for file2 in sorted_files:
    #     schema = schemas[file2]
    #     ym = extract_year_month(file2)

    #     if previous_schema is None:
    #         # premier schéma obligatoire
    #         schema_changes[ym] = schema
    #         previous_schema = schema
    #         continue

    #     # changement détecté
    #     if normalize_schema(schema) != normalize_schema(previous_schema):
    #         schema_changes[ym] = schema
    #         previous_schema = schema
    return


@app.cell
def _():
    # schema_changes
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Commentaire :

    À partir de janvier 2011 on observe un changement radical au niveau des noms de colonnes pour la localisation et le type de paiement.

    On se concentrera sur une partie des données (2024 à 2026) pour l'analyse et la création de la base de données.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Transformation des données pour les voyages en taxis

    - Durée : tpep_pickup_datetime=datetime.datetime(2026, 1, 1, 0, 54, 4) et tpep_dropoff_datetime=datetime.datetime(2026, 1, 1, 0, 59, 37)
    - tranche de distance : trip_distance=0.97 (miles) 1 km = 0,621371 miles

    0–2 km, 2–5 km, >5 km

    - Type de paiement : Table de correspondance

    A numeric code signifying how the passenger paid for the trip.
    0 = Flex Fare trip
    1 = Credit card
    2 = Cash
    3 = No charge
    4 = Dispute
    5 = Unknown
    6 = Voided trip

    - Pourcentage de pourboire : tip_amount / fare_amount
    - Heure de prise en charge, jour de la semaine (tpep_pickup_datetime)
    - PULocationID / DOLocationID : utilisation de la table zone CSV pour indiquer les différentes informations de la zone

    "LocationID","Borough","Zone","service_zone"
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Sélection des colonnes dans ce fichier parquet

    Virer colonnes suivantes :
    - VendorID
    - RatecodeID
    - store_and_fwd_flag
    - mta_tax
    - extra
    - improvement_surcharge
    - airport_fee
    - congestion_surcharge
    - cbd_congestion_fee
    """)
    return


@app.cell
def _(df_recent):
    df_recent.printSchema()
    return


@app.cell
def _(df_recent):
    df_selected = df_recent.select(
        df_recent.tpep_dropoff_datetime.alias("dropoff_datetime"), # Date de fin
        df_recent.tpep_pickup_datetime.alias("pickup_datetime"), # Date de début
        df_recent.passenger_count, # Nombre de passager
        (df_recent.trip_distance / 0.621371).alias('trip_distance_km'), # Distance en kilomètre
        df_recent.PULocationID, # Id de localisation début
        df_recent.DOLocationID, # Id de localisation fin
        df_recent.payment_type, # Id du type de paiement
        df_recent.fare_amount, # Tarif au temps et à la distance calculé au compteur
        df_recent.tip_amount, # Pourboire montant (seulement CB)
        df_recent.tolls_amount, # Montant payé pour les péages
        df_recent.total_amount # Montant total chargé pour les passagers

    )
    return (df_selected,)


@app.cell
def _(df_selected):
    df_selected.printSchema()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Filtrage des données pour n'en garder ceux qui n'ont pas de données nulles :
    - tpep_pickup_datetime
    - tpep_dropoff_datetime
    - passenger_count
    - trip_distance
    - tip_amount
    - fare_amount
    """)
    return


@app.cell
def _(F, df_selected):
    df_filtered = (
        df_selected
            .filter(F.col("trip_distance_km") > 0)
            .filter(F.col("total_amount") > 0)
            .filter(F.col("passenger_count") > 0)
            .dropna(subset=["dropoff_datetime", "pickup_datetime"])
            )
    return (df_filtered,)


@app.cell
def _(df_filtered, df_selected):
    print("Nombre de lignes après filtrage : ", df_filtered.count())
    print("Nombre de lignes avant filtrage : ", df_selected.count())
    print("Pourcentage de pertes : ", (df_selected.count() - df_filtered.count()) / df_selected.count())
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Ajout des nouvelles colonnes calculées sous Spark
    """)
    return


@app.cell
def _(spark):
    df_spark_csv_corr_localisation = spark.read.csv("./scripts/data/taxi_zone_lookup.csv", header=True)
    return (df_spark_csv_corr_localisation,)


@app.cell
def _(df_spark_csv_corr_localisation):
    df_spark_csv_corr_localisation.printSchema()
    return


@app.cell
def _(F, df_filtered):
    df_columns_calculated = (
        df_filtered
                # Pour les heures et jours de la semaine
                .withColumn("pickup_dow", F.dayofweek("pickup_datetime"))
                .withColumn("pickup_hour", F.hour("pickup_datetime"))
                .withColumn("pickup_date", F.to_date('pickup_datetime'))

                # Pour la durée et la distance de trajet
                .withColumn("trip_duration_min", (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60)
                .withColumn("tranche_trip_distance_km",
                            F.when(df_filtered.trip_distance_km <= 2, '0 et 2 km')
                             .when(df_filtered.trip_distance_km <= 5, '2 et 5 km')
                              .otherwise('>5 km'))

                # Pour le type de paiement, le pourcentage de pourboire
                .withColumn("payment_type_str", 
                           F.when(df_filtered.payment_type == 0, 'Flex Fare Trip')
                             .when(df_filtered.payment_type == 1, 'Credit card')
                             .when(df_filtered.payment_type == 2, 'Cash')
                             .when(df_filtered.payment_type == 3, 'No charge')
                             .when(df_filtered.payment_type == 4, 'Dispute')
                             .when(df_filtered.payment_type == 5, 'Unknown')
                             .when(df_filtered.payment_type == 6, "Voided trip")
                             .otherwise('Unknown')
                           )
                .drop("payment_type")
                .withColumn("prct_pourboire", F.expr("try_divide(tip_amount, fare_amount)"))
    )
    # Extract the day of the week of a given date/timestamp as integer. Ranges from 1 for a Sunday through to 7 for a Saturday
    return (df_columns_calculated,)


@app.cell
def _(df_columns_calculated):
    df_columns_calculated.head(1)
    return


@app.cell
def _(df_columns_calculated, df_spark_csv_corr_localisation):
    # Join sur les informations de zone

    df_localisation = (df_columns_calculated.join(df_spark_csv_corr_localisation, 
                               df_columns_calculated.PULocationID == df_spark_csv_corr_localisation.LocationID,
                              'left')
                          .drop(*["LocationID", "PULocationID"])
                          .withColumnsRenamed(
                              {"Borough": "PUBorough", "Zone": "PUZone", "service_zone" : "PU_service_zone"}
                          )
                          .join(df_spark_csv_corr_localisation, 
                               df_columns_calculated.DOLocationID == df_spark_csv_corr_localisation.LocationID,
                              'left')
                          .drop(*["LocationID", "DOLocationID"])
                          .withColumnsRenamed(
                              {"Borough": "DOBorough", "Zone": "DOZone", "service_zone" : "DO_service_zone"}
                          ))
    return (df_localisation,)


@app.cell
def _(df_localisation):
    df_localisation.printSchema()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Insertion des données dans la table des faits "fact_taxi_trips"
    """)
    return


@app.cell
def _(os):
    jdbc_url = "jdbc:postgresql://postgres:5432/data_warehouse"

    connection_properties = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }
    return (connection_properties,)


@app.cell
def _(connection_properties):
    connection_properties["password"]
    return


@app.cell
def _(df_localisation, os):
    df_localisation.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/data_warehouse") \
        .option("dbtable", "public.fact_taxi_trips") \
        .option("user", os.getenv("POSTGRES_USER")) \
        .option("password", os.getenv("POSTGRES_PASSWORD")) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .option("truncate", "true") \
        .save()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Observation des données pour la météo (extract et ajout colonnes)
    """)
    return


@app.cell
def _():
    path_meteo = (
            "s3a://raw-weather-faker/weather-fake/2025/01/5128581_1735689600.json"
        )
    return (path_meteo,)


@app.cell
def _(path_meteo, spark):
    exemple_meteo = spark.read.json(path_meteo)
    return (exemple_meteo,)


@app.cell
def _(exemple_meteo):
    exemple_meteo.head()
    return


@app.cell
def _(F, exemple_meteo):
    transformed_meteo = (exemple_meteo

        .withColumn("datetime_measure", F.from_unixtime(F.col("timestamp_unix")).cast("timestamp")) 

        .select(exemple_meteo.humidity_pct, exemple_meteo.temp_celsius, 
                F.col("datetime_measure"), exemple_meteo.weather_description,
               exemple_meteo.weather_main, exemple_meteo.wind_speed_ms)


        .withColumn("measure_dow", F.dayofweek("datetime_measure"))
        .withColumn("measure_hour", F.hour("datetime_measure"))
        .withColumn("date_measure", F.to_date('datetime_measure'))
    )
    return (transformed_meteo,)


@app.cell
def _(transformed_meteo):
    transformed_meteo.head()
    return


@app.cell
def _(transformed_meteo):
    transformed_meteo.printSchema()
    return


@app.cell
def _():
    #spark.stop()
    return


@app.cell
def _(os, transformed_meteo):
    transformed_meteo.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/data_warehouse") \
        .option("dbtable", "public.dim_weather") \
        .option("user", os.getenv("POSTGRES_USER")) \
        .option("password", os.getenv("POSTGRES_PASSWORD")) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    return


if __name__ == "__main__":
    app.run()
