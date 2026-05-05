"""
weather_streaming_dag.py
========================
DAG Airflow – Pipeline streaming météo.

Ce DAG fait deux choses en séquence :

  1. [generate_fake_weather]
     Génère N enregistrements météo fictifs pour les villes configurées
     et les dépose dans Minio → raw-weather-faker/weather-fake/YYYY/MM/

  2. [run_spark_streaming]
     Lance spark_streaming_transform_weather.py via spark-submit.
     Les JARs S3A et PostgreSQL sont chargés depuis le volume partagé
     /opt/spark/scripts/ → disponibles aussi sur les workers Spark.
     Grâce au trigger(availableNow=True), le job se termine proprement.

Schedule : toutes les heures.
"""

from __future__ import annotations

import json
import os
import random
from datetime import datetime, timedelta, timezone

import boto3
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from botocore.client import Config

# ─────────────────────────────────────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["DATALAKE_USER"]
MINIO_SECRET_KEY = os.environ["DATALAKE_PASSWORD"]

BUCKET_FAKER = "raw-weather-faker"

LOCATIONS: list[tuple[float, float, str, int]] = [
    (40.7128, -74.0060, "New York City", 5128581),
    (40.6413, -73.7781, "JFK Airport",   5115927),
]

FAKE_RECORDS_PER_LOCATION = 1

# Chemins des JARs — dans le volume partagé ./airflow/scripts:/opt/spark/scripts
# Ces JARs doivent être présents sur l'hôte dans ./airflow/scripts/
# Téléchargement (une seule fois) :
#   curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
#   curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
#   curl -O https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
JAR_DIR         = "/opt/spark/scripts"
JAR_HADOOP_AWS  = f"{JAR_DIR}/hadoop-aws-3.3.4.jar"
JAR_AWS_SDK     = f"{JAR_DIR}/aws-java-sdk-bundle-1.12.262.jar"
JAR_POSTGRES    = f"{JAR_DIR}/postgresql-42.7.3.jar"

ALL_JARS        = f"{JAR_HADOOP_AWS},{JAR_AWS_SDK},{JAR_POSTGRES}"
EXECUTOR_CP     = f"{JAR_HADOOP_AWS}:{JAR_AWS_SDK}:{JAR_POSTGRES}"

# ─────────────────────────────────────────────────────────────────────────────
#  Générateur de données fictives
# ─────────────────────────────────────────────────────────────────────────────

WEATHER_CONDITIONS = [
    ("Clear",        "clear sky"),
    ("Clouds",       "few clouds"),
    ("Clouds",       "scattered clouds"),
    ("Clouds",       "overcast clouds"),
    ("Rain",         "light rain"),
    ("Rain",         "moderate rain"),
    ("Drizzle",      "light intensity drizzle"),
    ("Thunderstorm", "thunderstorm with light rain"),
    ("Snow",         "light snow"),
    ("Mist",         "mist"),
    ("Fog",          "fog"),
]


def _generate_fake_record(lat, lon, city_name, city_id, ts_unix) -> dict:
    temp = round(random.uniform(-5.0, 35.0), 2)
    weather_main, weather_desc = random.choice(WEATHER_CONDITIONS)
    return {
        "ingested_at":         datetime.now(timezone.utc).isoformat(),
        "city_id":             city_id,
        "city_name":           city_name,
        "lat":                 lat,
        "lon":                 lon,
        "timestamp_unix":      ts_unix,
        "weather_main":        weather_main,
        "weather_description": weather_desc,
        "temp_celsius":        temp,
        "humidity_pct":        random.randint(20, 95),
        "wind_speed_ms":       round(random.uniform(0.0, 15.0), 2),
        "clouds_pct":          random.randint(0, 100),
    }


def generate_fake_weather(**context) -> dict:
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    now     = datetime.now(timezone.utc)
    ts_unix = int(now.replace(minute=0, second=0, microsecond=0).timestamp())
    uploaded = []

    for lat, lon, city_name, city_id in LOCATIONS:
        for _ in range(FAKE_RECORDS_PER_LOCATION):
            ts     = ts_unix + random.randint(0, 300)
            record = _generate_fake_record(lat, lon, city_name, city_id, ts)
            key    = f"weather-fake/{now.year:04d}/{now.month:02d}/{city_id}_{ts}.json"

            s3.put_object(
                Bucket=BUCKET_FAKER,
                Key=key,
                Body=json.dumps(record, ensure_ascii=False, default=str).encode(),
                ContentType="application/json",
            )
            uploaded.append(key)

    print(f"[generate_fake_weather] {len(uploaded)} fichiers déposés dans {BUCKET_FAKER}")
    return {"generated": len(uploaded), "keys": uploaded}


# ─────────────────────────────────────────────────────────────────────────────
#  Commande spark-submit — JARs locaux au lieu de --packages
#  → les workers reçoivent les JARs via extraClassPath, pas uniquement le driver
# ─────────────────────────────────────────────────────────────────────────────

SPARK_SUBMIT_CMD = f"""
/opt/spark/bin/spark-submit \\
  --master spark://spark-master:7077 \\
  --jars {ALL_JARS} \\
  --conf spark.executor.extraClassPath={EXECUTOR_CP} \\
  --conf spark.driver.extraClassPath={EXECUTOR_CP} \\
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \\
  --conf spark.hadoop.fs.s3a.path.style.access=true \\
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \\
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \\
  --conf spark.hadoop.fs.s3a.access.key=$DATALAKE_USER \\
  --conf spark.hadoop.fs.s3a.secret.key=$DATALAKE_PASSWORD \\
  /opt/spark/scripts/spark_streaming_transform_weather.py 2>&1
"""

# ─────────────────────────────────────────────────────────────────────────────
#  DAG
# ─────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner":            "airflow",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="weather_streaming_pipeline",
    description="Génère des données météo fictives puis lance le job Spark Streaming.",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["streaming", "weather", "spark", "minio"],
) as dag:

    generate_fake = PythonOperator(
        task_id="generate_fake_weather",
        python_callable=generate_fake_weather,
    )

    run_spark_streaming = BashOperator(
        task_id="run_spark_streaming",
        bash_command=SPARK_SUBMIT_CMD,
        env={
            "DATALAKE_USER":     os.environ["DATALAKE_USER"],
            "DATALAKE_PASSWORD": os.environ["DATALAKE_PASSWORD"],
            "POSTGRES_USER":     os.environ["POSTGRES_USER"],
            "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"],
            "MINIO_ENDPOINT":    os.environ["MINIO_ENDPOINT"],
            "PATH":              os.environ.get("PATH", ""),
            "JAVA_HOME":         "/usr/lib/jvm/java-17-openjdk-amd64",
        },
        execution_timeout=timedelta(minutes=30),
    )

    generate_fake >> run_spark_streaming
