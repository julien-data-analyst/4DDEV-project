"""
weather_streaming_dag.py
========================
DAG Airflow – Pipeline streaming météo.

Ce DAG fait deux choses en séquence :

  1. [generate_fake_weather]
     Génère N enregistrements météo fictifs pour les villes configurées
     et les dépose dans Minio → raw-weather-faker/weather-fake/YYYY/MM/
     Format identique aux données réelles (même schéma JSON), avec des
     valeurs aléatoires réalistes pour NYC.

  2. [run_spark_streaming]
     Lance spark_streaming_weather.py via spark-submit.
     Le script surveille les deux buckets (raw-weather + raw-weather-faker),
     transforme les nouveaux JSON et les insère dans PostgreSQL (dim_weather).
     Grâce au trigger(availableNow=True), le job Spark se termine tout seul
     une fois tous les fichiers disponibles traités → pas de process zombie.

Schedule : toutes les heures (même fréquence que la collecte réelle).

Idempotence : le checkpoint Spark dans Minio garantit qu'un fichier
déjà traité ne sera jamais relu, même si le DAG est relancé manuellement.
"""

from __future__ import annotations

import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

import boto3
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from botocore.client import Config

# ─────────────────────────────────────────────────────────────────────────────
#  Configuration
# ─────────────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = os.environ["MINIO_ENDPOINT"]          # http://minio:9000
MINIO_ACCESS_KEY = os.environ["DATALAKE_USER"]
MINIO_SECRET_KEY = os.environ["DATALAKE_PASSWORD"]

BUCKET_FAKER = "raw-weather-faker"

# Villes pour la génération fictive (même liste que la collecte réelle)
_raw_locs = os.environ.get("WEATHER_LOCATIONS", "40.7128,-74.0060|40.6413,-73.7781")
LOCATIONS: list[tuple[float, float, str, int]] = [
    # (lat, lon, city_name, city_id)
    (40.7128, -74.0060, "New York City", 5128581),
    (40.6413, -73.7781, "JFK Airport",   5115927),
]

# Nombre de fichiers fictifs générés par exécution du DAG (1 par ville par heure)
FAKE_RECORDS_PER_LOCATION = 1

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


def _generate_fake_record(
    lat: float,
    lon: float,
    city_name: str,
    city_id: int,
    ts_unix: int,
) -> dict:
    """Génère un enregistrement météo fictif réaliste pour NYC."""
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
        "clouds_pct":          random.randint(0, 100)
    }


def generate_fake_weather(**context) -> dict:
    """
    Callable Python pour PythonOperator.
    Génère les enregistrements fictifs et les pousse dans Minio.
    Retourne un résumé pour les logs Airflow.
    """
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    # Horodatage de référence : l'heure courante arrondie à l'heure
    now = datetime.now(timezone.utc)
    ts_unix = int(now.replace(minute=0, second=0, microsecond=0).timestamp())

    uploaded = []

    for lat, lon, city_name, city_id in LOCATIONS:
        for _ in range(FAKE_RECORDS_PER_LOCATION):
            # Décalage aléatoire de quelques minutes pour simuler une vraie collecte
            ts = ts_unix + random.randint(0, 300)
            record = _generate_fake_record(lat, lon, city_name, city_id, ts)

            # Chemin : weather-fake/YYYY/MM/<city_id>_<ts_unix>.json
            key = (
                f"weather-fake/{now.year:04d}/{now.month:02d}"
                f"/{city_id}_{ts}.json"
            )

            s3.put_object(
                Bucket=BUCKET_FAKER,
                Key=key,
                Body=json.dumps(record, ensure_ascii=False, default=str).encode(),
                ContentType="application/json",
            )
            uploaded.append(key)

    summary = {"generated": len(uploaded), "keys": uploaded}
    print(f"[generate_fake_weather] {len(uploaded)} fichiers déposés dans {BUCKET_FAKER}")
    return summary


# ─────────────────────────────────────────────────────────────────────────────
#  Commande spark-submit
# ─────────────────────────────────────────────────────────────────────────────

SPARK_SUBMIT_CMD = """
/opt/spark/bin/spark-submit \\
  --master spark://spark-master:7077 \\
  --jars /opt/airflow/scripts/postgresql-42.7.3.jar \\
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \\
  --conf spark.hadoop.fs.s3a.path.style.access=true \\
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \\
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \\
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \\
  --conf spark.hadoop.fs.s3a.access.key=$DATALAKE_USER \\
  --conf spark.hadoop.fs.s3a.secret.key=$DATALAKE_PASSWORD \\
  /opt/spark/scripts/spark_streaming_weather.py
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
    description=(
        "Génère des données météo fictives dans Minio puis lance "
        "le job Spark Structured Streaming pour traiter les deux buckets."
    ),
    schedule_interval="@hourly",   # toutes les heures
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["streaming", "weather", "spark", "minio"],
) as dag:

    # ── Tâche 1 : génération des données fictives ────────────────────────────
    generate_fake = PythonOperator(
        task_id="generate_fake_weather",
        python_callable=generate_fake_weather,
        # Pas de dépendance à des Variables Airflow : on lit os.environ directement
    )

    # ── Tâche 2 : lancement du job Spark Streaming ──────────────────────────
    run_spark_streaming = BashOperator(
        task_id="run_spark_streaming",
        bash_command=SPARK_SUBMIT_CMD,
        # Variables d'environnement lues depuis le conteneur Airflow (.env)
        env={
            "DATALAKE_USER":      os.environ["DATALAKE_USER"],
            "DATALAKE_PASSWORD":  os.environ["DATALAKE_PASSWORD"],
            "POSTGRES_USER":      os.environ["POSTGRES_USER"],
            "POSTGRES_PASSWORD":  os.environ["POSTGRES_PASSWORD"],
            "MINIO_ENDPOINT":     os.environ["MINIO_ENDPOINT"],
            "PATH":               os.environ.get("PATH", ""),
            "JAVA_HOME":          os.environ.get("JAVA_HOME", ""),
        },
        # Timeout généreux : le job Spark peut prendre quelques minutes
        # pour télécharger les packages au premier lancement
        execution_timeout=timedelta(minutes=30),
    )

    # ── Dépendance : génération d'abord, streaming ensuite ──────────────────
    generate_fake >> run_spark_streaming