from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import boto3
import json
import random
import os
from botocore.client import Config

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

MINIO_ENDPOINT = "http://minio:9000"
BUCKET = "raw-weather-faker"

CITY_ID = 5128581
CITY_NAME = "New York"
LAT = 40.7128
LON = -74.0060

WEATHER_STATES = [
    ("Clear", "ciel dégagé"),
    ("Clouds", "nuages"),
    ("Rain", "pluie"),
    ("Snow", "neige"),
]

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("DATALAKE_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("DATALAKE_PASSWORD", "minio_password_change_me")

# ─────────────────────────────────────────────
# MINIO CLIENT
# ─────────────────────────────────────────────

def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

# ─────────────────────────────────────────────
# GENERATION
# ─────────────────────────────────────────────

def generate_fake_weather(ts: datetime):
    state = random.choice(WEATHER_STATES)

    return {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "city_id": CITY_ID,
        "city_name": CITY_NAME,
        "lat": LAT,
        "lon": LON,
        "timestamp_unix": int(ts.timestamp()),
        "weather_main": state[0],
        "weather_description": state[1],
        "temp_celsius": round(random.uniform(-5, 30), 2),
        "humidity_pct": random.randint(20, 100),
        "wind_speed_ms": round(random.uniform(0.5, 12), 2),
    }


def store_minio(event):
    s3 = get_s3()

    ts = datetime.fromtimestamp(event["timestamp_unix"], tz=timezone.utc)

    key = (
        f"weather-fake/{ts.year:04d}/{ts.month:02d}/"
        f"{CITY_ID}_{event['timestamp_unix']}.json"
    )

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(event).encode(),
        ContentType="application/json",
    )


# ─────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────

def run_fake_weather():
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = datetime(2026, 4, 30, tzinfo=timezone.utc)

    current = start
    count = 0

    while current <= end:
        event = generate_fake_weather(current)
        store_minio(event)

        count += 1
        current += timedelta(hours=1)

    print(f"Inserted {count} fake weather events")


# ─────────────────────────────────────────────
# DAG AIRFLOW
# ─────────────────────────────────────────────

with DAG(
    dag_id="weather_fake_generator_minio",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["fake", "weather", "minio"],
) as dag:

    task = PythonOperator(
        task_id="generate_fake_weather",
        python_callable=run_fake_weather,
    )