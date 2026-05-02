from __future__ import annotations

import io
import logging
import os
from datetime import datetime

import boto3
import requests
from botocore.client import Config

from airflow import DAG
from airflow.operators.python import PythonOperator

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

BUCKET = "raw-taxi"
PREFIX = "zone"
KEY = f"{PREFIX}/taxi_zone_lookup.csv"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("DATALAKE_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("DATALAKE_PASSWORD", "minio_password_change_me")

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# MINIO CLIENT
# ─────────────────────────────────────────────

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


# ─────────────────────────────────────────────
# DOWNLOAD + UPLOAD STREAMING
# ─────────────────────────────────────────────

def ingest_zone_lookup():
    s3 = get_s3_client()

    log.info("Téléchargement CSV zone lookup...")

    with requests.get(URL, stream=True, timeout=60) as r:
        r.raise_for_status()

        buffer = io.BytesIO()

        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                buffer.write(chunk)

        buffer.seek(0)

        log.info("Upload vers MinIO → s3://%s/%s", BUCKET, KEY)

        s3.put_object(
            Bucket=BUCKET,
            Key=KEY,
            Body=buffer.read(),
            ContentType="text/csv"
        )

    log.info("Ingestion zone lookup terminée")


# ─────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="collect_taxi_zone_lookup",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
    catchup=False,
    tags=["taxi", "minio", "reference-data"],
) as dag:

    task_ingest_zone = PythonOperator(
        task_id="ingest_zone_lookup",
        python_callable=ingest_zone_lookup,
    )

    task_ingest_zone