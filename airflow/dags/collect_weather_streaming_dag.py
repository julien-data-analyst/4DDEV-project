# ─────────────────────────────────────────────────────────────────────────────
# AIRFLOW DAG
# ─────────────────────────────────────────────────────────────────────────────
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sys

sys.path.append("/opt/airflow/scripts")

from collecte_api_weather import run_weather_to_minio

with DAG(
        dag_id="weather_streaming_ingestion_minio",
        start_date=datetime(2026, 1, 1),
        schedule="@hourly",
        catchup=False,
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
        },
        tags=["weather", "minio", "ingestion"],
    ) as dag:

        task = PythonOperator(
            task_id="weather_ingest_minio",
            python_callable=run_weather_to_minio,
        )