from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
import sys

sys.path.append("/opt/airflow/scripts")

from collecte_taxi_trips import collect_taxi_batch

FIRST_YEAR = 2026
FIRST_MONTH = 1

default_args = {
        "owner": "airflow",
        "retries": 3,
        "retry_delay": __import__("datetime").timedelta(minutes=10),
        "email_on_failure": False,
    }
 
with DAG(
        dag_id="collecte_taxi_trips_batch",
        description="Ingestion batch mensuelle des taxis jaunes NYC (2026 → maintenant)",
        schedule_interval="@monthly",   # 1er de chaque mois à 3h UTC
        start_date=days_ago(1),
        catchup=False,
        default_args=default_args,
        tags=["ingestion", "batch", "taxi", "minio"],
    ) as dag:
 
        task_collect = PythonOperator(
            task_id="telecharger_parquets_manquants",
            python_callable=collect_taxi_batch,
            op_kwargs={"year_from": FIRST_YEAR, "month_from": FIRST_MONTH, "force": False},
        )
