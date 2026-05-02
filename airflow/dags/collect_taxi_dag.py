from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append("/opt/airflow/scripts")

from collecte_taxi_trips import collect_taxi_batch


# def run_ingestion(**context):
#     """
#     Utilise la date du DAG run pour déterminer le mois à ingérer.
#     """
#     execution_date = context["logical_date"]

#     year = 2013 #execution_date.year
#     month = 12    #execution_date.month

#     return collect_taxi_batch(
#         year_from=year,
#         month_from=month,
#         year_to=year,
#         month_to=month,
#         force=False
#     )

def run_ingestion(): 
    return collect_taxi_batch( year_from=2025, month_from=1, force=False )


with DAG(
    dag_id="taxi_ingestion_minio",
    start_date=datetime(2009, 1, 1),
    schedule="@monthly",
    catchup=True,
    tags=["taxi", "ingestion", "minio"],
) as dag:

    ingest_task = PythonOperator(
        task_id="collect_taxi_data",
        python_callable=run_ingestion,
        provide_context=True,
    )