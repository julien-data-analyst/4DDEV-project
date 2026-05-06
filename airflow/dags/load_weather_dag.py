from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="weather_parquet_to_postgres",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    load_to_db = BashOperator(
        task_id="load_parquet_to_postgres",
        bash_command="""
/opt/spark/bin/spark-submit \
/opt/spark/scripts/spark_weather_bdd.py""")