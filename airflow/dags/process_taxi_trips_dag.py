from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="taxi_trips_spark_batch",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@monthly",
    catchup=False,
) as dag:

    run_spark = BashOperator(
        task_id="transform_taxi",
        bash_command="""
        /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --jars /opt/airflow/scripts/postgresql-42.7.3.jar \
          --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
          --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
          --conf spark.hadoop.fs.s3a.path.style.access=true \
          --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
          --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
          --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
          --conf spark.hadoop.fs.s3a.access.key=$DATALAKE_USER \
          --conf spark.hadoop.fs.s3a.secret.key=$DATALAKE_PASSWORD \
          /opt/spark/scripts/spark_transform_taxi.py \
          2025 2026
        """,
        # On passe les variables d'env du conteneur directement,
        # sans passer par le système de Variables Airflow
        env={
            "DATALAKE_USER":      os.environ["DATALAKE_USER"],
            "DATALAKE_PASSWORD":  os.environ["DATALAKE_PASSWORD"],
            "POSTGRES_USER":      os.environ["POSTGRES_USER"],
            "POSTGRES_PASSWORD":  os.environ["POSTGRES_PASSWORD"],
            # PATH hérité pour que spark-submit soit trouvé
            "PATH":               os.environ.get("PATH", ""),
            "JAVA_HOME":          os.environ.get("JAVA_HOME", ""),
        },
    )