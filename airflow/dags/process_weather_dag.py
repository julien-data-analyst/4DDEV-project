from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="weather_streaming_to_parquet",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["weather", "minio", "transform", 'parquet'],
) as dag:

    run_stream = BashOperator(
        task_id="run_streaming",
        bash_command="""
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
            --jars /opt/spark/scripts/postgresql-42.7.3.jar,/opt/spark/scripts/hadoop-aws-3.3.4.jar,/opt/spark/scripts/aws-java-sdk-bundle-1.12.262.jar \
            --conf spark.executor.extraClassPath=/opt/spark/scripts/hadoop-aws-3.3.4.jar:/opt/spark/scripts/aws-java-sdk-bundle-1.12.262.jar:/opt/spark/scripts/postgresql-42.7.3.jar \
            --conf spark.driver.extraClassPath=/opt/spark/scripts/hadoop-aws-3.3.4.jar:/opt/spark/scripts/aws-java-sdk-bundle-1.12.262.jar:/opt/spark/scripts/postgresql-42.7.3.jar \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
            --conf spark.hadoop.fs.s3a.access.key=$DATALAKE_USER \
            --conf spark.hadoop.fs.s3a.secret.key=$DATALAKE_PASSWORD \
        /opt/spark/scripts/spark_streaming_transform_weather_parquet.py
"""
    )