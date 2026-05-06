from pyspark.sql import SparkSession

# ─────────────────────────────────────────────
# ENV
# ─────────────────────────────────────────────

POSTGRES_URL = "jdbc:postgresql://postgres:5432/data_warehouse"

INPUT_PATH = "s3a://processed/weather-prepared/"

# ─────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────

def create_spark():
    return SparkSession.builder.appName("parquet-to-postgres").getOrCreate()

def main():
    spark = create_spark()

    df = spark.read.parquet(INPUT_PATH)

    df.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "public.dim_weather") \
        .option("user", "dwh_user") \
        .option("password", "dwh_password") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

if __name__ == "__main__":
    main()