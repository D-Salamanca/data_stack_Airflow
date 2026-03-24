import os
from pyspark.sql import SparkSession


CATALOG_URI = os.getenv("CATALOG_URI", "http://fhbd-nessie:19120/api/v1")
WAREHOUSE = os.getenv("WAREHOUSE", "s3a://iceberg/")
STORAGE_URI = os.getenv("STORAGE_URI", "http://fhbd-minio:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY", "minioadmin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY", "minioadmin123")

INPUT_PATH = os.getenv("INPUT_PATH", "s3a://raw/nyc/yellow_tripdata_2025-01.parquet")
TARGET_TABLE = os.getenv("TARGET_TABLE", "nessie.nyc.yellow_tripdata")


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("spark_load_to_iceberg")
        .config("spark.executor.instances", "1")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.catalog.nessie.uri", CATALOG_URI)
        .config("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
        .config("spark.hadoop.fs.s3a.endpoint", STORAGE_URI)
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    return spark


def get_namespace_from_table(table_name: str) -> str:
    parts = table_name.split(".")
    if len(parts) < 3:
        raise ValueError(f"TARGET_TABLE inválida: {table_name}")
    return ".".join(parts[:-1])


def main() -> None:
    spark = build_spark_session()

    namespace = get_namespace_from_table(TARGET_TABLE)

    print(f"Spark version: {spark.version}")
    print(f"Reading data from: {INPUT_PATH}")
    print(f"Writing table to: {TARGET_TABLE}")
    print(f"Namespace: {namespace}")

    df_taxis = spark.read.parquet(INPUT_PATH)

    print("Schema:")
    df_taxis.printSchema()

    print("Preview:")
    df_taxis.show(5, truncate=False)

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    df_taxis.writeTo(TARGET_TABLE).createOrReplace()

    print("Job completed successfully.")

    spark.stop()


if __name__ == "__main__":
    main()