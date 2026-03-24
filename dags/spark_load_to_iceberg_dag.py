from datetime import datetime
import os

import boto3
from botocore.client import Config

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}

ICEBERG_VERSION = "1.6.1"
NESSIE_VERSION  = "0.95.0"
HADOOP_VERSION  = "3.3.4"
AWS_SDK_VERSION = "1.12.262"
SCALA_VERSION   = "2.12"

PACKAGES = ",".join([
    f"org.apache.iceberg:iceberg-spark-runtime-3.5_{SCALA_VERSION}:{ICEBERG_VERSION}",
    f"org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_{SCALA_VERSION}:{NESSIE_VERSION}",
    f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}",
    f"com.amazonaws:aws-java-sdk-bundle:{AWS_SDK_VERSION}",
])


def crear_buckets_y_subir_datos():
    """Crea los buckets en MinIO y sube el parquet si no existe."""
    import urllib.request

    endpoint  = os.getenv("MINIO_ENDPOINT", "http://fhbd-minio:9000")
    access    = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret    = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    bucket_raw     = os.getenv("MINIO_BUCKET_RAW", "raw")
    bucket_iceberg = os.getenv("MINIO_BUCKET_ICEBERG", "iceberg")
    object_key     = os.getenv("MINIO_OBJECT_KEY", "nyc/yellow_tripdata_2025-01.parquet")
    parquet_url    = os.getenv("PARQUET_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet")

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    # Crear buckets si no existen
    for bucket in [bucket_raw, bucket_iceberg]:
        try:
            s3.head_bucket(Bucket=bucket)
            print(f"Bucket '{bucket}' ya existe.")
        except Exception:
            s3.create_bucket(Bucket=bucket)
            print(f"Bucket '{bucket}' creado.")

    # Verificar si el parquet ya está subido
    try:
        s3.head_object(Bucket=bucket_raw, Key=object_key)
        print(f"Archivo '{object_key}' ya existe en '{bucket_raw}'. Saltando descarga.")
    except Exception:
        print(f"Descargando {parquet_url} ...")
        local_path = f"/tmp/{object_key.split('/')[-1]}"
        urllib.request.urlretrieve(parquet_url, local_path)
        print(f"Subiendo a s3://{bucket_raw}/{object_key} ...")
        s3.upload_file(local_path, bucket_raw, object_key)
        print("Subida completada.")


with DAG(
    dag_id="spark_load_to_iceberg_dag",
    default_args=default_args,
    description="Carga datos desde MinIO hacia Iceberg con Spark",
    start_date=datetime(2026, 3, 23),
    schedule=None,
    catchup=False,
    tags=["spark", "iceberg", "nessie", "minio"],
) as dag:

    preparar_minio = PythonOperator(
        task_id="preparar_minio",
        python_callable=crear_buckets_y_subir_datos,
    )

    run_spark_load_to_iceberg = SparkSubmitOperator(
        task_id="run_spark_load_to_iceberg",
        application="/opt/airflow/scripts/Spark-load-to-Iceberg.py",
        conn_id="spark_default",
        name="spark_load_to_iceberg_job",
        verbose=True,
        packages=PACKAGES,
        conf={
            "spark.submit.deployMode": "client",
            "spark.driver.bindAddress": "0.0.0.0",
            "spark.executor.instances": "1",
            "spark.executor.cores": "2",
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
            ),
            "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
            "spark.sql.catalog.nessie.uri": os.getenv("NESSIE_URL", "http://fhbd-nessie:19120/api/v1"),
            "spark.sql.catalog.nessie.ref": os.getenv("NESSIE_BRANCH", "main"),
            "spark.sql.catalog.nessie.authentication.type": "NONE",
            "spark.sql.catalog.nessie.warehouse": "s3a://iceberg/",
            "spark.hadoop.fs.s3a.endpoint": os.getenv("MINIO_ENDPOINT", "http://fhbd-minio:9000"),
            "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": (
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
            ),
        },
        env_vars={
            "CATALOG_URI": os.getenv("NESSIE_URL", "http://fhbd-nessie:19120/api/v1"),
            "WAREHOUSE": "s3a://iceberg/",
            "STORAGE_URI": os.getenv("MINIO_ENDPOINT", "http://fhbd-minio:9000"),
            "AWS_ACCESS_KEY": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            "AWS_SECRET_KEY": os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
            "INPUT_PATH": f"s3a://{os.getenv('MINIO_BUCKET_RAW', 'raw')}/{os.getenv('MINIO_OBJECT_KEY', 'nyc/yellow_tripdata_2025-01.parquet')}",
            "TARGET_TABLE": f"nessie.{os.getenv('ICEBERG_NAMESPACE', 'nyc')}.{os.getenv('ICEBERG_TABLE', 'yellow_tripdata')}",
        },
    )

    # Primero preparar MinIO, luego ejecutar Spark
    preparar_minio >> run_spark_load_to_iceberg