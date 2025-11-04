"""DAG to build the date dimension table."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_build_date_table(**context):
    """Execute the date table build job."""
    from src.jobs.build_date_table import build_date_table
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("BuildDateTable")
        .getOrCreate()
    )

    try:
        df = build_date_table(spark)
        df.write.mode("overwrite").parquet("/opt/airflow/data/date_table")
        print(f"Built date table with {df.count()} rows")
    finally:
        spark.stop()


with DAG(
    dag_id="build_date_table",
    default_args=default_args,
    description="Build date dimension table with NASDAQ calendar",
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dimension", "date"],
) as dag:

    build_task = PythonOperator(
        task_id="build_date_table",
        python_callable=run_build_date_table,
    )
