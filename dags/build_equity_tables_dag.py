"""DAG to build asset and daily price tables."""
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


def run_build_asset_table(**context):
    """Execute the asset table build job."""
    from pyspark.sql import SparkSession

    from src.jobs.build_asset_table import build_asset_table

    spark = SparkSession.builder.appName("BuildAssetTable").getOrCreate()

    try:
        df = build_asset_table(spark, "/opt/airflow/data/tickers.csv")
        df.write.mode("overwrite").parquet("/opt/airflow/data/asset_table")
        print(f"Built asset table with {df.count()} rows")
    finally:
        spark.stop()


def run_build_daily_table(**context):
    """Execute the daily price table build job."""
    from pyspark.sql import SparkSession

    from src.jobs.build_daily_table import build_daily_table

    spark = SparkSession.builder.appName("BuildDailyTable").getOrCreate()

    try:
        df = build_daily_table(spark, "/opt/airflow/data/tickers.csv")
        df.write.mode("overwrite").parquet("/opt/airflow/data/daily_table")
        print(f"Built daily table with {df.count()} rows")
    finally:
        spark.stop()


with DAG(
    dag_id="build_equity_tables",
    default_args=default_args,
    description="Build asset and daily price tables",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["equity", "daily"],
) as dag:

    build_asset = PythonOperator(
        task_id="build_asset_table",
        python_callable=run_build_asset_table,
    )

    build_daily = PythonOperator(
        task_id="build_daily_table",
        python_callable=run_build_daily_table,
    )

    build_asset >> build_daily
