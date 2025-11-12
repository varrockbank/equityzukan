"""DAG to build equity_table_extended with EDGAR shares outstanding."""
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


def run_build_equity_table_extended(**context):
    """Execute the equity_table_extended build job."""
    from pyspark.sql import SparkSession

    from src.jobs.build_equity_table_extended import build_equity_table_extended

    spark = SparkSession.builder.appName("BuildEquityTableExtended").getOrCreate()

    try:
        df = build_equity_table_extended(
            spark,
            "/opt/airflow/data/daily_table",
            "/opt/airflow/data/edgar_10q_table",
        )
        df.write.mode("overwrite").parquet("/opt/airflow/data/equity_table_extended")
        print(f"Built equity_table_extended with {df.count()} rows")
    finally:
        spark.stop()


with DAG(
    dag_id="build_equity_table_extended",
    default_args=default_args,
    description="Build equity_table_extended with EDGAR shares outstanding",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["equity", "extended", "edgar"],
) as dag:

    build_task = PythonOperator(
        task_id="build_equity_table_extended",
        python_callable=run_build_equity_table_extended,
    )
