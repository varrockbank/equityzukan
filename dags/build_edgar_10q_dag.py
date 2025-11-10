"""DAG to build EDGAR 10-Q table with shares outstanding data."""
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


def run_build_edgar_10q_table(**context):
    """Execute the EDGAR 10-Q table build job."""
    from pyspark.sql import SparkSession

    from src.jobs.build_edgar_10q_table import build_edgar_10q_table

    spark = SparkSession.builder.appName("BuildEdgar10QTable").getOrCreate()

    try:
        df = build_edgar_10q_table(spark, "/opt/airflow/data/tickers.csv")
        df.write.mode("overwrite").parquet("/opt/airflow/data/edgar_10q_table")
        print(f"Built EDGAR 10-Q table with {df.count()} rows")
    finally:
        spark.stop()


with DAG(
    dag_id="build_edgar_10q",
    default_args=default_args,
    description="Build EDGAR 10-Q table with shares outstanding",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["edgar", "10q", "shares"],
) as dag:

    build_task = PythonOperator(
        task_id="build_edgar_10q_table",
        python_callable=run_build_edgar_10q_table,
    )
