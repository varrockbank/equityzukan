"""Example Airflow DAG with PySpark integration."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_spark_job(**context):
    """Execute PySpark job."""
    from src.app import create_spark_session

    spark = create_spark_session("AirflowSparkJob")
    try:
        # Your Spark logic here
        print(f"Running Spark job, version: {spark.version}")
    finally:
        spark.stop()


with DAG(
    dag_id="example_spark_dag",
    default_args=default_args,
    description="Example DAG with PySpark",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "spark"],
) as dag:

    start = EmptyOperator(task_id="start")

    spark_task = PythonOperator(
        task_id="run_spark_job",
        python_callable=run_spark_job,
    )

    end = EmptyOperator(task_id="end")

    start >> spark_task >> end
