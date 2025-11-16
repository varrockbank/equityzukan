"""Master DAG to build all tables with explicit dependencies."""
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


def run_build_date_table(**context):
    """Execute the date table build job."""
    from pyspark.sql import SparkSession

    from src.jobs.build_date_table import build_date_table

    spark = SparkSession.builder.appName("BuildDateTable").getOrCreate()

    try:
        df = build_date_table(spark)
        df.write.mode("overwrite").parquet("/opt/airflow/data/date_table")
        print(f"Built date table with {df.count()} rows")
    finally:
        spark.stop()


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
    dag_id="build_all_tables",
    default_args=default_args,
    description="Build all tables with full dependency graph",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["master", "all", "equity", "date"],
) as dag:

    # Date dimension (independent)
    build_date = PythonOperator(
        task_id="build_date_table",
        python_callable=run_build_date_table,
    )

    # Asset table (independent)
    build_asset = PythonOperator(
        task_id="build_asset_table",
        python_callable=run_build_asset_table,
    )

    # Daily prices (independent)
    build_daily = PythonOperator(
        task_id="build_daily_table",
        python_callable=run_build_daily_table,
    )

    # EDGAR 10-Q (independent)
    build_edgar_10q = PythonOperator(
        task_id="build_edgar_10q_table",
        python_callable=run_build_edgar_10q_table,
    )

    # Equity extended (depends on daily + edgar_10q)
    build_equity_extended = PythonOperator(
        task_id="build_equity_table_extended",
        python_callable=run_build_equity_table_extended,
    )

    # Final node - depends on date_table and equity_table_extended
    all_tables_complete = EmptyOperator(
        task_id="all_tables_complete",
    )

    # Dependency graph:
    #
    # build_date_table ───────────────────────────────────┐
    #                                                     │
    # build_asset_table ──┬─────────────────────────┐     │
    #                     │                         │     │
    #                     ▼                         ▼     ├──► all_tables_complete
    #             build_daily_table      build_edgar_10q  │
    #                     │                         │     │
    #                     └────────────┬────────────┘     │
    #                                  ▼                  │
    #                       build_equity_extended ────────┘
    #
    build_asset >> [build_daily, build_edgar_10q]
    [build_daily, build_edgar_10q] >> build_equity_extended
    [build_date, build_equity_extended] >> all_tables_complete
