"""Build date dimension table with NASDAQ trading calendar."""
from datetime import timedelta

import pandas_market_calendars as mcal
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType

from config.settings import END_DATE, START_DATE


def get_nasdaq_trading_days(start_date, end_date):
    """Get set of dates when NASDAQ was open."""
    nasdaq = mcal.get_calendar("NASDAQ")
    schedule = nasdaq.schedule(start_date=start_date, end_date=end_date)
    return set(schedule.index.date)


DAYS_OF_WEEK = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]


def generate_date_records(start_date, end_date, trading_days):
    """Generate date records with nasdaq_open flag and day of week."""
    current = start_date
    while current <= end_date:
        date_key = int(current.strftime("%Y%m%d"))
        nasdaq_open = current in trading_days
        day_of_week = DAYS_OF_WEEK[current.weekday()]
        yield (date_key, day_of_week, nasdaq_open)
        current += timedelta(days=1)


def build_date_table(spark: SparkSession):
    """Build and return date table DataFrame."""
    trading_days = get_nasdaq_trading_days(START_DATE, END_DATE)
    records = list(generate_date_records(START_DATE, END_DATE, trading_days))

    schema = StructType([
        StructField("date_key", IntegerType(), nullable=False),
        StructField("day_of_week", StringType(), nullable=False),
        StructField("nasdaq_open", BooleanType(), nullable=False),
    ])

    return spark.createDataFrame(records, schema)


def main():
    """Main entry point."""
    spark = (
        SparkSession.builder
        .appName("BuildDateTable")
        .getOrCreate()
    )

    try:
        df = build_date_table(spark)
        df.show(10)
        print(f"Total rows: {df.count()}")
        print(f"Trading days: {df.filter(df.nasdaq_open).count()}")

        # Save as parquet
        df.write.mode("overwrite").parquet("/opt/airflow/data/date_table")
        print("Saved to /opt/airflow/data/date_table")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
