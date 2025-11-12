"""Build equity_table_extended by joining daily prices with EDGAR shares outstanding."""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def build_equity_table_extended(
    spark: SparkSession,
    daily_table_path: str,
    edgar_10q_table_path: str,
):
    """Build equity_table_extended by joining daily with EDGAR data.

    For each daily record, finds the latest EDGAR shares_outstanding
    on or before that date_key.

    Primary key for edgar_10q: (ticker, date_key)
    Foreign key in equity_table_extended: (ticker, edgar_10q_date_key)
    """
    # Read source tables
    daily_df = spark.read.parquet(daily_table_path)
    edgar_df = spark.read.parquet(edgar_10q_table_path)

    # Rename edgar date_key to avoid collision
    edgar_df = edgar_df.select(
        F.col("ticker").alias("edgar_ticker"),
        F.col("cik"),
        F.col("date_key").alias("edgar_date_key"),
        F.col("shares_outstanding"),
    )

    # Join daily with edgar where edgar_date_key <= daily date_key
    # This gets all EDGAR filings on or before each daily date
    joined_df = daily_df.join(
        edgar_df,
        (daily_df.ticker == edgar_df.edgar_ticker)
        & (edgar_df.edgar_date_key <= daily_df.date_key),
        "left",
    )

    # For each daily record, keep only the latest EDGAR filing
    window = Window.partitionBy(
        daily_df.ticker, daily_df.date_key
    ).orderBy(F.col("edgar_date_key").desc())

    ranked_df = joined_df.withColumn("rank", F.row_number().over(window))
    latest_df = ranked_df.filter(F.col("rank") == 1).drop("rank", "edgar_ticker")

    # Rename columns for clarity
    result_df = latest_df.select(
        daily_df.ticker,
        daily_df.source,
        daily_df.date_key,
        daily_df.stock_open,
        daily_df.stock_high,
        daily_df.stock_low,
        daily_df.stock_close,
        daily_df.volume,
        F.col("shares_outstanding").alias("edgar_shares_outstanding"),
        F.col("edgar_date_key").alias("edgar_10q_date_key"),  # FK to edgar_10q
        F.col("cik").alias("edgar_cik"),
    )

    return result_df


def main():
    """Main entry point."""
    spark = (
        SparkSession.builder
        .appName("BuildEquityTableExtended")
        .getOrCreate()
    )

    try:
        df = build_equity_table_extended(
            spark,
            "/opt/airflow/data/daily_table",
            "/opt/airflow/data/edgar_10q_table",
        )
        df.show(20)
        print(f"Total rows: {df.count()}")

        # Show rows with missing EDGAR data
        missing_count = df.filter(F.col("edgar_shares_outstanding").isNull()).count()
        print(f"Rows missing EDGAR data: {missing_count}")

        df.write.mode("overwrite").parquet("/opt/airflow/data/equity_table_extended")
        print("Saved to /opt/airflow/data/equity_table_extended")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
