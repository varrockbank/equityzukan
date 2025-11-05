"""Build daily price table by fetching data from Yahoo Finance."""
import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from config.settings import END_DATE, START_DATE


def fetch_daily_prices(tickers: list[str], start_date, end_date):
    """Fetch daily OHLC data for tickers from Yahoo Finance."""
    records = []

    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        stock = yf.Ticker(ticker)
        hist = stock.history(start=start_date, end=end_date)

        for date, row in hist.iterrows():
            date_key = int(date.strftime("%Y%m%d"))
            records.append((
                ticker,
                "NASDAQ",
                date_key,
                float(row["Open"]),
                float(row["High"]),
                float(row["Low"]),
                float(row["Close"]),
            ))

    return records


def build_daily_table(spark: SparkSession, tickers_path: str):
    """Build and return daily price table DataFrame."""
    # Read tickers from CSV
    tickers_df = spark.read.csv(tickers_path, header=True)
    tickers = [row.ticker for row in tickers_df.collect()]

    # Fetch price data
    records = fetch_daily_prices(tickers, START_DATE, END_DATE)

    schema = StructType([
        StructField("ticker", StringType(), nullable=False),
        StructField("exchange", StringType(), nullable=False),
        StructField("date_key", IntegerType(), nullable=False),
        StructField("stock_open", DoubleType(), nullable=False),
        StructField("stock_high", DoubleType(), nullable=False),
        StructField("stock_low", DoubleType(), nullable=False),
        StructField("stock_close", DoubleType(), nullable=False),
    ])

    return spark.createDataFrame(records, schema)


def main():
    """Main entry point."""
    spark = (
        SparkSession.builder
        .appName("BuildDailyTable")
        .getOrCreate()
    )

    try:
        df = build_daily_table(spark, "/opt/airflow/data/tickers.csv")
        df.show(10)
        print(f"Total rows: {df.count()}")

        df.write.mode("overwrite").parquet("/opt/airflow/data/daily_table")
        print("Saved to /opt/airflow/data/daily_table")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
