"""Build daily_currency table with exchange rates for each currency pair."""
# TODO: Replace yfinance with a dedicated currency API (e.g., Open Exchange Rates,
#       Fixer.io, or ECB rates) for more reliable and accurate exchange rate data.
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


def fetch_exchange_rates(currency: str, transaction_currency: str) -> list[tuple]:
    """Fetch daily exchange rates for a currency pair.

    Returns list of (currency, transaction_currency, date_key, currency_unit, rate).
    """
    records = []

    # yfinance forex ticker format: USDJPY=X
    ticker = f"{currency}{transaction_currency}=X"
    print(f"Fetching exchange rates for {ticker}...")

    try:
        forex = yf.Ticker(ticker)
        hist = forex.history(start=START_DATE, end=END_DATE)

        for date, row in hist.iterrows():
            date_key = int(date.strftime("%Y%m%d"))
            rate = float(row["Close"])
            records.append((
                currency,
                transaction_currency,
                date_key,
                1.0,  # currency_unit is always 1
                rate,
            ))
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")

    return records


def build_daily_currency_table(
    spark: SparkSession,
    currency_pairs_table_path: str,
):
    """Build daily_currency table with exchange rates for all currency pairs."""
    # Read currency pairs
    pairs_df = spark.read.parquet(currency_pairs_table_path)
    pairs = [(row.currency, row.transaction_currency) for row in pairs_df.collect()]

    all_records = []
    for currency, transaction_currency in pairs:
        records = fetch_exchange_rates(currency, transaction_currency)
        all_records.extend(records)

    schema = StructType([
        StructField("currency", StringType(), nullable=False),
        StructField("transaction_currency", StringType(), nullable=False),
        StructField("date_key", IntegerType(), nullable=False),
        StructField("currency_unit", DoubleType(), nullable=False),
        StructField("transaction_currency_rate", DoubleType(), nullable=False),
    ])

    return spark.createDataFrame(all_records, schema)


def main():
    """Main entry point."""
    spark = (
        SparkSession.builder
        .appName("BuildDailyCurrencyTable")
        .getOrCreate()
    )

    try:
        df = build_daily_currency_table(
            spark,
            "/opt/airflow/data/currency_pairs_table",
        )
        df.show(20)
        print(f"Total rows: {df.count()}")

        df.write.mode("overwrite").parquet("/opt/airflow/data/daily_currency_table")
        print("Saved to /opt/airflow/data/daily_currency_table")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
