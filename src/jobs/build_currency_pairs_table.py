"""Build currency_pairs table from config CSV."""
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)


def build_currency_pairs_table(spark: SparkSession, currency_pairs_path: str):
    """Build and return currency_pairs table DataFrame."""
    schema = StructType([
        StructField("currency", StringType(), nullable=False),
        StructField("transaction_currency", StringType(), nullable=False),
    ])

    return spark.read.csv(currency_pairs_path, header=True, schema=schema)


def main():
    """Main entry point."""
    spark = (
        SparkSession.builder
        .appName("BuildCurrencyPairsTable")
        .getOrCreate()
    )

    try:
        df = build_currency_pairs_table(spark, "/opt/airflow/data/currency_pairs.csv")
        df.show()
        print(f"Total rows: {df.count()}")

        df.write.mode("overwrite").parquet("/opt/airflow/data/currency_pairs_table")
        print("Saved to /opt/airflow/data/currency_pairs_table")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
