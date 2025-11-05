"""Build asset dimension table from tickers CSV."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, monotonically_increasing_id


def build_asset_table(spark: SparkSession, tickers_path: str):
    """Build and return asset table DataFrame."""
    df = spark.read.csv(tickers_path, header=True)

    # Add primary key and asset class (defaulting to 'equity' for stocks)
    df = df.withColumn("asset_id", monotonically_increasing_id())
    df = df.withColumn("asset_class", lit("equity"))

    # Reorder columns
    df = df.select("asset_id", "ticker", "asset_class")

    return df


def main():
    """Main entry point."""
    spark = (
        SparkSession.builder
        .appName("BuildAssetTable")
        .getOrCreate()
    )

    try:
        df = build_asset_table(spark, "/opt/airflow/data/tickers.csv")
        df.show()
        print(f"Total assets: {df.count()}")

        df.write.mode("overwrite").parquet("/opt/airflow/data/asset_table")
        print("Saved to /opt/airflow/data/asset_table")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
