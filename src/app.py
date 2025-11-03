"""Main PySpark application entry point."""
from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "EquityZukan") -> SparkSession:
    """Create and return a SparkSession."""
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def main():
    """Main application entry point."""
    spark = create_spark_session()

    # Example: Print Spark version
    print(f"Spark version: {spark.version}")

    # Your PySpark logic here

    spark.stop()


if __name__ == "__main__":
    main()
