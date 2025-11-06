"""Shared pytest fixtures."""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    spark = (
        SparkSession.builder
        .appName("TestSession")
        .master("local[*]")
        .getOrCreate()
    )
    yield spark
    spark.stop()
