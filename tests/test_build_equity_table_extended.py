"""Tests for build_equity_table_extended job."""
import tempfile
import os

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.jobs.build_equity_table_extended import build_equity_table_extended


class TestBuildEquityTableExtended:
    """Tests for build_equity_table_extended function."""

    def test_joins_daily_with_latest_edgar(self, spark):
        """Test that daily records are joined with the latest EDGAR filing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            daily_path = os.path.join(tmpdir, "daily_table")
            edgar_path = os.path.join(tmpdir, "edgar_10q_table")

            # Create daily data
            daily_schema = StructType([
                StructField("ticker", StringType(), nullable=False),
                StructField("source", StringType(), nullable=False),
                StructField("date_key", IntegerType(), nullable=False),
                StructField("stock_open", DoubleType(), nullable=False),
                StructField("stock_high", DoubleType(), nullable=False),
                StructField("stock_low", DoubleType(), nullable=False),
                StructField("stock_close", DoubleType(), nullable=False),
                StructField("volume", LongType(), nullable=False),
            ])
            daily_data = [
                ("AAPL", "yahoo_finance", 20240115, 100.0, 105.0, 99.0, 102.0, 1000000),
                ("AAPL", "yahoo_finance", 20240401, 110.0, 115.0, 109.0, 112.0, 1100000),
            ]
            daily_df = spark.createDataFrame(daily_data, daily_schema)
            daily_df.write.parquet(daily_path)

            # Create EDGAR data with two filings
            edgar_schema = StructType([
                StructField("ticker", StringType(), nullable=False),
                StructField("cik", StringType(), nullable=False),
                StructField("date_key", IntegerType(), nullable=False),
                StructField("shares_outstanding", LongType(), nullable=False),
            ])
            edgar_data = [
                ("AAPL", "0000320193", 20231231, 15000000000),  # Before START_DATE
                ("AAPL", "0000320193", 20240331, 15500000000),  # Q1 2024
            ]
            edgar_df = spark.createDataFrame(edgar_data, edgar_schema)
            edgar_df.write.parquet(edgar_path)

            # Build extended table
            result_df = build_equity_table_extended(spark, daily_path, edgar_path)
            result = result_df.collect()

            assert len(result) == 2

            # First daily record (Jan 15) should use Dec 31 filing
            jan_record = [r for r in result if r.date_key == 20240115][0]
            assert jan_record.edgar_shares_outstanding == 15000000000
            assert jan_record.edgar_10q_date_key == 20231231

            # Second daily record (Apr 1) should use Mar 31 filing
            apr_record = [r for r in result if r.date_key == 20240401][0]
            assert apr_record.edgar_shares_outstanding == 15500000000
            assert apr_record.edgar_10q_date_key == 20240331

    def test_handles_missing_edgar_data(self, spark):
        """Test that daily records without EDGAR data have null values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            daily_path = os.path.join(tmpdir, "daily_table")
            edgar_path = os.path.join(tmpdir, "edgar_10q_table")

            # Create daily data for a ticker with no EDGAR filings
            daily_schema = StructType([
                StructField("ticker", StringType(), nullable=False),
                StructField("source", StringType(), nullable=False),
                StructField("date_key", IntegerType(), nullable=False),
                StructField("stock_open", DoubleType(), nullable=False),
                StructField("stock_high", DoubleType(), nullable=False),
                StructField("stock_low", DoubleType(), nullable=False),
                StructField("stock_close", DoubleType(), nullable=False),
                StructField("volume", LongType(), nullable=False),
            ])
            daily_data = [
                ("GME", "yahoo_finance", 20240115, 15.0, 16.0, 14.0, 15.5, 500000),
            ]
            daily_df = spark.createDataFrame(daily_data, daily_schema)
            daily_df.write.parquet(daily_path)

            # Create empty EDGAR data
            edgar_schema = StructType([
                StructField("ticker", StringType(), nullable=False),
                StructField("cik", StringType(), nullable=False),
                StructField("date_key", IntegerType(), nullable=False),
                StructField("shares_outstanding", LongType(), nullable=False),
            ])
            edgar_df = spark.createDataFrame([], edgar_schema)
            edgar_df.write.parquet(edgar_path)

            # Build extended table
            result_df = build_equity_table_extended(spark, daily_path, edgar_path)
            result = result_df.collect()

            assert len(result) == 1
            assert result[0].ticker == "GME"
            assert result[0].edgar_shares_outstanding is None
            assert result[0].edgar_10q_date_key is None

    def test_preserves_all_daily_columns(self, spark):
        """Test that all daily table columns are preserved."""
        with tempfile.TemporaryDirectory() as tmpdir:
            daily_path = os.path.join(tmpdir, "daily_table")
            edgar_path = os.path.join(tmpdir, "edgar_10q_table")

            # Create daily data
            daily_schema = StructType([
                StructField("ticker", StringType(), nullable=False),
                StructField("source", StringType(), nullable=False),
                StructField("date_key", IntegerType(), nullable=False),
                StructField("stock_open", DoubleType(), nullable=False),
                StructField("stock_high", DoubleType(), nullable=False),
                StructField("stock_low", DoubleType(), nullable=False),
                StructField("stock_close", DoubleType(), nullable=False),
                StructField("volume", LongType(), nullable=False),
            ])
            daily_data = [
                ("NVDA", "yahoo_finance", 20240115, 500.0, 510.0, 495.0, 505.0, 2000000),
            ]
            daily_df = spark.createDataFrame(daily_data, daily_schema)
            daily_df.write.parquet(daily_path)

            # Create EDGAR data
            edgar_schema = StructType([
                StructField("ticker", StringType(), nullable=False),
                StructField("cik", StringType(), nullable=False),
                StructField("date_key", IntegerType(), nullable=False),
                StructField("shares_outstanding", LongType(), nullable=False),
            ])
            edgar_data = [
                ("NVDA", "0001045810", 20231231, 25000000000),
            ]
            edgar_df = spark.createDataFrame(edgar_data, edgar_schema)
            edgar_df.write.parquet(edgar_path)

            # Build extended table
            result_df = build_equity_table_extended(spark, daily_path, edgar_path)
            result = result_df.collect()[0]

            # Verify all daily columns are preserved
            assert result.ticker == "NVDA"
            assert result.source == "yahoo_finance"
            assert result.date_key == 20240115
            assert result.stock_open == 500.0
            assert result.stock_high == 510.0
            assert result.stock_low == 495.0
            assert result.stock_close == 505.0
            assert result.volume == 2000000

            # Verify EDGAR columns are added
            assert result.edgar_shares_outstanding == 25000000000
            assert result.edgar_10q_date_key == 20231231
            assert result.edgar_cik == "0001045810"

    def test_multiple_tickers(self, spark):
        """Test that multiple tickers are joined correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            daily_path = os.path.join(tmpdir, "daily_table")
            edgar_path = os.path.join(tmpdir, "edgar_10q_table")

            # Create daily data for multiple tickers
            daily_schema = StructType([
                StructField("ticker", StringType(), nullable=False),
                StructField("source", StringType(), nullable=False),
                StructField("date_key", IntegerType(), nullable=False),
                StructField("stock_open", DoubleType(), nullable=False),
                StructField("stock_high", DoubleType(), nullable=False),
                StructField("stock_low", DoubleType(), nullable=False),
                StructField("stock_close", DoubleType(), nullable=False),
                StructField("volume", LongType(), nullable=False),
            ])
            daily_data = [
                ("AAPL", "yahoo_finance", 20240115, 100.0, 105.0, 99.0, 102.0, 1000000),
                ("NVDA", "yahoo_finance", 20240115, 500.0, 510.0, 495.0, 505.0, 2000000),
            ]
            daily_df = spark.createDataFrame(daily_data, daily_schema)
            daily_df.write.parquet(daily_path)

            # Create EDGAR data for multiple tickers
            edgar_schema = StructType([
                StructField("ticker", StringType(), nullable=False),
                StructField("cik", StringType(), nullable=False),
                StructField("date_key", IntegerType(), nullable=False),
                StructField("shares_outstanding", LongType(), nullable=False),
            ])
            edgar_data = [
                ("AAPL", "0000320193", 20231231, 15000000000),
                ("NVDA", "0001045810", 20231231, 25000000000),
            ]
            edgar_df = spark.createDataFrame(edgar_data, edgar_schema)
            edgar_df.write.parquet(edgar_path)

            # Build extended table
            result_df = build_equity_table_extended(spark, daily_path, edgar_path)
            results = {r.ticker: r for r in result_df.collect()}

            assert len(results) == 2
            assert results["AAPL"].edgar_shares_outstanding == 15000000000
            assert results["NVDA"].edgar_shares_outstanding == 25000000000
