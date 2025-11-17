"""Tests for build_daily_currency_table job."""
import tempfile
import os
from unittest.mock import patch, MagicMock

import pandas as pd

from src.jobs.build_daily_currency_table import (
    fetch_exchange_rates,
    build_daily_currency_table,
)


class TestFetchExchangeRates:
    """Tests for fetch_exchange_rates function."""

    @patch("src.jobs.build_daily_currency_table.yf.Ticker")
    def test_fetches_rates_for_currency_pair(self, mock_ticker):
        """Test that exchange rates are fetched for a currency pair."""
        mock_forex = MagicMock()
        mock_ticker.return_value = mock_forex

        # Create mock historical data
        mock_hist = pd.DataFrame({
            "Close": [150.0, 151.0],
        }, index=pd.to_datetime(["2024-01-02", "2024-01-03"]))
        mock_forex.history.return_value = mock_hist

        records = fetch_exchange_rates("USD", "JPY")

        assert len(records) == 2
        assert records[0] == ("USD", "JPY", 20240102, 1.0, 150.0)
        assert records[1] == ("USD", "JPY", 20240103, 1.0, 151.0)
        mock_ticker.assert_called_once_with("USDJPY=X")

    @patch("src.jobs.build_daily_currency_table.yf.Ticker")
    def test_currency_unit_is_always_one(self, mock_ticker):
        """Test that currency_unit is always 1."""
        mock_forex = MagicMock()
        mock_ticker.return_value = mock_forex

        mock_hist = pd.DataFrame({
            "Close": [0.85],
        }, index=pd.to_datetime(["2024-01-02"]))
        mock_forex.history.return_value = mock_hist

        records = fetch_exchange_rates("EUR", "USD")

        assert len(records) == 1
        assert records[0][3] == 1.0  # currency_unit

    @patch("src.jobs.build_daily_currency_table.yf.Ticker")
    def test_handles_api_error(self, mock_ticker):
        """Test that API errors are handled gracefully."""
        mock_ticker.side_effect = Exception("API error")

        records = fetch_exchange_rates("USD", "JPY")

        assert len(records) == 0


class TestBuildDailyCurrencyTable:
    """Tests for build_daily_currency_table function."""

    @patch("src.jobs.build_daily_currency_table.fetch_exchange_rates")
    def test_builds_table_from_currency_pairs(self, mock_fetch, spark):
        """Test that table is built from currency pairs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            pairs_path = os.path.join(tmpdir, "currency_pairs_table")

            # Create currency pairs parquet
            pairs_df = spark.createDataFrame([
                ("USD", "JPY"),
            ], ["currency", "transaction_currency"])
            pairs_df.write.parquet(pairs_path)

            mock_fetch.return_value = [
                ("USD", "JPY", 20240102, 1.0, 150.0),
                ("USD", "JPY", 20240103, 1.0, 151.0),
            ]

            df = build_daily_currency_table(spark, pairs_path)
            result = df.collect()

            assert len(result) == 2
            mock_fetch.assert_called_once_with("USD", "JPY")

    @patch("src.jobs.build_daily_currency_table.fetch_exchange_rates")
    def test_builds_table_for_multiple_pairs(self, mock_fetch, spark):
        """Test that table is built for multiple currency pairs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            pairs_path = os.path.join(tmpdir, "currency_pairs_table")

            # Create currency pairs parquet
            pairs_df = spark.createDataFrame([
                ("USD", "JPY"),
                ("EUR", "USD"),
            ], ["currency", "transaction_currency"])
            pairs_df.write.parquet(pairs_path)

            mock_fetch.side_effect = [
                [("USD", "JPY", 20240102, 1.0, 150.0)],
                [("EUR", "USD", 20240102, 1.0, 1.10)],
            ]

            df = build_daily_currency_table(spark, pairs_path)
            result = df.collect()

            assert len(result) == 2
            assert mock_fetch.call_count == 2

    @patch("src.jobs.build_daily_currency_table.fetch_exchange_rates")
    def test_schema_has_correct_columns(self, mock_fetch, spark):
        """Test that schema has all required columns."""
        with tempfile.TemporaryDirectory() as tmpdir:
            pairs_path = os.path.join(tmpdir, "currency_pairs_table")

            pairs_df = spark.createDataFrame([
                ("USD", "JPY"),
            ], ["currency", "transaction_currency"])
            pairs_df.write.parquet(pairs_path)

            mock_fetch.return_value = [
                ("USD", "JPY", 20240102, 1.0, 150.0),
            ]

            df = build_daily_currency_table(spark, pairs_path)

            assert "currency" in df.columns
            assert "transaction_currency" in df.columns
            assert "date_key" in df.columns
            assert "currency_unit" in df.columns
            assert "transaction_currency_rate" in df.columns
