"""Tests for build_asset_table job."""
import os
import tempfile

import pytest

from src.jobs.build_asset_table import build_asset_table


class TestBuildAssetTable:
    """Tests for build_asset_table function."""

    @pytest.fixture
    def tickers_csv(self):
        """Create a temporary tickers CSV file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("ticker\n")
            f.write("AAPL\n")
            f.write("NVDA\n")
            f.write("GME\n")
            f.flush()
            yield f.name
        os.unlink(f.name)

    def test_returns_dataframe_with_correct_schema(self, spark, tickers_csv):
        df = build_asset_table(spark, tickers_csv)

        assert "asset_id" in df.columns
        assert "ticker" in df.columns
        assert "asset_class" in df.columns

    def test_loads_all_tickers(self, spark, tickers_csv):
        df = build_asset_table(spark, tickers_csv)

        assert df.count() == 3

    def test_tickers_are_correct(self, spark, tickers_csv):
        df = build_asset_table(spark, tickers_csv)
        tickers = [row.ticker for row in df.collect()]

        assert "AAPL" in tickers
        assert "NVDA" in tickers
        assert "GME" in tickers

    def test_asset_class_defaults_to_equity(self, spark, tickers_csv):
        df = build_asset_table(spark, tickers_csv)
        asset_classes = [row.asset_class for row in df.collect()]

        assert all(ac == "equity" for ac in asset_classes)

    def test_asset_ids_are_unique(self, spark, tickers_csv):
        df = build_asset_table(spark, tickers_csv)
        asset_ids = [row.asset_id for row in df.collect()]

        assert len(asset_ids) == len(set(asset_ids))
