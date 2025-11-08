"""Tests for build_daily_table job."""
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.jobs.build_daily_table import fetch_daily_prices


class TestFetchDailyPrices:
    """Tests for fetch_daily_prices function."""

    @patch("src.jobs.build_daily_table.yf.Ticker")
    def test_returns_records_for_each_ticker(self, mock_ticker):
        # Mock yfinance response
        mock_hist = pd.DataFrame(
            {
                "Open": [150.0, 151.0],
                "High": [155.0, 156.0],
                "Low": [149.0, 150.0],
                "Close": [154.0, 155.0],
                "Volume": [1000000, 1100000],
            },
            index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
        )
        mock_ticker.return_value.history.return_value = mock_hist

        records = fetch_daily_prices(["AAPL"], date(2024, 1, 1), date(2024, 1, 5))

        assert len(records) == 2

    @patch("src.jobs.build_daily_table.yf.Ticker")
    def test_record_format(self, mock_ticker):
        mock_hist = pd.DataFrame(
            {
                "Open": [150.0],
                "High": [155.0],
                "Low": [149.0],
                "Close": [154.0],
                "Volume": [1000000],
            },
            index=pd.to_datetime(["2024-01-02"]),
        )
        mock_ticker.return_value.history.return_value = mock_hist

        records = fetch_daily_prices(["AAPL"], date(2024, 1, 1), date(2024, 1, 5))

        assert len(records) == 1
        ticker, exchange, date_key, open_price, high, low, close, volume = records[0]
        assert ticker == "AAPL"
        assert exchange == "NASDAQ"
        assert date_key == 20240102
        assert open_price == 150.0
        assert high == 155.0
        assert low == 149.0
        assert close == 154.0
        assert volume == 1000000

    @patch("src.jobs.build_daily_table.yf.Ticker")
    def test_handles_multiple_tickers(self, mock_ticker):
        mock_hist = pd.DataFrame(
            {
                "Open": [100.0],
                "High": [105.0],
                "Low": [99.0],
                "Close": [104.0],
                "Volume": [500000],
            },
            index=pd.to_datetime(["2024-01-02"]),
        )
        mock_ticker.return_value.history.return_value = mock_hist

        records = fetch_daily_prices(
            ["AAPL", "NVDA", "GME"], date(2024, 1, 1), date(2024, 1, 5)
        )

        assert len(records) == 3
        tickers = [r[0] for r in records]
        assert "AAPL" in tickers
        assert "NVDA" in tickers
        assert "GME" in tickers

    @patch("src.jobs.build_daily_table.yf.Ticker")
    def test_handles_empty_history(self, mock_ticker):
        mock_ticker.return_value.history.return_value = pd.DataFrame()

        records = fetch_daily_prices(["AAPL"], date(2024, 1, 1), date(2024, 1, 5))

        assert len(records) == 0
