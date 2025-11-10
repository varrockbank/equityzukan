"""Tests for build_edgar_10q_table job."""
from unittest.mock import patch, MagicMock

import pytest

from src.jobs.build_edgar_10q_table import (
    get_cik_mapping,
    fetch_shares_outstanding,
    fetch_edgar_10q_data,
)


class TestGetCikMapping:
    """Tests for get_cik_mapping function."""

    @patch("src.jobs.build_edgar_10q_table.requests.get")
    def test_returns_ticker_to_cik_mapping(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
            "1": {"cik_str": 1045810, "ticker": "NVDA", "title": "NVIDIA CORP"},
        }
        mock_get.return_value = mock_response

        result = get_cik_mapping()

        assert result["AAPL"] == "0000320193"
        assert result["NVDA"] == "0001045810"

    @patch("src.jobs.build_edgar_10q_table.requests.get")
    def test_pads_cik_to_10_digits(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "0": {"cik_str": 123, "ticker": "TEST", "title": "Test Inc."},
        }
        mock_get.return_value = mock_response

        result = get_cik_mapping()

        assert result["TEST"] == "0000000123"


class TestFetchSharesOutstanding:
    """Tests for fetch_shares_outstanding function."""

    @patch("src.jobs.build_edgar_10q_table.requests.get")
    def test_extracts_shares_from_10q(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "facts": {
                "us-gaap": {
                    "CommonStockSharesOutstanding": {
                        "units": {
                            "shares": [
                                {
                                    "form": "10-Q",
                                    "end": "2024-03-31",
                                    "filed": "2024-05-15",
                                    "val": 15000000000,
                                },
                                {
                                    "form": "10-K",  # Should be ignored
                                    "end": "2024-01-15",
                                    "filed": "2024-01-15",
                                    "val": 14000000000,
                                },
                            ]
                        }
                    }
                }
            }
        }
        mock_get.return_value = mock_response

        records = fetch_shares_outstanding("0000320193", "AAPL")

        assert len(records) == 1
        assert records[0] == ("AAPL", "0000320193", 20240331, 15000000000)

    @patch("src.jobs.build_edgar_10q_table.requests.get")
    def test_deduplicates_by_period_end_date(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "facts": {
                "us-gaap": {
                    "CommonStockSharesOutstanding": {
                        "units": {
                            "shares": [
                                {
                                    "form": "10-Q",
                                    "end": "2024-03-31",
                                    "filed": "2024-05-01",
                                    "val": 15000000000,
                                },
                                {
                                    "form": "10-Q",
                                    "end": "2024-03-31",  # Same period, later filing
                                    "filed": "2024-05-15",
                                    "val": 15500000000,
                                },
                            ]
                        }
                    }
                }
            }
        }
        mock_get.return_value = mock_response

        records = fetch_shares_outstanding("0000320193", "AAPL")

        assert len(records) == 1
        # Should keep the later filing (15500000000)
        assert records[0] == ("AAPL", "0000320193", 20240331, 15500000000)

    @patch("src.jobs.build_edgar_10q_table.requests.get")
    def test_handles_missing_data(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {"facts": {}}
        mock_get.return_value = mock_response

        records = fetch_shares_outstanding("0000320193", "AAPL")

        assert len(records) == 0

    @patch("src.jobs.build_edgar_10q_table.requests.get")
    def test_handles_request_error(self, mock_get):
        mock_get.side_effect = Exception("Network error")

        records = fetch_shares_outstanding("0000320193", "AAPL")

        assert len(records) == 0


class TestFetchEdgar10qData:
    """Tests for fetch_edgar_10q_data function."""

    @patch("src.jobs.build_edgar_10q_table.fetch_shares_outstanding")
    @patch("src.jobs.build_edgar_10q_table.get_cik_mapping")
    @patch("src.jobs.build_edgar_10q_table.time.sleep")
    def test_fetches_data_for_all_tickers(self, mock_sleep, mock_cik, mock_fetch):
        mock_cik.return_value = {
            "AAPL": "0000320193",
            "NVDA": "0001045810",
        }
        mock_fetch.side_effect = [
            [("AAPL", "0000320193", 20240515, 15000000000)],
            [("NVDA", "0001045810", 20240515, 25000000000)],
        ]

        records = fetch_edgar_10q_data(["AAPL", "NVDA"])

        assert len(records) == 2

    @patch("src.jobs.build_edgar_10q_table.fetch_shares_outstanding")
    @patch("src.jobs.build_edgar_10q_table.get_cik_mapping")
    @patch("src.jobs.build_edgar_10q_table.time.sleep")
    def test_skips_unknown_tickers(self, mock_sleep, mock_cik, mock_fetch):
        mock_cik.return_value = {"AAPL": "0000320193"}
        mock_fetch.return_value = [("AAPL", "0000320193", 20240515, 15000000000)]

        records = fetch_edgar_10q_data(["AAPL", "UNKNOWN"])

        assert len(records) == 1
        assert records[0][0] == "AAPL"
        assert records[0][1] == "0000320193"
