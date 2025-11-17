"""Tests for build_currency_pairs_table job."""
import tempfile
import os

from src.jobs.build_currency_pairs_table import build_currency_pairs_table


class TestBuildCurrencyPairsTable:
    """Tests for build_currency_pairs_table function."""

    def test_reads_currency_pairs_from_csv(self, spark):
        """Test that currency pairs are read from CSV."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "currency_pairs.csv")

            with open(csv_path, "w") as f:
                f.write("currency,transaction_currency\n")
                f.write("USD,JPY\n")

            df = build_currency_pairs_table(spark, csv_path)
            result = df.collect()

            assert len(result) == 1
            assert result[0].currency == "USD"
            assert result[0].transaction_currency == "JPY"

    def test_reads_multiple_pairs(self, spark):
        """Test that multiple currency pairs are read."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "currency_pairs.csv")

            with open(csv_path, "w") as f:
                f.write("currency,transaction_currency\n")
                f.write("USD,JPY\n")
                f.write("EUR,USD\n")
                f.write("GBP,EUR\n")

            df = build_currency_pairs_table(spark, csv_path)
            result = df.collect()

            assert len(result) == 3
            pairs = {(r.currency, r.transaction_currency) for r in result}
            assert ("USD", "JPY") in pairs
            assert ("EUR", "USD") in pairs
            assert ("GBP", "EUR") in pairs

    def test_schema_has_correct_columns(self, spark):
        """Test that schema has currency and transaction_currency columns."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "currency_pairs.csv")

            with open(csv_path, "w") as f:
                f.write("currency,transaction_currency\n")
                f.write("USD,JPY\n")

            df = build_currency_pairs_table(spark, csv_path)

            assert "currency" in df.columns
            assert "transaction_currency" in df.columns
            assert len(df.columns) == 2
