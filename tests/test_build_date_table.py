"""Tests for build_date_table job."""
from datetime import date

import pytest

from src.jobs.build_date_table import (
    build_date_table,
    generate_date_records,
    get_nasdaq_trading_days,
)


class TestGetNasdaqTradingDays:
    """Tests for get_nasdaq_trading_days function."""

    def test_returns_set_of_dates(self):
        start = date(2024, 1, 1)
        end = date(2024, 1, 31)
        result = get_nasdaq_trading_days(start, end)

        assert isinstance(result, set)
        assert len(result) > 0

    def test_excludes_weekends(self):
        start = date(2024, 1, 1)
        end = date(2024, 1, 31)
        trading_days = get_nasdaq_trading_days(start, end)

        # Jan 6, 2024 is a Saturday
        assert date(2024, 1, 6) not in trading_days
        # Jan 7, 2024 is a Sunday
        assert date(2024, 1, 7) not in trading_days

    def test_excludes_holidays(self):
        start = date(2024, 1, 1)
        end = date(2024, 1, 31)
        trading_days = get_nasdaq_trading_days(start, end)

        # Jan 1, 2024 is New Year's Day
        assert date(2024, 1, 1) not in trading_days
        # Jan 15, 2024 is MLK Day
        assert date(2024, 1, 15) not in trading_days


class TestGenerateDateRecords:
    """Tests for generate_date_records function."""

    def test_generates_correct_date_range(self):
        start = date(2024, 1, 1)
        end = date(2024, 1, 5)
        trading_days = {date(2024, 1, 2), date(2024, 1, 3)}

        records = list(generate_date_records(start, end, trading_days))

        assert len(records) == 5
        assert records[0] == (20240101, False)
        assert records[1] == (20240102, True)
        assert records[2] == (20240103, True)
        assert records[3] == (20240104, False)
        assert records[4] == (20240105, False)

    def test_date_key_format(self):
        start = date(2024, 12, 31)
        end = date(2024, 12, 31)
        trading_days = set()

        records = list(generate_date_records(start, end, trading_days))

        assert records[0][0] == 20241231


class TestBuildDateTable:
    """Tests for build_date_table function."""

    def test_returns_dataframe_with_correct_schema(self, spark):
        df = build_date_table(spark)

        assert "date_key" in df.columns
        assert "nasdaq_open" in df.columns
        assert df.schema["date_key"].dataType.simpleString() == "int"
        assert df.schema["nasdaq_open"].dataType.simpleString() == "boolean"

    def test_dataframe_has_rows(self, spark):
        df = build_date_table(spark)

        assert df.count() > 0

    def test_has_both_open_and_closed_days(self, spark):
        df = build_date_table(spark)

        open_days = df.filter(df.nasdaq_open).count()
        closed_days = df.filter(~df.nasdaq_open).count()

        assert open_days > 0
        assert closed_days > 0
