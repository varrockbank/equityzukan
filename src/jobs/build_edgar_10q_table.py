"""Build EDGAR 10-Q table with shares outstanding data."""
import time

import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from config.settings import END_DATE, START_DATE

# SEC requires a user agent header
SEC_HEADERS = {
    "User-Agent": "EquityZukan research@equityzukan.com",
    "Accept-Encoding": "gzip, deflate",
}

# Rate limit: SEC allows 10 requests per second
SEC_RATE_LIMIT_DELAY = 0.1


def get_cik_mapping() -> dict[str, str]:
    """Fetch ticker to CIK mapping from SEC."""
    url = "https://www.sec.gov/files/company_tickers.json"
    response = requests.get(url, headers=SEC_HEADERS)
    response.raise_for_status()

    data = response.json()
    # Map ticker -> CIK (zero-padded to 10 digits)
    return {
        item["ticker"]: str(item["cik_str"]).zfill(10)
        for item in data.values()
    }


def fetch_shares_outstanding(cik: str, ticker: str) -> list[tuple]:
    """Fetch shares outstanding from 10-Q filings for a company."""
    # Use dict to deduplicate by (period_end_date) - keep latest filed
    records_by_period = {}

    # Fetch company facts from SEC EDGAR API
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

    try:
        response = requests.get(url, headers=SEC_HEADERS)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {ticker} (CIK: {cik}): {e}")
        return []

    # Look for shares outstanding in US-GAAP taxonomy
    us_gaap = data.get("facts", {}).get("us-gaap", {})

    # Try multiple field names (companies use different ones)
    shares_fields = [
        "CommonStockSharesOutstanding",
        "CommonStockSharesIssued",
        "SharesOutstanding",
        "WeightedAverageNumberOfSharesOutstandingBasic",
    ]

    shares_data = []
    # First pass: prefer fields with 10-Q data (more granular quarterly data)
    for field in shares_fields:
        if field in us_gaap:
            units = us_gaap[field].get("units", {})
            candidate_data = units.get("shares", [])
            has_10q = any(entry.get("form") == "10-Q" for entry in candidate_data)
            if has_10q:
                shares_data = candidate_data
                print(f"  Using field: {field}")
                break

    # Second pass: fall back to fields with only 10-K data
    if not shares_data:
        for field in shares_fields:
            if field in us_gaap:
                units = us_gaap[field].get("units", {})
                candidate_data = units.get("shares", [])
                has_10k = any(entry.get("form") == "10-K" for entry in candidate_data)
                if has_10k:
                    shares_data = candidate_data
                    print(f"  Using field (10-K fallback): {field}")
                    break

    if not shares_data:
        print(f"Warning: No quarterly shares data found for {ticker}")
        return []

    start_key = int(START_DATE.strftime("%Y%m%d"))
    end_key = int(END_DATE.strftime("%Y%m%d"))

    # Track the latest filing before START_DATE to anchor early dates
    latest_before_start = None  # (date_key, shares, filed_key)

    for entry in shares_data:
        # Include both 10-Q and 10-K filings
        if entry.get("form") not in ("10-Q", "10-K"):
            continue

        # Use period end date (not filing date) as the date_key
        end_date = entry.get("end", "")
        filed_date = entry.get("filed", "")
        if not end_date:
            continue

        # Convert end date to date_key format (YYYYMMDD)
        try:
            date_key = int(end_date.replace("-", ""))
            filed_key = int(filed_date.replace("-", "")) if filed_date else 0
        except ValueError:
            continue

        shares = entry.get("val")
        if shares is None:
            continue

        # Check if this is within our date range
        if start_key <= date_key <= end_key:
            # Keep the record with the latest filing date for each period end date
            if date_key not in records_by_period or filed_key > records_by_period[date_key][1]:
                records_by_period[date_key] = (int(shares), filed_key)
        elif date_key < start_key:
            # Track the latest filing before START_DATE to anchor early dates
            if latest_before_start is None or date_key > latest_before_start[0]:
                latest_before_start = (date_key, int(shares), filed_key)
            elif date_key == latest_before_start[0] and filed_key > latest_before_start[2]:
                # Same period, but later filing - update
                latest_before_start = (date_key, int(shares), filed_key)

    # Include the pre-start anchor if found
    if latest_before_start:
        date_key, shares, _ = latest_before_start
        records_by_period[date_key] = (shares, 0)

    # Convert to list of tuples
    return [
        (ticker, cik, date_key, shares_filed[0])
        for date_key, shares_filed in sorted(records_by_period.items())
    ]


def fetch_edgar_10q_data(tickers: list[str]) -> list[tuple]:
    """Fetch 10-Q shares outstanding data for all tickers."""
    print("Fetching CIK mapping from SEC...")
    cik_mapping = get_cik_mapping()
    time.sleep(SEC_RATE_LIMIT_DELAY)

    all_records = []

    for ticker in tickers:
        cik = cik_mapping.get(ticker)
        if not cik:
            print(f"Warning: No CIK found for {ticker}")
            continue

        print(f"Fetching 10-Q data for {ticker} (CIK: {cik})...")
        records = fetch_shares_outstanding(cik, ticker)
        all_records.extend(records)

        # Respect SEC rate limit
        time.sleep(SEC_RATE_LIMIT_DELAY)

    return all_records


def build_edgar_10q_table(spark: SparkSession, tickers_path: str):
    """Build and return EDGAR 10-Q table DataFrame."""
    # Read tickers from CSV
    tickers_df = spark.read.csv(tickers_path, header=True)
    tickers = [row.ticker for row in tickers_df.collect()]

    # Fetch EDGAR data
    records = fetch_edgar_10q_data(tickers)

    if not records:
        print("Warning: No 10-Q records found")

    schema = StructType([
        StructField("ticker", StringType(), nullable=False),
        StructField("cik", StringType(), nullable=False),
        StructField("date_key", IntegerType(), nullable=False),
        StructField("shares_outstanding", LongType(), nullable=False),
    ])

    return spark.createDataFrame(records, schema)


def main():
    """Main entry point."""
    spark = (
        SparkSession.builder
        .appName("BuildEdgar10QTable")
        .getOrCreate()
    )

    try:
        df = build_edgar_10q_table(spark, "/opt/airflow/data/tickers.csv")
        df.show(20)
        print(f"Total rows: {df.count()}")

        df.write.mode("overwrite").parquet("/opt/airflow/data/edgar_10q_table")
        print("Saved to /opt/airflow/data/edgar_10q_table")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
