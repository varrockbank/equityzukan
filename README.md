This project is a data warehouse centered on day-level equity data 
produced by a batch-processed Spark ELT pipeline collating various data sources. 

Current integrations:
- currency data from Yahoo Finance (will swap out later)
- OLHC data from Yahoo Finance
- shares outstanding from EDGAR 

# Requirements

Tech Stack (Subject to change):
- DB: Parquet (Later Postgres )
- ELT (extract-load-transform) big data pipeline: PySpark (Python-based Apache Spark)
- Workflow Orchestration: Apache Airflow

Scope will/may expand along various axis:
1. US equity coverage starting from Nasdaq to NYSE and beyond
2. resolution may be more granular then daily, although this DB format is not ideal for tick-by-tick data
3. Cryptocurrencies and other assets
4. European and Asia tickers
5. Historical timespan and trailing number of years of data
6. Currency Conversions 
7. Technical Indicators

We should be sufficiently busy striving for trailing-10-years of day-level data for Nasdaq and NYSE. Various complexities will crop up including handling stock-splits, delistings, new listings 

Out of scope:
1. tick-by-tick market data
2. Web UI. Use tool of choice to validate Parquet files. 

# Docker (Recommended)

```bash
docker compose up -d
```
Starts
- PostgreSQL (Airflow metadata database)
- Airflow UI at http://localhost:8080 (login: admin/admin)
- Airflow Scheduler

Note: New or modified DAGs may take ~30 seconds to appear in the UI as the scheduler periodically scans the `dags/` folder.

Stop:

```bash
docker compose down
```

To also remove volumes:
```bash
docker compose down -v
```

Rebuild After Changes:
```bash
docker compose build
docker compose up -d
```

# Native Setup (not recommended)

It is recommended you run with Docker-compose.

## Prerequisites
- Python 3.11+
- Java 17+ (required for PySpark)

## Installation

```bash
pip install -r requirements.txt
```

## Running Airflow

Initialize the database and create an admin user:
```bash
export AIRFLOW_HOME=$(pwd)
airflow db init
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

Start Airflow (standalone mode for development):
```bash
airflow standalone
```

Or run webserver and scheduler separately:
```bash
airflow webserver --port 8080 &
airflow scheduler &
```

Access the UI at http://localhost:8080 (login: admin/admin)

## Running PySpark

```bash
python src/app.py
```

# Testing

## Docker (Recommended)

Run all tests:
```bash
docker compose run --rm airflow-webserver python -m pytest -v
```

Run specific test file:
```bash
docker compose run --rm airflow-webserver python -m pytest tests/test_build_date_table.py -v
```

## Native

Run all tests:
```bash
pytest -v
```

Run specific test file:
```bash
pytest tests/test_build_date_table.py -v
```

Run with coverage:
```bash
pytest --cov=src --cov-report=term-missing
```

# About the name

In Japanese, the Pokedex is called Pokemon Zukan, Zukan being encyclipedia. This project is like a Pokedex but for equities.
Hence, Equity Zukan. It helps that the domain name was not taken. 



