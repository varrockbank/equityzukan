This project is a data warehouse centered on day-level equity data.

Tech Stack (Subject to change):
- DB Engine: Columnar Postgres SQL DB
- ELT (extract-load-transform) big data pipeline: PySpark (Python-based Apache Spark)
- Workflow Orchestration: Apache Airflow

Scope will/may expand along various axis:
1. US equity coverage starting from Nasdaq to NYSE and beyond
2. resolution may be more granular then daily, although this DB format is not ideal for tick-by-tick data
3. Cryptocurrencies and other assets
4. European and Asia tickers
5. Historical timespan and trailing number of years of data

We should be sufficiently busy striving for trailing-10-years of day-level data for Nasdaq and NYSE.
Various complexities will crop up including handling stock-splits, delistings, new listings 

Out of scope:
1. tick-by-tick market data
2. Web UI, except for debugging purposes

# Setup

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

# About the name

In Japanese, the Pokedex is called Pokemon Zukan, Zukan being encyclipedia. This project is like a Pokedex but for equities.
Hence, Equity Zukan. It helps that the domain name was not taken. 



