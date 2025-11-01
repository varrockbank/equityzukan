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

# About the name

In Japanese, the Pokedex is called Pokemon Zukan, Zukan being encyclipedia. This project is like a Pokedex but for equities. 
Hence, Equity Zukan. It helps that the domain name was not taken. 



