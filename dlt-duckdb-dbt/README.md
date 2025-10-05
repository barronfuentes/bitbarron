# 4d Data Pipeline

* dlt - Reads data from sources and write to target
* duckdb - In-process SQL database for analytical queries on local data
* dbt - Framework for transforming data in duckdb for consumption

## Setup

### Install dependencies

```bash
brew install uv duckdb
uv venv
source .venv/bin/activate
uv sync
```

### Run dlt Pipeline

```bash
python rest_api_pipeline.py
```

### View dlt Dashboard

```bash
dlt pipeline rest_api_pokemon show
```
