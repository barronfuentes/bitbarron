# 4d Data Pipeline

* dlt - Ingests data from sources and write to target
* duckdb - In-process SQL database for analytical queries on local data
* dbt - Framework for transforming data in duckdb for consumption

```mermaid
graph LR
    A[GitHub REST API] -->|dlt pipeline| B[(DuckDB Raw Data)]
    B -->|dbt transform| C[(DuckDB Models)]
    C -->|copy to| D[(S3 Storage)]
    
    style A fill:#e1f5fe
    style B fill:#e8f5e9
    style C fill:#e8f5e9
    style D fill:#ede7f6
```

## Setup

### Install dependencies

```bash
brew install uv duckdb
uv venv
source .venv/bin/activate
uv sync
```

### Start MinIO

```bash
docker run \
   -d \
   --name minio-dlt \
   -p 9000:9000 \
   -p 9001:9001 \
   -e "MINIO_ROOT_USER=minioadmin" \
   -e "MINIO_ROOT_PASSWORD=minioadmin" \
   minio/minio server /data --console-address ":9001"
docker exec minio-dlt mc mb data/4d-pipeline
```

### Run dlt Pipeline

```bash
export SOURCES__GITHUB__ACCESS_TOKEN="<YOUR_GITHUB_PAT>"
python rest_api_pipeline.py
```

### View dlt Dashboard

```bash
dlt pipeline rest_api_github show
```
