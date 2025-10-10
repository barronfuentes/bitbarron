import duckdb

def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    minio_endpoint = "127.0.0.1:9000"
    minio_region = "us-east-1"
    minio_access_key = "minioadmin"
    minio_secret_key = "minioadmin"
    use_ssl = False

    conn.execute(f"SET s3_endpoint='{minio_endpoint}';")
    conn.execute(f"SET s3_region='{minio_region}';")
    conn.execute(f"SET s3_access_key_id='{minio_access_key}';")
    conn.execute(f"SET s3_secret_access_key='{minio_secret_key}';")
    conn.execute(f"SET s3_use_ssl={str(use_ssl).lower()};")
    conn.execute("SET s3_url_style = path;")
    return conn


def verify_connection(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS rest_api_data;")
    conn.execute("CREATE OR REPLACE TABLE rest_api_data.test AS SELECT 42 AS value;")
    conn.execute(
        "COPY rest_api_data.test TO 's3://4d-pipeline/rest_api_data/test.parquet' (FORMAT PARQUET);"
    )
    result = conn.execute(
        "SELECT * FROM read_parquet('s3://4d-pipeline/rest_api_data/*.parquet');"
    ).fetchall()
    assert result == [(42,)]
