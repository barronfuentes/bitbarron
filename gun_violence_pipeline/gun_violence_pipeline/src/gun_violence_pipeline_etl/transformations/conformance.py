from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

SOURCE_STREAMING_TABLE = "workspace.default.gun_violence"

def _try_cast(col_name: str, spark_sql_type: str) -> F.Column:
    return F.expr(f"TRY_CAST({col_name} AS {spark_sql_type})")

def _parse_map_int_key(col_name: str, value_sql_type: str) -> F.Column:
    # Handles JSON maps OR "0::Unknown||1::Stolen" style. Never CASTs STRING->MAP.
    norm = f"regexp_replace(CAST({col_name} AS STRING), '\\\\s*\\\\|\\\\|\\\\s*', '||')"
    norm = f"regexp_replace({norm}, '\\\\s*\\\\|\\\\s*', '||')"

    json_map = f"from_json(CAST({col_name} AS STRING), 'MAP<STRING,{value_sql_type}>')"
    json_to_int_keys = f"transform_keys({json_map}, (k, v) -> TRY_CAST(k AS INT))"

    parts = f"split({norm}, '\\\\|\\\\|')"
    key_expr = "TRY_CAST(regexp_extract(x, '^\\\\s*(\\\\d+)::', 1) AS INT)"
    if value_sql_type.upper() == "INT":
        val_expr = "TRY_CAST(regexp_replace(x, '^\\\\s*\\\\d+::', '') AS INT)"
    else:
        val_expr = "CAST(regexp_replace(x, '^\\\\s*\\\\d+::', '') AS STRING)"

    kv_entries = (
        "map_from_entries("
        "  transform("
        f"    filter({parts}, x -> x RLIKE '^\\\\s*\\\\d+::' AND {key_expr} IS NOT NULL),"
        f"    x -> struct({key_expr}, {val_expr})"
        "  )"
        ")"
    )

    return F.expr(
        "CASE "
        f"WHEN {col_name} IS NULL OR TRIM(CAST({col_name} AS STRING)) = '' OR lower(TRIM(CAST({col_name} AS STRING))) = 'null' THEN NULL "
        f"WHEN TRIM(CAST({col_name} AS STRING)) RLIKE '^\\\\s*\\\\{{' THEN {json_to_int_keys} "
        f"WHEN CAST({col_name} AS STRING) RLIKE '^\\\\s*\\\\d+::' THEN {kv_entries} "
        "ELSE NULL END"
    )

def _parse_array_str(col_name: str) -> F.Column:
    json_arr = f"from_json(CAST({col_name} AS STRING), 'ARRAY<STRING>')"
    split_pipe = f"split(CAST({col_name} AS STRING), '\\\\s*\\\\|\\\\|\\\\s*')"
    split_semicolon = f"split(CAST({col_name} AS STRING), '\\\\s*;\\\\s*')"
    return F.expr(
        "CASE "
        f"WHEN {col_name} IS NULL OR TRIM(CAST({col_name} AS STRING)) = '' OR lower(TRIM(CAST({col_name} AS STRING))) = 'null' THEN NULL "
        f"WHEN TRIM(CAST({col_name} AS STRING)) RLIKE '^\\\\s*\\\\[' THEN {json_arr} "
        f"WHEN CAST({col_name} AS STRING) LIKE '%||%' THEN {split_pipe} "
        f"WHEN CAST({col_name} AS STRING) LIKE '%;%' THEN {split_semicolon} "
        f"ELSE array(CAST({col_name} AS STRING)) END"
    )

# These show up in the PIPELINE UI (Data quality), not in the table schema.
EXPECTATIONS = {
    # Required fields (strings must be non-empty)
    "incident_id_required": "incident_id IS NOT NULL",
    "date_required": "date IS NOT NULL AND TRIM(date) <> ''",
    "state_required": "state IS NOT NULL AND TRIM(state) <> ''",
    "city_or_county_required": "city_or_county IS NOT NULL AND TRIM(city_or_county) <> ''",
    "address_required": "address IS NOT NULL AND TRIM(address) <> ''",
    "n_killed_required": "n_killed IS NOT NULL",
    "n_injured_required": "n_injured IS NOT NULL",
    "incident_url_required": "incident_url IS NOT NULL AND TRIM(incident_url) <> ''",
    "incident_url_fields_missing_required": "incident_url_fields_missing IS NOT NULL",

    # Type/domain sanity
    "incident_id_positive": "incident_id IS NULL OR incident_id > 0",
    "n_killed_nonneg": "n_killed IS NULL OR n_killed >= 0",
    "n_injured_nonneg": "n_injured IS NULL OR n_injured >= 0",
    "n_guns_involved_nonneg": "n_guns_involved IS NULL OR n_guns_involved >= 0",
    "incident_url_http": "incident_url IS NULL OR incident_url RLIKE '^https?://'",
    "source_url_http": "source_url IS NULL OR TRIM(source_url) = '' OR source_url RLIKE '^https?://'",
    "incident_url_fields_missing_false": "incident_url_fields_missing = FALSE",
    "latitude_bounds": "latitude IS NULL OR (latitude >= -90.0 AND latitude <= 90.0)",
    "longitude_bounds": "longitude IS NULL OR (longitude >= -180.0 AND longitude <= 180.0)",
    "date_parseable": "date_parsed IS NOT NULL",

    # Parseability: if raw provided, parsed must not be null
    "gun_stolen_parseable_if_provided": "gun_stolen_raw IS NULL OR TRIM(gun_stolen_raw) = '' OR lower(TRIM(gun_stolen_raw)) = 'null' OR gun_stolen IS NOT NULL",
    "gun_type_parseable_if_provided": "gun_type_raw IS NULL OR TRIM(gun_type_raw) = '' OR lower(TRIM(gun_type_raw)) = 'null' OR gun_type IS NOT NULL",
    "incident_characteristics_parseable_if_provided": "incident_characteristics_raw IS NULL OR TRIM(incident_characteristics_raw) = '' OR lower(TRIM(incident_characteristics_raw)) = 'null' OR incident_characteristics IS NOT NULL",
    "sources_parseable_if_provided": "sources_raw IS NULL OR TRIM(sources_raw) = '' OR lower(TRIM(sources_raw)) = 'null' OR sources IS NOT NULL",
}

@dp.view
def raw_csv():
    return spark.readStream.table(SOURCE_STREAMING_TABLE)

@dp.table(
    name="staged_quarantine",
    partition_cols=["is_quarantined"],
    table_properties={"quality": "bronze"}
)
@dp.expect_all(EXPECTATIONS)
def staged_quarantine():
    raw = dp.read_stream("raw_csv")

    # Keep raw copies for parseability expectations
    df = (raw
          .withColumn("gun_stolen_raw", F.col("gun_stolen").cast("string"))
          .withColumn("gun_type_raw", F.col("gun_type").cast("string"))
          .withColumn("incident_characteristics_raw", F.col("incident_characteristics").cast("string"))
          .withColumn("sources_raw", F.col("sources").cast("string"))
    )

    # Canonical primitive casting
    df = (df
          .withColumn("incident_id", _try_cast("incident_id", "INT"))
          .withColumn("n_killed", _try_cast("n_killed", "INT"))
          .withColumn("n_injured", _try_cast("n_injured", "INT"))
          .withColumn("congressional_district", _try_cast("congressional_district", "INT"))
          .withColumn("state_house_district", _try_cast("state_house_district", "INT"))
          .withColumn("state_senate_district", _try_cast("state_senate_district", "INT"))
          .withColumn("n_guns_involved", _try_cast("n_guns_involved", "INT"))
          .withColumn("latitude", _try_cast("latitude", "FLOAT"))
          .withColumn("longitude", _try_cast("longitude", "FLOAT"))
          .withColumn("incident_url_fields_missing", _try_cast("incident_url_fields_missing", "BOOLEAN"))
          .withColumn("date", F.col("date").cast("string"))
          .withColumn("state", F.col("state").cast("string"))
          .withColumn("city_or_county", F.col("city_or_county").cast("string"))
          .withColumn("address", F.col("address").cast("string"))
          .withColumn("incident_url", F.col("incident_url").cast("string"))
          .withColumn("source_url", F.col("source_url").cast("string"))
    )

    # Parsed date (required)
    df = df.withColumn(
        "date_parsed",
        F.coalesce(
            F.to_date(F.col("date"), "yyyy-MM-dd"),
            F.to_date(F.col("date"), "MM/dd/yyyy"),
            F.to_date(F.col("date"), "M/d/yyyy"),
        ).cast(DateType())
    )

    # Complex parsing (no STRING->MAP casts)
    df = df.withColumn("gun_stolen", _parse_map_int_key("gun_stolen_raw", "STRING"))
    df = df.withColumn("gun_type", _parse_map_int_key("gun_type_raw", "STRING"))
    df = df.withColumn("incident_characteristics", _parse_array_str("incident_characteristics_raw"))
    df = df.withColumn("sources", _parse_array_str("sources_raw"))

    # Quarantine logic (consistent with expectations)
    # Note: this flags a row as quarantined if ANY expectation fails.
    # We compute it explicitly because expectations alone don't route rows.
    expectation_expr = " AND ".join([f"({sql})" for sql in EXPECTATIONS.values()])
    df = df.withColumn("is_quarantined", F.expr(f"NOT({expectation_expr})"))

    return df

@dp.table(name="gun_violence_clean", table_properties={"quality": "silver"})
def gun_violence_clean():
    return dp.read("staged_quarantine").filter("is_quarantined = false")

@dp.table(name="gun_violence_quarantine", table_properties={"quality": "quarantine"})
def gun_violence_quarantine():
    return dp.read("staged_quarantine").filter("is_quarantined = true")
