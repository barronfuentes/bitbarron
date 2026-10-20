from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
)

# JSON schema for Nest HVAC runtime data (44 fields from parsed JSON).
# The bronze table has these 44 data columns plus 3 metadata columns
# (_source_file, _device_id_from_filename, _ingest_timestamp) added by the pipeline.
NEST_HVAC_SCHEMA = StructType([
    StructField("airwave_time", LongType(), True),
    StructField("alt_heat1_time", LongType(), True),
    StructField("alt_heat2_time", LongType(), True),
    StructField("auto_away_time", LongType(), True),
    StructField("aux_heat_time", LongType(), True),
    StructField("away_cooling_temp", DoubleType(), True),
    StructField("away_heating_temp", DoubleType(), True),
    StructField("cool_capable", BooleanType(), True),
    StructField("cool_mode_time", LongType(), True),
    StructField("cool_stage1_time", LongType(), True),
    StructField("cool_stage2_time", LongType(), True),
    StructField("cool_stage3_time", LongType(), True),
    StructField("cooling_target", DoubleType(), True),
    StructField("cooling_time", LongType(), True),
    StructField("device_id", StringType(), True),
    StructField("emergency_heat_time", LongType(), True),
    StructField("end_time_local", StringType(), True),
    StructField("fan_time", LongType(), True),
    StructField("firmware_version", StringType(), True),
    StructField("has_ob_wire", BooleanType(), True),
    StructField("heat_capable", BooleanType(), True),
    StructField("heat_mode_time", LongType(), True),
    StructField("heat_stage1_time", LongType(), True),
    StructField("heat_stage2_time", LongType(), True),
    StructField("heat_stage3_time", LongType(), True),
    StructField("heatcool_mode_time", LongType(), True),
    StructField("heating_target", StringType(), True),
    StructField("heating_time", LongType(), True),
    StructField("indoor_humidity", DoubleType(), True),
    StructField("indoor_temp", DoubleType(), True),
    StructField("interval_end", StringType(), True),
    StructField("interval_start", StringType(), True),
    StructField("manual_away_time", LongType(), True),
    StructField("off_mode_time", LongType(), True),
    StructField("outdoor_humidity", LongType(), True),
    StructField("outdoor_temp", LongType(), True),
    StructField("postal_code", StringType(), True),
    StructField("sched_cooling_target", DoubleType(), True),
    StructField("sched_heating_target", StringType(), True),
    StructField("start_time_local", StringType(), True),
    StructField("structure_id", StringType(), True),
    StructField("time_zone", StringType(), True),
    StructField("weather_humidity", LongType(), True),
    StructField("weather_temp", DoubleType(), True),
])


@dp.table(
    name="bronze_nest_hvac_runtime",
    comment="Bronze layer: Nest thermostat HVAC runtime data from CSV-quoted JSONL files",
)
def bronze_nest_hvac_runtime():
    """Read CSV-quoted JSONL files from a UC volume, unescape CSV quoting,
    parse JSON into structured columns, and add provenance metadata.
    """
    volume_path = spark.conf.get("volume_path")

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("pathGlobFilter", "*.jsonl")
        .option("cloudFiles.includeExistingFiles", "true")
        .load(volume_path)
        # Unescape CSV quoting: strip outer wrapping quotes, replace doubled quotes
        .withColumn(
            "json_str",
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(F.col("value"), '^"', ''),
                    '"$', '',
                ),
                '""', '"',
            ),
        )
        # Parse the unescaped JSON string into a typed struct
        .withColumn("parsed", F.from_json(F.col("json_str"), NEST_HVAC_SCHEMA))
        # Select all parsed fields plus provenance metadata
        .select(
            F.col("parsed.*"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.regexp_extract(
                F.col("_metadata.file_path"),
                r"/(\w+)_\d{4}_\d{2}_HvacRuntime\.jsonl",
                1,
            ).alias("_device_id_from_filename"),
            F.current_timestamp().alias("_ingest_timestamp"),
        )
    )
