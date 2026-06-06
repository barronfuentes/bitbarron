# data.yaml schema

This document describes the current schema used by `college_scorecard_pipeline/metadata/data.yaml` as observed in the file.

## Top-level keys

- `version` (string): version timestamp for the dictionary (example: `2025-10-06-17:21-0500`).
- `api` (string): API domain/collection name (example: `schools`).
- `index` (string): index name (example: `school-data`).
- `unique` (list[string]): list of unique identifiers for records (example: `["id"]`).
- `options` (map): configuration for the API/search layer.
  - `columns` (string): column selection mode (example: `all`).
  - `search` (string): search mode (example: `dictionary_only`).
- `null_value` (list[string]): sentinel values considered null (examples: `NULL`, `NA`, `PS`).
- `examples` (list[map]): sample API queries.
  - `name` (string)
  - `description` (string)
  - `params` (string)
- `dictionary` (map[string -> map]): data dictionary entries keyed by the field name.
- `files` (list[map]): source data file references by year and optional map.

## dictionary entry schema

Each `dictionary` value is a map with the following observed keys:

- `source` (string, required for column sync): source column name in the raw data.
- `description` (string, required for column sync): human-readable column description. Can be a multi-line block.
- `type` (string, optional): data type or semantic type (examples: `integer`, `float`, `autocomplete`).
- `index` (string, optional): index/storage hint (examples: `tinyint`, `varchar(50)`).
- `calculate` (string, optional): derivation expression (example: `C150_L4_POOLED_SUPP or C150_4_POOLED_SUPP`).
- `map` (string, optional): mapping group (examples: `program`).

Notes:
- Dictionary keys are dot-delimited field names (example: `school.name`).
- For the column comment sync, only `source` and `description` are required; other keys are treated as metadata.

## files entry schema

Each `files` item is a map with:

- `name` (string): file name (example: `MERGED2023_24_PP.csv`).
- `key` (string): logical year/version key (example: `2023`).
- `map` (string, optional): mapping group for field-of-study files (example: `program_data`).
