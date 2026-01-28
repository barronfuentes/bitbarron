# Column Sync (Metadata)

CLI tool to sync column descriptions in Databricks Unity Catalog with the `data.yaml` dictionary.

## Build with uv

From `college_scorecard_pipeline/metadata`:

```bash
uv venv
uv pip install -e .
```

## Run the CLI

Example (uses the `DEFAULT` profile in `~/.databrickscfg`):

```bash
python -m column_sync \
  --catalog workspace \
  --schema default \
  --table college_scorecard \
  --yaml data.yaml
```

Override the profile:

```bash
python -m column_sync \
  --profile myprofile \
  --catalog workspace \
  --schema default \
  --table college_scorecard \
  --yaml data.yaml
```
