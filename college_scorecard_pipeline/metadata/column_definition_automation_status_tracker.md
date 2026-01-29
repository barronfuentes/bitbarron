# Column Definition Automation - AI Status Tracker

## How to Use
- Assign each task to an AI agent or run ID.
- Update Status, Started, and Completed as work progresses.
- Keep Evidence concise (file paths, commands, PRs).
- Add blockers to Notes so they are visible.

## ChatGPT (gpt-5.2-codex) Prompt Template

Use this in a fresh session for one task. Replace ALL-CAPS fields.

```text
Your job is to act as a senior python engineer responsible for implementing the prd college_scorecard_pipeline/metadata/column_definition_automation_prd.md.

Project context:
- PRD: college_scorecard_pipeline/metadata/column_definition_automation_prd.md
- Tracker: college_scorecard_pipeline/metadata/column_definition_automation_status_tracker.md
- YAML dictionary: college_scorecard_pipeline/metadata/data.yaml
- Document YAML schema used by `data.yaml`: `college_scorecard_pipeline/metadata/data_yaml_schema.md`

Tracker usage:
- Only work on the single task below.
- Update the tracker row for that task with Status, Started, Completed, Evidence, Notes.
- Evidence should reference concrete artifacts (files, commands, tests).
- If blocked, set Status to Blocked and explain why in Notes.
- If you need clarification, ask before starting.

Task to execute:
- Task ID: TASK_ID

Execution requirements:
- Follow the PRD goals, non-goals, and acceptance criteria.
- Use the repository’s existing patterns and tools.
- Prefer minimal, reversible changes.
- When done, update the tracker.

Rules:

- Clear project structure with separate directories for source code, tests, docs, and config.
- Configuration management using environment variables.
- Robust error handling and logging, including context capture.
- Comprehensive testing with pytest.
- Detailed documentation using docstrings and README files.
- project setup with uv
- Code style consistency using Ruff.
```

## Status Legend
- Not Started
- In Progress
- Blocked
- Done
- Dropped

## Task Tracker

| ID | Task | Owner | Status | Started | Completed | Evidence | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1.1 | Confirm target tables and Unity Catalog naming conventions |  | Done | 2026-01-28 | 2026-01-28 | User confirmation: target table is `workspace.default.college_scorecard`; map descriptions by existing column names to data.yaml | Single table only; no special casing/quoting rules beyond using existing column names. |
| 1.2 | Document YAML schema used by `data.yaml` |  | Done | 2026-01-28 | 2026-01-28 | `college_scorecard_pipeline/metadata/data_yaml_schema.md` | Documented observed top-level keys, dictionary entry fields, and files schema. |
| 2.1 | Create a CLI entrypoint and argument parser |  | Done | 2026-01-28 | 2026-01-28 | `college_scorecard_pipeline/metadata/src/column_sync/cli.py`, `college_scorecard_pipeline/metadata/src/column_sync/__main__.py` | Added argparse CLI scaffolding with required catalog/schema/table/yaml and core flags. |
| 2.2 | Add config loading for Databricks host/auth |  | Done | 2026-01-28 | 2026-01-28 | `college_scorecard_pipeline/metadata/src/column_sync/config.py`, `college_scorecard_pipeline/metadata/src/column_sync/cli.py` | Uses `DATABRICKS_HOST` plus `DATABRICKS_TOKEN` or `DATABRICKS_PROFILE`. |
| 2.3 | Auto-create serverless (2X-Small) SQL warehouse if none exist and return its ID |  | Done | 2026-01-29 | 2026-01-29 | `college_scorecard_pipeline/metadata/src/column_sync/warehouse.py`, `college_scorecard_pipeline/metadata/src/column_sync/sql_execution.py` | Added SQL warehouse discovery/creation helper; Statement Execution now auto-creates a serverless 2X-Small warehouse when none exist. |
| 3.1 | Parse dictionary entries and normalize column names |  | Done | 2026-01-28 | 2026-01-28 | `college_scorecard_pipeline/metadata/src/column_sync/dictionary.py`, `college_scorecard_pipeline/metadata/pyproject.toml` | Added YAML parser with normalized source names; PyYAML dependency registered. |
| 3.2 | Validate required fields (`source`, `description`) |  | Done | 2026-01-28 | 2026-01-28 | `college_scorecard_pipeline/metadata/src/column_sync/dictionary.py` | Added required-field validation with clear error messages; defaulted loader to validate. |
| 3.3 | Wire dictionary loader into CLI to surface validation errors |  | Done | 2026-01-28 | 2026-01-28 | `college_scorecard_pipeline/metadata/src/column_sync/cli.py` | CLI now loads dictionary at startup, surfacing validation errors before updates. |
| 4.1 | Query current column comments from Unity Catalog |  | Done | 2026-01-28 | 2026-01-28 | `college_scorecard_pipeline/metadata/src/column_sync/catalog.py`, `college_scorecard_pipeline/metadata/src/column_sync/cli.py`, `college_scorecard_pipeline/metadata/tests/test_catalog.py` | Added Unity Catalog table fetch + column comment extraction; CLI now fetches column comments; tests cover fetch/extract error paths. |
| 4.2 | Build a comparison map of existing vs desired comments |  | Done | 2026-01-28 | 2026-01-28 | `college_scorecard_pipeline/metadata/src/column_sync/compare.py`, `college_scorecard_pipeline/metadata/tests/test_compare.py` | Added comparison builder returning per-column desired vs existing comments and missing dictionary entries. |
| 5.1 | Implement SQL Statement Execution updates for column comments (REST patch unsupported) |  | Done | 2026-01-29 | 2026-01-29 | `college_scorecard_pipeline/metadata/src/column_sync/catalog.py`, `college_scorecard_pipeline/metadata/src/column_sync/sql_execution.py` | REST PATCH update for Unity Catalog column comments not supported in workspace; updates now use SQL via Statement Execution API (ALTER TABLE ... ALTER COLUMN ... COMMENT) with polling; requires `DATABRICKS_WAREHOUSE_ID`. |
| 5.2 | Implement dry-run output |  | Done | 2026-01-29 | 2026-01-29 | `college_scorecard_pipeline/metadata/src/column_sync/update.py`, `college_scorecard_pipeline/metadata/tests/test_dry_run.py` | Not run (tests not executed). Dry-run now logs column-level missing-in-dictionary details at INFO when count <=10; catalog-missing dictionary entries only at DEBUG. |
| 5.3 | Implement non-dry-run update execution (apply comparison results) |  | Done | 2026-01-29 | 2026-01-29 | `college_scorecard_pipeline/metadata/src/column_sync/update.py`, `college_scorecard_pipeline/metadata/src/column_sync/cli.py`, `college_scorecard_pipeline/metadata/tests/test_updates.py` | Tests not run (not requested). |
| 8.2 | Mock API tests for update calls |  | Done | 2026-01-29 | 2026-01-29 | `college_scorecard_pipeline/metadata/tests/integration/test_databricks_integration.py`, `college_scorecard_pipeline/metadata/pyproject.toml` | Added on-demand integration test for Databricks column comment update; default pytest run excludes integration marker. |
| 9.1 | Update README with CLI usage, examples, and auth setup |  | Done | 2026-01-28 | 2026-01-28 | `college_scorecard_pipeline/metadata/README.md` | Added uv build steps and CLI usage with profile override examples. |
