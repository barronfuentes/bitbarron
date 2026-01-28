# PRD: Column Definition Automation (CLI + Databricks REST/dbutils)

## Overview
Build a CLI tool that reads the data dictionary from `college_scorecard_pipeline/metadata/data.yaml` and programmatically updates column descriptions (comments) in Databricks Unity Catalog via the Databricks REST API (preferred) or `dbutils` in a notebook context.

## Goals
- Keep Unity Catalog column descriptions in sync with the YAML data dictionary.
- Provide a repeatable, auditable, and idempotent CLI workflow.
- Support dry-run and selective updates to minimize risk.

## Non-Goals
- Automatic schema evolution or column creation.
- Updating table-level properties beyond column comments.
- Cross-workspace synchronization.

## Users & Use Cases
- Data engineers updating documentation when metadata changes.
- CI/CD pipeline step to enforce metadata parity.
- Quick one-off syncs during development.

## Assumptions
- Unity Catalog is enabled and the target tables exist.
- The YAML dictionary contains the source column name and description.
- Databricks CLI or PAT/OAuth credentials are available in the environment.

## Functional Requirements
- Parse `data.yaml` and map dictionary entries to target columns.
- Update column comments in Unity Catalog for a given catalog.schema.table.
- Support a dry-run mode showing intended changes without applying.
- Only update when the description differs (idempotent behavior).
- Log a summary of updates, skips, and errors.
- Allow filtering by table, column prefix, or explicit column list.

## CLI Interface (Proposed)
- Command: `column-sync`
- Example:
  - `column-sync --catalog main --schema default --table college_scorecard --yaml college_scorecard_pipeline/metadata/data.yaml --dry-run`

## API Integration
- Prefer Databricks REST API for table/column comments.
- Fallback to `dbutils`/`spark.sql` if REST is unavailable.

## Error Handling
- Clear errors for missing catalog/schema/table.
- Warn and skip dictionary entries not found in target table.
- Fail fast on authentication errors.

## Observability
- Structured logs (JSON) with counts and duration.
- Optional `--output` to write a report file.

## Security
- Use environment variables or Databricks CLI profile for tokens.
- Do not log secrets.

## Acceptance Criteria
- Running the CLI updates column descriptions in Unity Catalog.
- Dry-run produces a plan without side effects.
- Re-running with no changes results in zero updates.

## Fine-Grain Task Breakdown
1) Requirements & discovery
- Confirm target tables and Unity Catalog naming conventions.
- Document YAML schema used by `data.yaml`.

2) CLI scaffolding
- Create a CLI entrypoint and argument parser.
- Add config loading for Databricks host/auth.

3) YAML parsing
- Parse dictionary entries and normalize column names.
- Validate required fields (`source`, `description`).

4) Catalog metadata fetch
- Query current column comments from Unity Catalog.
- Build a comparison map of existing vs desired comments.

5) Update engine
- Implement REST call(s) to update column comments.
- Implement dry-run output.
- Implement idempotent update checks.

6) Filters & selection
- Add filters for table, column prefix, or explicit list.
- Ensure filters are applied before any updates.

7) Logging & reporting
- Print summary counts (updated/skipped/errors).
- Optional report file with per-column outcome.

8) Testing
- Unit tests for YAML parsing and diff logic.
- Mock API tests for update calls.

9) Documentation
- Update README with CLI usage, examples, and auth setup.

## Open Questions
- Which REST endpoint is preferred for column comments in current Databricks version?
- Should the CLI support multiple tables from a single YAML?
- What is the canonical mapping from dictionary key to column name when `source` differs?
