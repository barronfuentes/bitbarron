"""Unity Catalog metadata access helpers."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.catalog import ColumnInfo

from column_sync.config import DatabricksConfig
from column_sync.dictionary import normalize_column_name
from column_sync.sql_execution import execute_statement

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ColumnComment:
    """Column comment metadata fetched from Unity Catalog."""

    name: str
    comment: str | None
    normalized_name: str


@dataclass(frozen=True)
class ColumnCommentUpdate:
    """Column comment update payload."""

    name: str
    comment: str | None


def fetch_table_metadata(
    config: DatabricksConfig,
    catalog: str,
    schema: str,
    table: str,
    *,
    timeout: int = 10,
) -> dict[str, Any]:
    """Fetch table metadata from Unity Catalog."""

    full_name = f"{catalog}.{schema}.{table}"
    try:
        table_info = config.client.tables.get(full_name)
    except DatabricksError as exc:
        raise ValueError(f"Failed to fetch Unity Catalog table metadata for {full_name}.") from exc

    return table_info.as_dict()


def update_column_comments(
    config: DatabricksConfig,
    catalog: str,
    schema: str,
    table: str,
    updates: Iterable[ColumnCommentUpdate],
    *,
    timeout: int = 120,
    warehouse_id: str | None = None,
) -> None:
    """Update column comments for a Unity Catalog table."""

    updates_list = list(updates)
    if not updates_list:
        logger.info("No column comments to update for %s.%s.%s.", catalog, schema, table)
        return

    columns_payload = []
    for update in updates_list:
        name = update.name.strip() if isinstance(update.name, str) else ""
        if not name:
            raise ValueError("Column comment update requires a non-empty column name.")
        columns_payload.append({"name": update.name, "comment": update.comment})

    full_name = f"{catalog}.{schema}.{table}"
    table_ref = ".".join(_escape_identifier(part) for part in (catalog, schema, table))

    for update in columns_payload:
        column_name = _escape_identifier(update["name"])
        comment = update["comment"]
        if comment is None:
            comment_clause = "COMMENT NULL"
        else:
            comment_clause = f"COMMENT '{_escape_comment(comment)}'"
        statement = f"ALTER TABLE {table_ref} ALTER COLUMN {column_name} {comment_clause}"
        logger.info("Updating comment for %s.%s.%s.%s.", catalog, schema, table, update["name"])
        execute_statement(config, statement, timeout=timeout, warehouse_id=warehouse_id)


def _escape_identifier(identifier: str) -> str:
    """Escape a Databricks identifier with backticks."""

    return f"`{identifier.replace('`', '``')}`"


def _escape_comment(comment: str) -> str:
    """Escape single quotes in a comment string."""

    return comment.replace("'", "''")


def extract_column_comments(
    table_payload: Mapping[str, Any],
) -> list[ColumnComment]:
    """Extract column comments from a Unity Catalog table payload."""

    columns = table_payload.get("columns")
    if not isinstance(columns, list):
        raise ValueError("Databricks response missing 'columns' list.")

    results: list[ColumnComment] = []
    for column in columns:
        if isinstance(column, ColumnInfo):
            name = column.name
            comment = column.comment
        elif isinstance(column, dict):
            name = column.get("name")
            comment = column.get("comment")
        else:
            logger.warning("Skipping unexpected column payload: %r", column)
            continue

        if not isinstance(name, str) or not name.strip():
            logger.warning("Skipping column with invalid name: %r", column)
            continue

        if not isinstance(comment, str) or not comment.strip():
            comment = None

        results.append(
            ColumnComment(
                name=name,
                comment=comment,
                normalized_name=normalize_column_name(name),
            )
        )

    return results


def fetch_column_comments(
    config: DatabricksConfig,
    catalog: str,
    schema: str,
    table: str,
    *,
    timeout: int = 10,
) -> list[ColumnComment]:
    """Fetch column comments for a Unity Catalog table."""

    payload = fetch_table_metadata(
        config,
        catalog,
        schema,
        table,
        timeout=timeout,
    )
    columns = extract_column_comments(payload)
    logger.info("Fetched %s columns from %s.%s.%s.", len(columns), catalog, schema, table)
    return columns
