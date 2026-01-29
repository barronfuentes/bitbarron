"""Apply column comment updates based on comparison results."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from column_sync.catalog import ColumnCommentUpdate, update_column_comments
from column_sync.compare import CommentComparison, ComparisonResult
from column_sync.config import DatabricksConfig
from column_sync.warehouse import ensure_warehouse_id

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class UpdateSummary:
    """Summary of applied column updates."""

    attempted: int
    updated: int
    skipped_missing: int
    skipped_no_change: int
    missing_in_catalog: int
    errors: int


def build_comment_updates(
    comparisons: list[CommentComparison],
) -> tuple[list[ColumnCommentUpdate], list[str], int]:
    """Build update payloads and skip details from comparisons."""

    updates: list[ColumnCommentUpdate] = []
    missing_in_dictionary: list[str] = []
    no_change = 0
    for comparison in comparisons:
        if not comparison.desired_comment:
            missing_in_dictionary.append(comparison.column_name)
            continue
        if _normalize_comment(comparison.desired_comment) == _normalize_comment(comparison.existing_comment):
            no_change += 1
            continue
        updates.append(
            ColumnCommentUpdate(
                name=comparison.column_name,
                comment=comparison.desired_comment,
            )
        )

    return updates, missing_in_dictionary, no_change


def apply_comment_updates(
    config: DatabricksConfig,
    catalog: str,
    schema: str,
    table: str,
    result: ComparisonResult,
) -> UpdateSummary:
    """Apply column comment updates and return a summary."""

    updates, missing_in_dictionary, no_change = build_comment_updates(result.comparisons)
    logger.info(
        "Update plan: %s columns to update, %s columns not matched in dictionary, %s columns already match.",
        len(updates),
        len(missing_in_dictionary),
        no_change,
    )
    if missing_in_dictionary:
        if len(missing_in_dictionary) <= 10:
            logger.info(
                "Columns not matched in dictionary (%s): %s",
                len(missing_in_dictionary),
                ", ".join(sorted(missing_in_dictionary)),
            )
        else:
            logger.debug(
                "Columns not matched in dictionary (%s): %s",
                len(missing_in_dictionary),
                ", ".join(sorted(missing_in_dictionary)),
            )
    if result.missing_columns:
        logger.debug(
            "Dictionary entries missing from catalog: %s entries.",
            len(result.missing_columns),
        )
        missing_keys = ", ".join(entry.key for entry in result.missing_columns)
        logger.debug("Missing dictionary keys: %s", missing_keys)

    warehouse_id = None
    if updates:
        warehouse_id = ensure_warehouse_id(config)

    updated = 0
    errors = 0
    for update in updates:
        try:
            update_column_comments(
                config,
                catalog,
                schema,
                table,
                [update],
                warehouse_id=warehouse_id,
            )
            updated += 1
        except ValueError as exc:
            errors += 1
            logger.error(
                "Failed to update comment for %s.%s.%s.%s: %s",
                catalog,
                schema,
                table,
                update.name,
                exc,
            )

    if errors:
        logger.error("Update completed with %s errors; %s/%s columns updated.", errors, updated, len(updates))
    else:
        logger.info("Update completed: %s/%s columns updated.", updated, len(updates))

    return UpdateSummary(
        attempted=len(updates),
        updated=updated,
        skipped_missing=len(missing_in_dictionary),
        skipped_no_change=no_change,
        missing_in_catalog=len(result.missing_columns),
        errors=errors,
    )


def _normalize_comment(value: str | None) -> str | None:
    """Normalize comment strings for comparison."""

    if value is None:
        return None
    return str(value).strip() or None
