"""Apply column comment updates based on comparison results."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from column_sync.catalog import ColumnCommentUpdate, update_column_comments
from column_sync.compare import ComparisonResult, normalize_comment
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


@dataclass(frozen=True)
class UpdatePlan:
    """Planned updates and skip reasons derived from comparisons."""

    updates: list[ColumnCommentUpdate]
    missing_in_dictionary: list[str]
    no_change: int


def build_update_plan(result: ComparisonResult) -> UpdatePlan:
    """Build update payloads and skip details from comparisons."""

    updates: list[ColumnCommentUpdate] = []
    missing_in_dictionary: list[str] = []
    no_change = 0
    for comparison in result.comparisons:
        if not comparison.desired_comment:
            missing_in_dictionary.append(comparison.column_name)
            continue
        if normalize_comment(comparison.desired_comment) == normalize_comment(comparison.existing_comment):
            no_change += 1
            continue
        updates.append(
            ColumnCommentUpdate(
                name=comparison.column_name,
                comment=comparison.desired_comment,
            )
        )

    return UpdatePlan(
        updates=updates,
        missing_in_dictionary=missing_in_dictionary,
        no_change=no_change,
    )


def log_dry_run_plan(result: ComparisonResult) -> list[ColumnCommentUpdate]:
    """Log the dry-run plan and return planned updates."""

    plan = build_update_plan(result)
    logger.info(
        "Dry-run summary: %s columns will be updated, %s columns not matched in dictionary, %s columns already match.",
        len(plan.updates),
        len(plan.missing_in_dictionary),
        plan.no_change,
    )
    if plan.missing_in_dictionary:
        if len(plan.missing_in_dictionary) <= 10:
            logger.info(
                "Columns not matched in dictionary (%s): %s",
                len(plan.missing_in_dictionary),
                ", ".join(sorted(plan.missing_in_dictionary)),
            )
        else:
            logger.debug(
                "Columns not matched in dictionary (%s): %s",
                len(plan.missing_in_dictionary),
                ", ".join(sorted(plan.missing_in_dictionary)),
            )
    if logger.isEnabledFor(logging.DEBUG):
        for comparison in result.comparisons:
            if not comparison.desired_comment:
                continue
            if normalize_comment(comparison.desired_comment) == normalize_comment(comparison.existing_comment):
                continue
            logger.debug(
                "Dry-run update %s: existing=%r desired=%r dictionary_key=%s source=%s",
                comparison.column_name,
                comparison.existing_comment,
                comparison.desired_comment,
                comparison.dictionary_key,
                comparison.dictionary_source,
            )

    if result.missing_columns:
        logger.debug(
            "Dictionary entries missing from catalog: %s entries.",
            len(result.missing_columns),
        )
        missing_keys = ", ".join(entry.key for entry in result.missing_columns)
        logger.debug("Missing dictionary keys: %s", missing_keys)

    return plan.updates


def apply_comment_updates(
    config: DatabricksConfig,
    catalog: str,
    schema: str,
    table: str,
    result: ComparisonResult,
) -> UpdateSummary:
    """Apply column comment updates and return a summary."""

    plan = build_update_plan(result)
    logger.info(
        "Update plan: %s columns to update, %s columns not matched in dictionary, %s columns already match.",
        len(plan.updates),
        len(plan.missing_in_dictionary),
        plan.no_change,
    )
    if plan.missing_in_dictionary:
        if len(plan.missing_in_dictionary) <= 10:
            logger.info(
                "Columns not matched in dictionary (%s): %s",
                len(plan.missing_in_dictionary),
                ", ".join(sorted(plan.missing_in_dictionary)),
            )
        else:
            logger.debug(
                "Columns not matched in dictionary (%s): %s",
                len(plan.missing_in_dictionary),
                ", ".join(sorted(plan.missing_in_dictionary)),
            )
    if result.missing_columns:
        logger.debug(
            "Dictionary entries missing from catalog: %s entries.",
            len(result.missing_columns),
        )
        missing_keys = ", ".join(entry.key for entry in result.missing_columns)
        logger.debug("Missing dictionary keys: %s", missing_keys)

    warehouse_id = config.warehouse_id
    if plan.updates and not warehouse_id:
        warehouse_id = ensure_warehouse_id(config)

    updated = 0
    errors = 0
    for update in plan.updates:
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
        logger.error(
            "Update completed with %s errors; %s/%s columns updated.",
            errors,
            updated,
            len(plan.updates),
        )
    else:
        logger.info("Update completed: %s/%s columns updated.", updated, len(plan.updates))

    return UpdateSummary(
        attempted=len(plan.updates),
        updated=updated,
        skipped_missing=len(plan.missing_in_dictionary),
        skipped_no_change=plan.no_change,
        missing_in_catalog=len(result.missing_columns),
        errors=errors,
    )
