"""Plan and apply column comment updates."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from column_sync.catalog import ColumnComment, ColumnCommentUpdate, update_column_comments
from column_sync.config import DatabricksConfig
from column_sync.dictionary import DictionaryEntry
from column_sync.sql_execution import ensure_warehouse_id

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CommentComparison:
    """Comparison of existing vs desired comments for a column."""

    column_name: str
    normalized_name: str
    existing_comment: str | None
    desired_comment: str | None
    dictionary_key: str | None
    dictionary_source: str | None


@dataclass(frozen=True)
class ComparisonResult:
    """Comparison results plus dictionary entries missing in the catalog."""

    comparisons: list[CommentComparison]
    missing_columns: list[DictionaryEntry]


@dataclass(frozen=True)
class UpdatePlan:
    """Planned updates and skip reasons derived from comparisons."""

    updates: list[ColumnCommentUpdate]
    missing_in_dictionary: list[str]
    no_change: int


@dataclass(frozen=True)
class UpdateSummary:
    """Summary of applied column updates."""

    attempted: int
    updated: int
    skipped_missing: int
    skipped_no_change: int
    missing_in_catalog: int
    errors: int


def normalize_comment(value: str | None) -> str | None:
    """Normalize comment strings for comparison."""

    if value is None:
        return None
    return str(value).strip() or None


def build_comment_comparisons(
    entries: list[DictionaryEntry],
    columns: list[ColumnComment],
) -> ComparisonResult:
    """Build comparisons between catalog comments and dictionary descriptions."""

    dictionary_map: dict[str, DictionaryEntry] = {}
    for entry in entries:
        if not entry.normalized_source:
            logger.debug("Skipping dictionary entry %s with missing normalized source.", entry.key)
            continue
        if entry.normalized_source in dictionary_map:
            logger.warning(
                "Duplicate dictionary source %r found for keys %s and %s.",
                entry.normalized_source,
                dictionary_map[entry.normalized_source].key,
                entry.key,
            )
            continue
        dictionary_map[entry.normalized_source] = entry

    column_map: dict[str, ColumnComment] = {}
    for column in columns:
        if column.normalized_name in column_map:
            logger.warning(
                "Duplicate catalog column %r found for %s and %s.",
                column.normalized_name,
                column_map[column.normalized_name].name,
                column.name,
            )
            continue
        column_map[column.normalized_name] = column

    comparisons: list[CommentComparison] = []
    for column in columns:
        entry = dictionary_map.get(column.normalized_name)
        comparisons.append(
            CommentComparison(
                column_name=column.name,
                normalized_name=column.normalized_name,
                existing_comment=column.comment,
                desired_comment=entry.description if entry else None,
                dictionary_key=entry.key if entry else None,
                dictionary_source=entry.source if entry else None,
            )
        )

    missing_columns = [
        entry
        for entry in entries
        if entry.normalized_source and entry.normalized_source not in column_map
    ]

    return ComparisonResult(comparisons=comparisons, missing_columns=missing_columns)


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

def _log_plan(plan: UpdatePlan, result: ComparisonResult, *, dry_run: bool) -> None:
    label = "Dry-run summary" if dry_run else "Update plan"
    action = "will be updated" if dry_run else "to update"
    logger.info(
        "%s: %s columns %s, %s columns not matched in dictionary, %s columns already match.",
        label,
        len(plan.updates),
        action,
        len(plan.missing_in_dictionary),
        plan.no_change,
    )
    if plan.missing_in_dictionary:
        log_fn = logger.info if len(plan.missing_in_dictionary) <= 10 else logger.debug
        log_fn(
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
                "%s update %s: existing=%r desired=%r dictionary_key=%s source=%s",
                "Dry-run" if dry_run else "Planned",
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


def log_dry_run_plan(result: ComparisonResult) -> list[ColumnCommentUpdate]:
    """Log the dry-run plan and return planned updates."""

    plan = build_update_plan(result)
    _log_plan(plan, result, dry_run=True)
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
    _log_plan(plan, result, dry_run=False)

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
