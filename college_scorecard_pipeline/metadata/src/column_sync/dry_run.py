"""Dry-run planning helpers for column comment updates."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from column_sync.compare import CommentComparison, ComparisonResult, normalize_comment

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DryRunUpdate:
    """Planned column update for dry-run output."""

    column_name: str
    existing_comment: str | None
    desired_comment: str
    dictionary_key: str | None
    dictionary_source: str | None


def build_dry_run_updates(comparisons: list[CommentComparison]) -> list[DryRunUpdate]:
    """Build dry-run updates from comparisons."""

    updates: list[DryRunUpdate] = []
    for comparison in comparisons:
        if not comparison.desired_comment:
            continue
        if normalize_comment(comparison.desired_comment) == normalize_comment(comparison.existing_comment):
            continue
        updates.append(
            DryRunUpdate(
                column_name=comparison.column_name,
                existing_comment=comparison.existing_comment,
                desired_comment=comparison.desired_comment,
                dictionary_key=comparison.dictionary_key,
                dictionary_source=comparison.dictionary_source,
            )
        )
    return updates


def log_dry_run_plan(result: ComparisonResult) -> list[DryRunUpdate]:
    """Log the dry-run plan and return planned updates."""

    updates = build_dry_run_updates(result.comparisons)
    matched_missing = 0
    matched_no_change = 0
    missing_in_dictionary: list[str] = []
    for comparison in result.comparisons:
        if not comparison.desired_comment:
            matched_missing += 1
            missing_in_dictionary.append(comparison.column_name)
            continue
        if normalize_comment(comparison.desired_comment) == normalize_comment(comparison.existing_comment):
            matched_no_change += 1

    logger.info(
        "Dry-run summary: %s columns will be updated, %s columns not matched in dictionary, %s columns already match.",
        len(updates),
        matched_missing,
        matched_no_change,
    )
    if matched_missing:
        if matched_missing <= 10:
            logger.info(
                "Columns not matched in dictionary (%s): %s",
                matched_missing,
                ", ".join(sorted(missing_in_dictionary)),
            )
        else:
            logger.debug(
                "Columns not matched in dictionary (%s): %s",
                matched_missing,
                ", ".join(sorted(missing_in_dictionary)),
            )
    if logger.isEnabledFor(logging.DEBUG):
        for update in updates:
            logger.debug(
                "Dry-run update %s: existing=%r desired=%r dictionary_key=%s source=%s",
                update.column_name,
                update.existing_comment,
                update.desired_comment,
                update.dictionary_key,
                update.dictionary_source,
            )

    if result.missing_columns:
        logger.debug(
            "Dictionary entries missing from catalog: %s entries.",
            len(result.missing_columns),
        )
        missing_keys = ", ".join(entry.key for entry in result.missing_columns)
        logger.debug("Missing dictionary keys: %s", missing_keys)

    return updates
