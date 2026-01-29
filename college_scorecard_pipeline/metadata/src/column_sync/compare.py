"""Comparison helpers for catalog comments vs. dictionary descriptions."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from column_sync.catalog import ColumnComment
from column_sync.dictionary import DictionaryEntry

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
