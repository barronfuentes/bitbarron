from __future__ import annotations

from column_sync.catalog import ColumnComment
from column_sync.compare import build_comment_comparisons
from column_sync.dictionary import DictionaryEntry


def test_build_comment_comparisons_matches_columns() -> None:
    entries = [
        DictionaryEntry(
            key="school.unit_id",
            source="UNITID",
            description="Unit ID",
            normalized_source="unitid",
        )
    ]
    columns = [
        ColumnComment(
            name="UnitID",
            comment="Old comment",
            normalized_name="unitid",
        )
    ]

    result = build_comment_comparisons(entries, columns)

    assert result.missing_columns == []
    assert len(result.comparisons) == 1
    comparison = result.comparisons[0]
    assert comparison.column_name == "UnitID"
    assert comparison.existing_comment == "Old comment"
    assert comparison.desired_comment == "Unit ID"
    assert comparison.dictionary_key == "school.unit_id"


def test_build_comment_comparisons_reports_missing_dictionary_matches() -> None:
    entries = [
        DictionaryEntry(
            key="school.unit_id",
            source="UNITID",
            description="Unit ID",
            normalized_source="unitid",
        ),
        DictionaryEntry(
            key="school.new_col",
            source="NEW_COL",
            description="New column",
            normalized_source="new_col",
        ),
    ]
    columns = [
        ColumnComment(
            name="UnitID",
            comment="Unit ID",
            normalized_name="unitid",
        )
    ]

    result = build_comment_comparisons(entries, columns)

    assert len(result.missing_columns) == 1
    assert result.missing_columns[0].key == "school.new_col"
