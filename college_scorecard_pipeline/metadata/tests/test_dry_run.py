from __future__ import annotations

import logging

from column_sync.update import CommentComparison, ComparisonResult
from column_sync.dictionary import DictionaryEntry
from column_sync.update import build_update_plan, log_dry_run_plan


def test_build_update_plan_filters_missing_desired_comment() -> None:
    comparisons = [
        CommentComparison(
            column_name="UNITID",
            normalized_name="unitid",
            existing_comment="Old",
            desired_comment="New",
            dictionary_key="school.unit_id",
            dictionary_source="UNITID",
        ),
        CommentComparison(
            column_name="OPEID",
            normalized_name="opeid",
            existing_comment=None,
            desired_comment=None,
            dictionary_key=None,
            dictionary_source=None,
        ),
    ]

    plan = build_update_plan(ComparisonResult(comparisons=comparisons, missing_columns=[]))

    assert len(plan.updates) == 1
    assert plan.updates[0].name == "UNITID"
    assert plan.updates[0].comment == "New"


def test_log_dry_run_plan_logs_updates_and_missing_columns(caplog) -> None:
    comparisons = [
        CommentComparison(
            column_name="UNITID",
            normalized_name="unitid",
            existing_comment="Old",
            desired_comment="New",
            dictionary_key="school.unit_id",
            dictionary_source="UNITID",
        )
    ]
    comparisons.append(
        CommentComparison(
            column_name="OPEID",
            normalized_name="opeid",
            existing_comment=None,
            desired_comment=None,
            dictionary_key=None,
            dictionary_source=None,
        )
    )
    missing = [
        DictionaryEntry(
            key="school.new_col",
            source="NEW_COL",
            description="New column",
            normalized_source="new_col",
        )
    ]
    result = ComparisonResult(comparisons=comparisons, missing_columns=missing)

    with caplog.at_level(logging.INFO):
        updates = log_dry_run_plan(result)

    assert len(updates) == 1
    assert updates[0].name == "UNITID"
    assert "Dry-run summary: 1 columns will be updated, 1 columns not matched in dictionary, 0 columns already match." in caplog.text
    assert "Columns not matched in dictionary (1): OPEID" in caplog.text
