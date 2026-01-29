from __future__ import annotations

from column_sync.compare import CommentComparison, ComparisonResult
from databricks.sdk import WorkspaceClient

from column_sync.config import DatabricksConfig
from column_sync.dictionary import DictionaryEntry
from column_sync.update import apply_comment_updates, build_update_plan


def test_build_update_plan_filters_and_counts() -> None:
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
            existing_comment="Same",
            desired_comment=None,
            dictionary_key=None,
            dictionary_source=None,
        ),
        CommentComparison(
            column_name="INSTNM",
            normalized_name="instnm",
            existing_comment="Name",
            desired_comment="Name",
            dictionary_key="school.name",
            dictionary_source="INSTNM",
        ),
    ]

    plan = build_update_plan(ComparisonResult(comparisons=comparisons, missing_columns=[]))

    assert len(plan.updates) == 1
    assert plan.updates[0].name == "UNITID"
    assert plan.missing_in_dictionary == ["OPEID"]
    assert plan.no_change == 1


def test_apply_comment_updates_tracks_success_and_errors(monkeypatch) -> None:
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
            column_name="FAIL_COL",
            normalized_name="fail_col",
            existing_comment="Old",
            desired_comment="Newer",
            dictionary_key="school.fail_col",
            dictionary_source="FAIL_COL",
        ),
    ]
    result = ComparisonResult(
        comparisons=comparisons,
        missing_columns=[
            DictionaryEntry(
                key="school.missing",
                source="MISSING",
                description="Missing",
                normalized_source="missing",
            )
        ],
    )
    config = DatabricksConfig(
        client=WorkspaceClient(host="https://example", token="token"),
        warehouse_id="test-warehouse",
    )
    calls: list[str] = []

    def fake_update_column_comments(
        _config,
        _catalog,
        _schema,
        _table,
        updates,
        *,
        timeout=10,
        warehouse_id=None,
    ):
        name = updates[0].name
        calls.append(name)
        if name == "FAIL_COL":
            raise ValueError("boom")

    monkeypatch.setattr("column_sync.update.update_column_comments", fake_update_column_comments)

    summary = apply_comment_updates(config, "main", "default", "scorecard", result)

    assert calls == ["UNITID", "FAIL_COL"]
    assert summary.attempted == 2
    assert summary.updated == 1
    assert summary.errors == 1
    assert summary.missing_in_catalog == 1
