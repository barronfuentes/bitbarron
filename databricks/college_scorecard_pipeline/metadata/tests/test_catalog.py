from __future__ import annotations

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.catalog import ColumnInfo, TableInfo

import pytest

from column_sync.catalog import ColumnComment, extract_column_comments, fetch_column_comments
from column_sync.config import DatabricksConfig


def _make_config() -> DatabricksConfig:
    return DatabricksConfig(
        client=WorkspaceClient(host="https://example.databricks.com", token="token"),
        profile=None,
    )


def test_extract_column_comments_normalizes_and_cleans() -> None:
    payload = [
        {"name": "UnitID", "comment": "Unit ID"},
        {"name": "NAME", "comment": ""},
        {"name": "  `foo`  ", "comment": None},
    ]

    comments = extract_column_comments(payload)

    assert comments == [
        ColumnComment(name="UnitID", comment="Unit ID", normalized_name="unitid"),
        ColumnComment(name="NAME", comment=None, normalized_name="name"),
        ColumnComment(name="  `foo`  ", comment=None, normalized_name="foo"),
    ]


def test_fetch_column_comments_passthrough() -> None:
    config = _make_config()
    table_info = TableInfo(
        columns=[ColumnInfo(name="UnitID", comment="Unit ID")],
        full_name="main.default.college_scorecard",
    )

    def fake_get(_full_name):
        return table_info

    config.client.tables.get = fake_get  # type: ignore[method-assign]
    comments = fetch_column_comments(config, "main", "default", "college_scorecard")

    assert len(comments) == 1


def test_fetch_column_comments_handles_error() -> None:
    config = _make_config()

    def fake_get(_full_name):
        raise DatabricksError("boom")

    config.client.tables.get = fake_get  # type: ignore[method-assign]
    with pytest.raises(ValueError, match="Failed to fetch Unity Catalog table metadata"):
        fetch_column_comments(config, "main", "default", "college_scorecard")
