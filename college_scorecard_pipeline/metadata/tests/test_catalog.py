from __future__ import annotations

from io import BytesIO
from unittest.mock import MagicMock, Mock, patch
from urllib.error import HTTPError, URLError

import pytest

from column_sync.catalog import (
    ColumnComment,
    extract_column_comments,
    fetch_column_comments,
    fetch_table_metadata,
)
from column_sync.config import DatabricksConfig


def _make_config() -> DatabricksConfig:
    return DatabricksConfig(
        host="https://example.databricks.com",
        token="token",
        profile=None,
    )


def _mock_response(payload: bytes) -> Mock:
    response = MagicMock()
    response.read.return_value = payload
    response.__enter__.return_value = response
    response.__exit__.return_value = None
    return response


def test_fetch_table_metadata_success() -> None:
    config = _make_config()
    payload = b'{"columns": [{"name": "UnitID", "comment": "Unit ID"}]}'

    with patch("column_sync.catalog.request.urlopen", return_value=_mock_response(payload)) as urlopen:
        data = fetch_table_metadata(config, "main", "default", "college_scorecard")

    assert data["columns"][0]["name"] == "UnitID"
    urlopen.assert_called_once()


def test_fetch_table_metadata_handles_http_error() -> None:
    config = _make_config()
    error = HTTPError(
        url="https://example.databricks.com/api/2.1/unity-catalog/tables/main.default.t",
        code=401,
        msg="Unauthorized",
        hdrs=None,
        fp=BytesIO(b"{}"),
    )

    with patch("column_sync.catalog.request.urlopen", side_effect=error):
        with pytest.raises(ValueError, match="Failed to fetch Unity Catalog table metadata"):
            fetch_table_metadata(config, "main", "default", "t")


def test_fetch_table_metadata_handles_url_error() -> None:
    config = _make_config()

    with patch("column_sync.catalog.request.urlopen", side_effect=URLError("boom")):
        with pytest.raises(ValueError, match="Unable to reach Databricks host"):
            fetch_table_metadata(config, "main", "default", "t")


def test_extract_column_comments_normalizes_and_cleans() -> None:
    payload = {
        "columns": [
            {"name": "UnitID", "comment": "Unit ID"},
            {"name": "NAME", "comment": ""},
            {"name": "  `foo`  ", "comment": None},
        ]
    }

    comments = extract_column_comments(payload)

    assert comments == [
        ColumnComment(name="UnitID", comment="Unit ID", normalized_name="unitid"),
        ColumnComment(name="NAME", comment=None, normalized_name="name"),
        ColumnComment(name="  `foo`  ", comment=None, normalized_name="foo"),
    ]


def test_extract_column_comments_requires_columns() -> None:
    with pytest.raises(ValueError, match="columns"):
        extract_column_comments({"name": "college_scorecard"})


def test_fetch_column_comments_passthrough() -> None:
    config = _make_config()
    payload = b'{"columns": [{"name": "UnitID", "comment": "Unit ID"}]}'

    with patch("column_sync.catalog.request.urlopen", return_value=_mock_response(payload)):
        comments = fetch_column_comments(config, "main", "default", "college_scorecard")

    assert len(comments) == 1
