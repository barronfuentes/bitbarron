from __future__ import annotations

import os

import pytest

from column_sync.catalog import ColumnCommentUpdate, fetch_column_comments, update_column_comments
from column_sync.config import load_databricks_config, validate_databricks_credentials


@pytest.mark.integration
def test_update_column_comment_unitid() -> None:
    if os.getenv("RUN_DATABRICKS_INTEGRATION_TESTS") != "1":
        pytest.skip("Set RUN_DATABRICKS_INTEGRATION_TESTS=1 to run Databricks integration tests.")

    catalog = "workspace"
    schema = "default"
    table = "college_scorecard"
    target_normalized = "unitid"

    config = load_databricks_config()
    validate_databricks_credentials(config, catalog, schema, table)

    columns = fetch_column_comments(config, catalog, schema, table)
    target = next((col for col in columns if col.normalized_name == target_normalized), None)
    assert target is not None, "UNITID column not found in catalog table."

    original_comment = target.comment
    update = ColumnCommentUpdate(name=target.name, comment="test")

    try:
        update_column_comments(config, catalog, schema, table, [update])
        updated_columns = fetch_column_comments(config, catalog, schema, table)
        updated = next((col for col in updated_columns if col.normalized_name == target_normalized), None)
        assert updated is not None, "UNITID column missing after update."
        assert updated.comment == "test"
    finally:
        restore = ColumnCommentUpdate(name=target.name, comment=original_comment)
        update_column_comments(config, catalog, schema, table, [restore])
