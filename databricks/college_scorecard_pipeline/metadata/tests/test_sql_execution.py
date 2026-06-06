from __future__ import annotations

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql

from column_sync import sql_execution
from column_sync.config import DatabricksConfig


class _FakeStatementExecution:
    def __init__(self, responses: list[sql.StatementResponse]) -> None:
        self._responses = responses
        self._calls = 0

    def execute_statement(self, statement: str, warehouse_id: str, disposition=None):
        return sql.StatementResponse(statement_id="stmt-1")

    def get_statement(self, statement_id: str):
        response = self._responses[min(self._calls, len(self._responses) - 1)]
        self._calls += 1
        return response


def _make_config(statement_execution: _FakeStatementExecution, monkeypatch) -> DatabricksConfig:
    client = WorkspaceClient(host="https://example", token="token")
    monkeypatch.setattr(client.statement_execution, "execute_statement", statement_execution.execute_statement)
    monkeypatch.setattr(client.statement_execution, "get_statement", statement_execution.get_statement)
    return DatabricksConfig(client=client, warehouse_id="test-warehouse")


def test_execute_statement_polls_until_success(monkeypatch) -> None:
    responses = [
        sql.StatementResponse(status=sql.StatementStatus(state=sql.StatementState.RUNNING)),
        sql.StatementResponse(status=sql.StatementStatus(state=sql.StatementState.SUCCEEDED)),
    ]
    config = _make_config(_FakeStatementExecution(responses), monkeypatch)

    monkeypatch.setattr(sql_execution.time, "sleep", lambda _s: None)

    payload = sql_execution.execute_statement(config, "SELECT 1", warehouse_id="test-warehouse")

    assert payload["status"]["state"] == "SUCCEEDED"


def test_execute_statement_raises_on_failure(monkeypatch) -> None:
    responses = [
        sql.StatementResponse(
            status=sql.StatementStatus(
                state=sql.StatementState.FAILED,
                error=sql.ServiceError(message="boom"),
            )
        )
    ]
    config = _make_config(_FakeStatementExecution(responses), monkeypatch)

    monkeypatch.setattr(sql_execution.time, "sleep", lambda _s: None)

    with pytest.raises(ValueError, match="boom"):
        sql_execution.execute_statement(config, "SELECT 1", warehouse_id="test-warehouse")
