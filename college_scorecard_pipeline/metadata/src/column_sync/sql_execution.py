"""Helpers for running SQL statements via Databricks Statement Execution API."""

from __future__ import annotations

import logging
import time
from typing import Any

from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.sql import Disposition, StatementState

from column_sync.config import DatabricksConfig
from column_sync.warehouse import ensure_warehouse_id

logger = logging.getLogger(__name__)

TERMINAL_STATES = {StatementState.SUCCEEDED, StatementState.FAILED, StatementState.CANCELED}
DEFAULT_TIMEOUT = 120


def execute_statement(
    config: DatabricksConfig,
    statement: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    poll_interval: float = 0.25,
    warehouse_id: str | None = None,
) -> dict[str, Any]:
    """Execute a SQL statement using Databricks Statement Execution API."""

    warehouse_id = warehouse_id or ensure_warehouse_id(config, timeout=timeout)

    try:
        response = config.client.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=warehouse_id,
            disposition=Disposition.INLINE,
        )
    except DatabricksError as exc:
        raise ValueError("Failed to execute statement.") from exc

    statement_id = response.statement_id
    if not statement_id:
        raise ValueError("Databricks response missing statement_id.")

    return _wait_for_statement(
        config,
        statement_id,
        timeout=timeout,
        poll_interval=poll_interval,
    )


def _wait_for_statement(
    config: DatabricksConfig,
    statement_id: str,
    *,
    timeout: int,
    poll_interval: float,
) -> dict[str, Any]:
    start = time.monotonic()
    while True:
        if time.monotonic() - start > timeout:
            raise ValueError(f"Statement execution timed out after {timeout} seconds.")

        try:
            payload = config.client.statement_execution.get_statement(statement_id)
        except DatabricksError as exc:
            raise ValueError("Failed to fetch statement status.") from exc

        status = payload.status
        if not status or not status.state:
            raise ValueError("Databricks response missing statement state.")

        if status.state in TERMINAL_STATES:
            if status.state != StatementState.SUCCEEDED:
                error_message = None
                if status.error and status.error.message:
                    error_message = status.error.message
                raise ValueError(error_message or f"Statement failed with state {status.state}.")
            return payload.as_dict()

        time.sleep(poll_interval)
