"""Helpers for running SQL statements via Databricks Statement Execution API."""

from __future__ import annotations

import json
import logging
import time
from typing import Any
from urllib import request
from urllib.error import HTTPError, URLError

from column_sync.config import DatabricksConfig
from column_sync.warehouse import ensure_warehouse_id

logger = logging.getLogger(__name__)

TERMINAL_STATES = {"SUCCEEDED", "FAILED", "CANCELED"}


def execute_statement(
    config: DatabricksConfig,
    statement: str,
    *,
    timeout: int = 60,
    poll_interval: float = 1.0,
) -> dict[str, Any]:
    """Execute a SQL statement using Databricks Statement Execution API."""

    warehouse_id = ensure_warehouse_id(config, timeout=timeout)

    url = f"{config.host.rstrip('/')}/api/2.0/sql/statements"
    payload = json.dumps(
        {
            "statement": statement,
            "warehouse_id": warehouse_id,
            "disposition": "INLINE",
        }
    ).encode("utf-8")
    req = request.Request(url, data=payload, method="POST")
    req.add_header("Authorization", f"Bearer {config.token}")
    req.add_header("Accept", "application/json")
    req.add_header("Content-Type", "application/json")

    try:
        with request.urlopen(req, timeout=timeout) as response:
            data = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise ValueError(f"Failed to execute statement. Status {exc.code}.") from exc
    except URLError as exc:
        raise ValueError("Unable to reach Databricks host for statement execution.") from exc
    except json.JSONDecodeError as exc:
        raise ValueError("Databricks response for statement execution was not valid JSON.") from exc

    statement_id = data.get("statement_id")
    if not statement_id:
        raise ValueError("Databricks response missing statement_id.")

    return _wait_for_statement(config, statement_id, timeout=timeout, poll_interval=poll_interval)


def _wait_for_statement(
    config: DatabricksConfig,
    statement_id: str,
    *,
    timeout: int,
    poll_interval: float,
) -> dict[str, Any]:
    url = f"{config.host.rstrip('/')}/api/2.0/sql/statements/{statement_id}"
    req = request.Request(url)
    req.add_header("Authorization", f"Bearer {config.token}")
    req.add_header("Accept", "application/json")

    start = time.monotonic()
    while True:
        if time.monotonic() - start > timeout:
            raise ValueError(f"Statement execution timed out after {timeout} seconds.")

        try:
            with request.urlopen(req, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            raise ValueError(f"Failed to fetch statement status. Status {exc.code}.") from exc
        except URLError as exc:
            raise ValueError("Unable to reach Databricks host for statement status.") from exc
        except json.JSONDecodeError as exc:
            raise ValueError("Databricks response for statement status was not valid JSON.") from exc

        status = payload.get("status")
        if not isinstance(status, dict):
            raise ValueError("Databricks response missing status payload.")

        state = status.get("state")
        if not state:
            raise ValueError("Databricks response missing statement state.")

        if state in TERMINAL_STATES:
            if state != "SUCCEEDED":
                error = status.get("error") or {}
                message = error.get("message") or f"Statement failed with state {state}."
                raise ValueError(message)
            return payload

        time.sleep(poll_interval)
