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
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
DEFAULT_TIMEOUT = 120
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_SECONDS = 1.0


def execute_statement(
    config: DatabricksConfig,
    statement: str,
    *,
    timeout: int = DEFAULT_TIMEOUT,
    poll_interval: float = 0.25,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_seconds: float = DEFAULT_BACKOFF_SECONDS,
    warehouse_id: str | None = None,
) -> dict[str, Any]:
    """Execute a SQL statement using Databricks Statement Execution API."""

    warehouse_id = warehouse_id or ensure_warehouse_id(config, timeout=timeout)

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
        data = _request_json_with_retries(
            req,
            timeout=timeout,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
            error_message="Failed to execute statement.",
        )
    except json.JSONDecodeError as exc:
        raise ValueError("Databricks response for statement execution was not valid JSON.") from exc

    statement_id = data.get("statement_id")
    if not statement_id:
        raise ValueError("Databricks response missing statement_id.")

    return _wait_for_statement(
        config,
        statement_id,
        timeout=timeout,
        poll_interval=poll_interval,
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
    )


def _wait_for_statement(
    config: DatabricksConfig,
    statement_id: str,
    *,
    timeout: int,
    poll_interval: float,
    max_retries: int,
    backoff_seconds: float,
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
            payload = _request_json_with_retries(
                req,
                timeout=timeout,
                max_retries=max_retries,
                backoff_seconds=backoff_seconds,
                error_message="Failed to fetch statement status.",
            )
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


def _request_json_with_retries(
    req: request.Request,
    *,
    timeout: int,
    max_retries: int,
    backoff_seconds: float,
    error_message: str,
) -> dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        if attempt:
            sleep_for = backoff_seconds * (2 ** (attempt - 1))
            logger.warning(
                "Retrying request after %.1fs (attempt %s/%s).",
                sleep_for,
                attempt,
                max_retries,
            )
            time.sleep(sleep_for)
        try:
            with request.urlopen(req, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            last_exc = exc
            if exc.code in RETRYABLE_STATUS_CODES and attempt < max_retries:
                logger.warning("%s Status %s.", error_message, exc.code)
                continue
            raise ValueError(f"{error_message} Status {exc.code}.") from exc
        except URLError as exc:
            last_exc = exc
            if attempt < max_retries:
                logger.warning("%s Network error: %s.", error_message, exc)
                continue
            raise ValueError(f"{error_message} Network error.") from exc

    if last_exc:
        raise ValueError(f"{error_message} Retry attempts exhausted.") from last_exc
    raise ValueError(f"{error_message} Retry attempts exhausted.")
