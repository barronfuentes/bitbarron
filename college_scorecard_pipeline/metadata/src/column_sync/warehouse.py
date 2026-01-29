"""Helpers for discovering or creating Databricks SQL warehouses."""

from __future__ import annotations

import json
import logging
from typing import Any
from urllib import request
from urllib.error import HTTPError, URLError

from column_sync.config import DatabricksConfig

logger = logging.getLogger(__name__)

DEFAULT_WAREHOUSE_NAME = "column-sync-auto"


def ensure_warehouse_id(
    config: DatabricksConfig,
    *,
    timeout: int = 10,
) -> str:
    """Return a warehouse id, creating a serverless 2X-Small warehouse if none exist."""

    if config.warehouse_id:
        return config.warehouse_id

    warehouses = list_warehouses(config, timeout=timeout)
    if warehouses:
        preferred = _select_preferred_warehouse(warehouses)
        logger.info("Using existing SQL warehouse %s.", preferred["id"])
        return preferred["id"]

    created_id = create_serverless_warehouse(config, timeout=timeout)
    logger.info("Created SQL warehouse %s.", created_id)
    return created_id


def list_warehouses(
    config: DatabricksConfig,
    *,
    timeout: int = 10,
) -> list[dict[str, Any]]:
    """List SQL warehouses visible to the current user."""

    url = f"{config.host.rstrip('/')}/api/2.0/sql/warehouses"
    req = request.Request(url)
    req.add_header("Authorization", f"Bearer {config.token}")
    req.add_header("Accept", "application/json")

    try:
        with request.urlopen(req, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise ValueError(f"Failed to list SQL warehouses. Status {exc.code}.") from exc
    except URLError as exc:
        raise ValueError("Unable to reach Databricks host when listing SQL warehouses.") from exc
    except json.JSONDecodeError as exc:
        raise ValueError("Databricks response for SQL warehouses list was not valid JSON.") from exc

    warehouses = payload.get("warehouses")
    if not isinstance(warehouses, list):
        raise ValueError("Databricks response missing SQL warehouses list.")

    return [item for item in warehouses if isinstance(item, dict) and item.get("id")]


def create_serverless_warehouse(
    config: DatabricksConfig,
    *,
    timeout: int = 10,
) -> str:
    """Create a serverless 2X-Small SQL warehouse and return its id."""

    url = f"{config.host.rstrip('/')}/api/2.0/sql/warehouses"
    payload = json.dumps(
        {
            "name": DEFAULT_WAREHOUSE_NAME,
            "cluster_size": "2X-Small",
            "auto_stop_mins": 10,
            "enable_serverless_compute": True,
            "warehouse_type": "PRO",
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
        raise ValueError(f"Failed to create SQL warehouse. Status {exc.code}.") from exc
    except URLError as exc:
        raise ValueError("Unable to reach Databricks host when creating SQL warehouse.") from exc
    except json.JSONDecodeError as exc:
        raise ValueError("Databricks response for SQL warehouse create was not valid JSON.") from exc

    warehouse_id = data.get("id")
    if not warehouse_id:
        raise ValueError("Databricks response missing SQL warehouse id.")

    return warehouse_id


def _select_preferred_warehouse(warehouses: list[dict[str, Any]]) -> dict[str, Any]:
    for warehouse in warehouses:
        if warehouse.get("enable_serverless_compute") is True:
            return warehouse
    return warehouses[0]
