"""Helpers for discovering or creating Databricks SQL warehouses."""

from __future__ import annotations

import logging

from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.sql import EndpointInfo

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
        logger.info("Using existing SQL warehouse %s.", preferred.id)
        return preferred.id

    created_id = create_serverless_warehouse(config, timeout=timeout)
    logger.info("Created SQL warehouse %s.", created_id)
    return created_id


def list_warehouses(
    config: DatabricksConfig,
    *,
    timeout: int = 10,
) -> list[EndpointInfo]:
    """List SQL warehouses visible to the current user."""

    try:
        return list(config.client.warehouses.list())
    except DatabricksError as exc:
        raise ValueError("Failed to list SQL warehouses.") from exc


def create_serverless_warehouse(
    config: DatabricksConfig,
    *,
    timeout: int = 10,
) -> str:
    """Create a serverless 2X-Small SQL warehouse and return its id."""
    try:
        response = config.client.warehouses.create(
            name=DEFAULT_WAREHOUSE_NAME,
            cluster_size="2X-Small",
            auto_stop_mins=10,
            enable_serverless_compute=True,
            warehouse_type="PRO",
        ).result(timeout=timeout)
    except DatabricksError as exc:
        raise ValueError("Failed to create SQL warehouse.") from exc

    if not response.id:
        raise ValueError("Databricks response missing SQL warehouse id.")

    return response.id


def _select_preferred_warehouse(warehouses: list[EndpointInfo]) -> EndpointInfo:
    for warehouse in warehouses:
        if warehouse.enable_serverless_compute is True:
            return warehouse
    return warehouses[0]
