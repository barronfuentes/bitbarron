"""Configuration loading for Databricks auth and host settings."""

from __future__ import annotations

import os
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError


@dataclass(frozen=True)
class DatabricksConfig:
    """Databricks SDK configuration wrapper."""

    client: WorkspaceClient
    profile: str | None = None
    warehouse_id: str | None = None


def load_databricks_config(profile_override: str | None = None) -> DatabricksConfig:
    """Load Databricks configuration using the Databricks SDK."""

    profile = profile_override or None
    client = WorkspaceClient(profile=profile) if profile else WorkspaceClient()
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if warehouse_id:
        warehouse_id = warehouse_id.strip() or None
    return DatabricksConfig(client=client, profile=profile, warehouse_id=warehouse_id)


def validate_databricks_credentials(config: DatabricksConfig, catalog: str, schema: str, table: str) -> None:
    """Validate that credentials can access the target Unity Catalog table."""

    full_name = f"{catalog}.{schema}.{table}"
    try:
        config.client.tables.get(full_name)
    except DatabricksError as exc:
        raise ValueError("Databricks auth validation failed for Unity Catalog table access.") from exc
