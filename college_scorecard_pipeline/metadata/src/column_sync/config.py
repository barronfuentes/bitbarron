"""Configuration loading for Databricks auth and host settings."""

from __future__ import annotations

import configparser
from dataclasses import dataclass, field
import os
from pathlib import Path
from urllib import request
from urllib.error import HTTPError, URLError


@dataclass(frozen=True)
class DatabricksConfig:
    """Databricks host/auth configuration."""

    host: str
    profile: str | None = None
    warehouse_id: str | None = None
    token: str | None = field(default=None, repr=False)


def load_databricks_config(profile_override: str | None = None) -> DatabricksConfig:
    """Load Databricks configuration from ~/.databrickscfg.

    Args:
        profile_override: Optional profile name to load. Defaults to "DEFAULT".
    """

    profile = profile_override or "DEFAULT"
    host, token = _load_profile(profile)
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if warehouse_id:
        warehouse_id = warehouse_id.strip() or None
    return DatabricksConfig(host=host, token=token, profile=profile, warehouse_id=warehouse_id)


def validate_databricks_credentials(config: DatabricksConfig, catalog: str, schema: str, table: str) -> None:
    """Validate that credentials can access the target Unity Catalog table."""

    full_name = f"{catalog}.{schema}.{table}"
    url = f"{config.host.rstrip('/')}/api/2.1/unity-catalog/tables/{full_name}"
    req = request.Request(url)
    req.add_header("Authorization", f"Bearer {config.token}")
    req.add_header("Accept", "application/json")

    try:
        with request.urlopen(req, timeout=10) as response:
            if not (200 <= response.status < 300):
                raise ValueError(f"Databricks auth validation failed with status {response.status}.")
    except HTTPError as exc:
        raise ValueError(f"Databricks auth validation failed with status {exc.code}.") from exc
    except URLError as exc:
        raise ValueError("Unable to reach Databricks host for auth validation.") from exc


def _load_profile(profile: str) -> tuple[str | None, str | None]:
    path = Path("~/.databrickscfg").expanduser()

    if not path.exists():
        raise ValueError("Missing ~/.databrickscfg; run databricks auth login.")

    parser = configparser.RawConfigParser()
    parser.read(path)

    if profile == "DEFAULT":
        defaults = parser.defaults()
        host = defaults.get("host")
        token = defaults.get("token")
    else:
        if not parser.has_section(profile):
            raise ValueError(f"Missing profile {profile!r} in ~/.databrickscfg.")
        host = parser.get(profile, "host", fallback=None)
        token = parser.get(profile, "token", fallback=None)

    if host:
        host = host.strip() or None
    if token:
        token = token.strip() or None

    if not host:
        raise ValueError(f"Missing host for profile {profile!r} in ~/.databrickscfg.")

    if not token:
        raise ValueError(f"Missing token for profile {profile!r} in ~/.databrickscfg.")

    return host, token
