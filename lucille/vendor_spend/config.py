"""
Config loader for the vendor-spend report.

The YAML lives outside the repo at ~/bin/vendor_spend.yaml by convention.
Secrets are *not* in the YAML; they come from environment variables whose
names are listed in the YAML (``token_env``, ``api_key_env``, etc.).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = "~/bin/vendor_spend.yaml"


@dataclass(frozen=True)
class AwsConfig:
    account_id: str
    region: str = "us-east-1"


@dataclass(frozen=True)
class DatabricksConfig:
    account_id: str
    # Static-token escape hatch (legacy account-admin PAT). If set in env, used as-is.
    token_env: str = "DATABRICKS_TOKEN"
    # OAuth client-credentials path (preferred). If the static token env is
    # unset, we mint a short-lived bearer using these.
    client_id_env: str = "DATABRICKS_CLIENT_ID"
    client_secret_env: str = "DATABRICKS_CLIENT_SECRET"
    accounts_host: str = "https://accounts.cloud.databricks.com"
    # The legacy /api/2.0/.../usage/download CSV reports DBU consumption only,
    # no dollars. To turn DBUs into USD we multiply by a per-SKU price map
    # supplied here. Keys are exact SKU strings as they appear in the
    # "sku" column of the CSV (e.g. "STANDARD_ALL_PURPOSE_COMPUTE").
    sku_prices: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class DatadogConfig:
    site: str = "datadoghq.com"
    api_key_env: str = "DD_API_KEY"
    app_key_env: str = "DD_APP_KEY"


@dataclass(frozen=True)
class VendorSpendConfig:
    output_dir: Path
    weeks: int
    aws: AwsConfig
    databricks: DatabricksConfig
    datadog: DatadogConfig


def expand_path(p: str | os.PathLike) -> Path:
    return Path(os.path.expanduser(str(p))).resolve()


def load_config(path: str | os.PathLike | None = None) -> VendorSpendConfig:
    """Load and validate the YAML config. ``path`` defaults to ``~/bin/vendor_spend.yaml``."""
    cfg_path = expand_path(path or DEFAULT_CONFIG_PATH)
    logger.info("Loading vendor-spend config from %s", cfg_path)
    with open(cfg_path, "r") as f:
        raw: dict[str, Any] = yaml.safe_load(f) or {}
    return parse_config(raw)


def parse_config(raw: dict[str, Any]) -> VendorSpendConfig:
    """Parse a raw YAML mapping into a ``VendorSpendConfig``. Pure function."""
    try:
        output_dir = expand_path(raw.get("output_dir", "~/Desktop/debris"))
        weeks = int(raw.get("weeks", 6))
        aws_raw = raw["aws"]
        databricks_raw = raw["databricks"]
        datadog_raw = raw.get("datadog", {})
    except KeyError as e:
        raise ValueError(f"Missing required config section: {e}") from e

    if weeks <= 0:
        raise ValueError("`weeks` must be positive")

    aws = AwsConfig(
        account_id=str(aws_raw["account_id"]),
        region=str(aws_raw.get("region", "us-east-1")),
    )
    sku_prices_raw = databricks_raw.get("sku_prices") or {}
    if not isinstance(sku_prices_raw, dict):
        raise ValueError("databricks.sku_prices must be a mapping of sku -> usd_per_dbu")
    sku_prices = {str(k): float(v) for k, v in sku_prices_raw.items()}

    databricks = DatabricksConfig(
        account_id=str(databricks_raw["account_id"]),
        token_env=str(databricks_raw.get("token_env", "DATABRICKS_TOKEN")),
        client_id_env=str(databricks_raw.get("client_id_env", "DATABRICKS_CLIENT_ID")),
        client_secret_env=str(
            databricks_raw.get("client_secret_env", "DATABRICKS_CLIENT_SECRET")
        ),
        accounts_host=str(
            databricks_raw.get("accounts_host", "https://accounts.cloud.databricks.com")
        ),
        sku_prices=sku_prices,
    )
    datadog = DatadogConfig(
        site=str(datadog_raw.get("site", "datadoghq.com")),
        api_key_env=str(datadog_raw.get("api_key_env", "DD_API_KEY")),
        app_key_env=str(datadog_raw.get("app_key_env", "DD_APP_KEY")),
    )

    return VendorSpendConfig(
        output_dir=output_dir,
        weeks=weeks,
        aws=aws,
        databricks=databricks,
        datadog=datadog,
    )


def require_env(var_name: str) -> str:
    """Read a required environment variable or raise with a helpful message."""
    val = os.environ.get(var_name)
    if not val:
        raise RuntimeError(
            f"Required environment variable {var_name!r} is not set. "
            f"Export it before running the vendor-spend scripts."
        )
    return val
