"""Unit tests for vendor_spend.config (pure parser + env helper)."""

import pytest

from lucille.vendor_spend.config import (
    AwsConfig,
    DatabricksConfig,
    DatadogConfig,
    parse_config,
    require_env,
)


def _minimal_raw():
    return {
        "output_dir": "~/Desktop/debris",
        "weeks": 6,
        "aws": {"account_id": "123456789012"},
        "databricks": {"account_id": "abc-uuid"},
        "datadog": {},
    }


class TestParseConfig:
    def test_minimal_valid(self):
        cfg = parse_config(_minimal_raw())
        assert cfg.weeks == 6
        assert cfg.aws == AwsConfig(account_id="123456789012", region="us-east-1")
        assert cfg.databricks.account_id == "abc-uuid"
        assert cfg.databricks.token_env == "DATABRICKS_TOKEN"
        assert cfg.databricks.client_id_env == "DATABRICKS_CLIENT_ID"
        assert cfg.databricks.client_secret_env == "DATABRICKS_CLIENT_SECRET"
        assert cfg.datadog == DatadogConfig()

    def test_databricks_sku_prices_default_empty(self):
        cfg = parse_config(_minimal_raw())
        assert cfg.databricks.sku_prices == {}

    def test_databricks_sku_prices_parsed(self):
        raw = _minimal_raw()
        raw["databricks"]["sku_prices"] = {
            "STANDARD_JOBS_COMPUTE": 0.10,
            "STANDARD_ALL_PURPOSE_COMPUTE": "0.55",  # string -> float coercion
        }
        cfg = parse_config(raw)
        assert cfg.databricks.sku_prices == {
            "STANDARD_JOBS_COMPUTE": 0.10,
            "STANDARD_ALL_PURPOSE_COMPUTE": 0.55,
        }

    def test_databricks_sku_prices_must_be_mapping(self):
        raw = _minimal_raw()
        raw["databricks"]["sku_prices"] = ["not", "a", "map"]
        with pytest.raises(ValueError, match="sku_prices"):
            parse_config(raw)

    def test_databricks_oauth_env_overrides(self):
        raw = _minimal_raw()
        raw["databricks"]["client_id_env"] = "MY_DBX_ID"
        raw["databricks"]["client_secret_env"] = "MY_DBX_SECRET"
        cfg = parse_config(raw)
        assert cfg.databricks.client_id_env == "MY_DBX_ID"
        assert cfg.databricks.client_secret_env == "MY_DBX_SECRET"

    def test_overrides_propagate(self):
        raw = _minimal_raw()
        raw["aws"]["region"] = "us-west-2"
        raw["databricks"]["token_env"] = "DBX_TOKEN"
        raw["datadog"] = {"site": "datadoghq.eu", "api_key_env": "X", "app_key_env": "Y"}
        cfg = parse_config(raw)
        assert cfg.aws.region == "us-west-2"
        assert cfg.databricks.token_env == "DBX_TOKEN"
        assert cfg.datadog.site == "datadoghq.eu"
        assert cfg.datadog.api_key_env == "X"
        assert cfg.datadog.app_key_env == "Y"

    def test_output_dir_expands_user(self):
        cfg = parse_config(_minimal_raw())
        assert "~" not in str(cfg.output_dir)

    def test_default_weeks(self):
        raw = _minimal_raw()
        del raw["weeks"]
        assert parse_config(raw).weeks == 6

    @pytest.mark.parametrize("bad", [0, -1, -10])
    def test_non_positive_weeks_rejected(self, bad):
        raw = _minimal_raw()
        raw["weeks"] = bad
        with pytest.raises(ValueError):
            parse_config(raw)

    def test_missing_aws_section_raises(self):
        raw = _minimal_raw()
        del raw["aws"]
        with pytest.raises(ValueError, match="aws"):
            parse_config(raw)

    def test_missing_databricks_section_raises(self):
        raw = _minimal_raw()
        del raw["databricks"]
        with pytest.raises(ValueError, match="databricks"):
            parse_config(raw)


class TestRequireEnv:
    def test_returns_value_when_set(self, monkeypatch):
        monkeypatch.setenv("VS_TEST_VAR", "hello")
        assert require_env("VS_TEST_VAR") == "hello"

    def test_raises_when_unset(self, monkeypatch):
        monkeypatch.delenv("VS_TEST_VAR", raising=False)
        with pytest.raises(RuntimeError, match="VS_TEST_VAR"):
            require_env("VS_TEST_VAR")

    def test_raises_when_empty(self, monkeypatch):
        monkeypatch.setenv("VS_TEST_VAR", "")
        with pytest.raises(RuntimeError):
            require_env("VS_TEST_VAR")
