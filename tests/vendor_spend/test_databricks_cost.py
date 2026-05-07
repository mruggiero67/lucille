"""Unit tests for vendor_spend.databricks_cost."""

from datetime import date
from unittest.mock import MagicMock

import pytest

from lucille.vendor_spend.config import DatabricksConfig
from lucille.vendor_spend.databricks_auth import clear_token_cache
from lucille.vendor_spend.databricks_cost import (

    _months_covering,
    _pick_price_column,
    fetch_daily_costs,
    fetch_raw,
    parse_billable_usage_csv,
)


CSV_HEADER_NEW = "usage_date,usage_quantity,usage_unit_price,sku_name\n"
CSV_HEADER_LEGACY = "usage_date,usage_quantity,list_price,sku_name\n"
# True legacy v1 "billable usage logs" schema (DBUs only, no price).
CSV_HEADER_LEGACY_V1 = (
    "clusterId,clusterName,dbus,machineHours,sku,timestamp,workspaceId\n"
)


@pytest.fixture(autouse=True)
def _reset_dbx_token_cache():
    clear_token_cache()
    yield
    clear_token_cache()


class TestMonthsCovering:
    def test_same_month(self):
        assert _months_covering(date(2026, 4, 1), date(2026, 4, 30)) == ("2026-04", "2026-04")

    def test_spans_months(self):
        assert _months_covering(date(2026, 3, 16), date(2026, 4, 26)) == ("2026-03", "2026-04")

    def test_inverted_raises(self):
        with pytest.raises(ValueError):
            _months_covering(date(2026, 4, 26), date(2026, 3, 16))


class TestPickPriceColumn:
    def test_prefers_usage_unit_price(self):
        assert _pick_price_column(["usage_date", "usage_unit_price", "list_price"]) == "usage_unit_price"

    def test_falls_back_to_list_price(self):
        assert _pick_price_column(["usage_date", "list_price"]) == "list_price"

    def test_raises_when_neither_present(self):
        with pytest.raises(ValueError):
            _pick_price_column(["usage_date", "usage_quantity"])


class TestParseBillableUsageCsv:
    def test_basic_new_schema(self):
        body = (
            CSV_HEADER_NEW
            + "2026-04-13,10,0.5,JOBS\n"
            + "2026-04-14,4,1.25,JOBS\n"
        )
        rows = parse_billable_usage_csv(body, date(2026, 4, 1), date(2026, 4, 30))
        assert rows == [(date(2026, 4, 13), 5.0), (date(2026, 4, 14), 5.0)]

    def test_filters_to_window(self):
        body = (
            CSV_HEADER_NEW
            + "2026-03-15,10,1.0,X\n"   # before window
            + "2026-04-13,10,1.0,X\n"   # in
            + "2026-05-01,10,1.0,X\n"   # after
        )
        rows = parse_billable_usage_csv(body, date(2026, 4, 1), date(2026, 4, 30))
        assert rows == [(date(2026, 4, 13), 10.0)]

    def test_legacy_list_price(self):
        body = CSV_HEADER_LEGACY + "2026-04-13,2,3.0,JOBS\n"
        rows = parse_billable_usage_csv(body, date(2026, 4, 1), date(2026, 4, 30))
        assert rows == [(date(2026, 4, 13), 6.0)]

    def test_skips_rows_with_blank_date(self):
        body = CSV_HEADER_NEW + ",10,1.0,X\n2026-04-13,1,1.0,X\n"
        rows = parse_billable_usage_csv(body, date(2026, 4, 1), date(2026, 4, 30))
        assert rows == [(date(2026, 4, 13), 1.0)]

    def test_skips_rows_with_unparseable_numbers(self):
        body = CSV_HEADER_NEW + "2026-04-13,oops,1.0,X\n2026-04-14,1,2.0,X\n"
        rows = parse_billable_usage_csv(body, date(2026, 4, 1), date(2026, 4, 30))
        assert rows == [(date(2026, 4, 14), 2.0)]

    def test_treats_missing_numbers_as_zero(self):
        body = CSV_HEADER_NEW + "2026-04-13,,,X\n"
        rows = parse_billable_usage_csv(body, date(2026, 4, 1), date(2026, 4, 30))
        assert rows == [(date(2026, 4, 13), 0.0)]

    def test_empty_csv(self):
        assert parse_billable_usage_csv("", date(2026, 4, 1), date(2026, 4, 30)) == []

    def test_missing_price_column_raises(self):
        body = "usage_date,usage_quantity\n2026-04-13,1\n"
        with pytest.raises(ValueError):
            parse_billable_usage_csv(body, date(2026, 4, 1), date(2026, 4, 30))


class TestParseBillableUsageCsvLegacyV1:
    def test_multiplies_dbus_by_sku_price(self):
        body = (
            CSV_HEADER_LEGACY_V1
            + "c1,job-cluster,10,5,STANDARD_JOBS_COMPUTE,2026-04-13,ws1\n"
            + "c2,interactive,4,2,STANDARD_ALL_PURPOSE_COMPUTE,2026-04-14,ws1\n"
        )
        prices = {
            "STANDARD_JOBS_COMPUTE": 0.10,
            "STANDARD_ALL_PURPOSE_COMPUTE": 0.55,
        }
        rows = parse_billable_usage_csv(
            body, date(2026, 4, 1), date(2026, 4, 30), sku_prices=prices
        )
        assert rows == [
            (date(2026, 4, 13), 1.0),    # 10 * 0.10
            (date(2026, 4, 14), 2.20),   # 4 * 0.55
        ]

    def test_filters_window(self):
        body = (
            CSV_HEADER_LEGACY_V1
            + "c1,x,10,5,SKU1,2026-03-01,ws1\n"
            + "c1,x,10,5,SKU1,2026-04-13,ws1\n"
            + "c1,x,10,5,SKU1,2026-05-15,ws1\n"
        )
        rows = parse_billable_usage_csv(
            body, date(2026, 4, 1), date(2026, 4, 30), sku_prices={"SKU1": 1.0}
        )
        assert rows == [(date(2026, 4, 13), 10.0)]

    def test_no_prices_provided_raises(self):
        body = CSV_HEADER_LEGACY_V1 + "c1,x,10,5,SKU1,2026-04-13,ws1\n"
        with pytest.raises(RuntimeError, match="legacy CSV schema"):
            parse_billable_usage_csv(body, date(2026, 4, 1), date(2026, 4, 30))

    def test_no_prices_provided_empty_dict_raises(self):
        body = CSV_HEADER_LEGACY_V1 + "c1,x,10,5,SKU1,2026-04-13,ws1\n"
        with pytest.raises(RuntimeError, match="legacy CSV schema"):
            parse_billable_usage_csv(
                body, date(2026, 4, 1), date(2026, 4, 30), sku_prices={}
            )

    def test_unknown_sku_raises_with_list(self):
        body = (
            CSV_HEADER_LEGACY_V1
            + "c1,x,10,5,KNOWN_SKU,2026-04-13,ws1\n"
            + "c1,x,10,5,UNKNOWN_SKU_A,2026-04-14,ws1\n"
            + "c1,x,10,5,UNKNOWN_SKU_B,2026-04-15,ws1\n"
        )
        with pytest.raises(RuntimeError, match="UNKNOWN_SKU_A.*UNKNOWN_SKU_B"):
            parse_billable_usage_csv(
                body,
                date(2026, 4, 1),
                date(2026, 4, 30),
                sku_prices={"KNOWN_SKU": 1.0},
            )

    def test_zero_priced_sku_is_ignored_silently(self):
        body = (
            CSV_HEADER_LEGACY_V1
            + "c1,x,10,5,FREE_SKU,2026-04-13,ws1\n"
            + "c1,x,4,2,PAID_SKU,2026-04-14,ws1\n"
        )
        rows = parse_billable_usage_csv(
            body,
            date(2026, 4, 1),
            date(2026, 4, 30),
            sku_prices={"FREE_SKU": 0.0, "PAID_SKU": 1.0},
        )
        assert rows == [(date(2026, 4, 13), 0.0), (date(2026, 4, 14), 4.0)]

    def test_handles_iso_timestamp_with_time_component(self):
        body = (
            CSV_HEADER_LEGACY_V1
            + "c1,x,10,5,SKU1,2026-04-13T12:34:56Z,ws1\n"
        )
        rows = parse_billable_usage_csv(
            body, date(2026, 4, 1), date(2026, 4, 30), sku_prices={"SKU1": 1.0}
        )
        assert rows == [(date(2026, 4, 13), 10.0)]


class TestFetchRaw:
    def test_calls_correct_url_and_headers(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        cfg = DatabricksConfig(account_id="acct-uuid")
        session = MagicMock()
        response = MagicMock()
        response.text = CSV_HEADER_NEW
        response.headers = {"Content-Type": "text/csv"}
        response.raise_for_status.return_value = None
        session.get.return_value = response

        fetch_raw(cfg, date(2026, 3, 16), date(2026, 4, 26), session=session)

        url = session.get.call_args.args[0]
        kwargs = session.get.call_args.kwargs
        assert url == "https://accounts.cloud.databricks.com/api/2.0/accounts/acct-uuid/usage/download"
        assert kwargs["params"] == {"start_month": "2026-03", "end_month": "2026-04"}
        assert kwargs["headers"]["Authorization"] == "Bearer tok"

    def test_missing_token_raises(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        with pytest.raises(RuntimeError, match="No Databricks credentials"):
            fetch_raw(DatabricksConfig(account_id="x"), date(2026, 4, 1), date(2026, 4, 7))

    def test_uses_oauth_when_no_static_token(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")
        cfg = DatabricksConfig(account_id="acct-uuid")

        session = MagicMock()
        token_resp = MagicMock()
        token_resp.json.return_value = {"access_token": "minted-tok"}
        token_resp.raise_for_status.return_value = None
        csv_resp = MagicMock()
        csv_resp.text = CSV_HEADER_NEW
        csv_resp.headers = {"Content-Type": "text/csv"}
        csv_resp.raise_for_status.return_value = None
        session.post.return_value = token_resp
        session.get.return_value = csv_resp

        fetch_raw(cfg, date(2026, 4, 1), date(2026, 4, 7), session=session)

        # OAuth exchange happened
        session.post.assert_called_once()
        assert "oidc/accounts/acct-uuid/v1/token" in session.post.call_args.args[0]
        # Minted token was used on the GET
        assert session.get.call_args.kwargs["headers"]["Authorization"] == "Bearer minted-tok"


class TestFetchDailyCosts:
    def test_end_to_end(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        session = MagicMock()
        response = MagicMock()
        response.text = CSV_HEADER_NEW + "2026-04-13,10,0.5,JOBS\n"
        response.headers = {"Content-Type": "text/csv"}
        response.raise_for_status.return_value = None
        session.get.return_value = response

        rows = fetch_daily_costs(
            DatabricksConfig(account_id="acct"),
            date(2026, 4, 1),
            date(2026, 4, 30),
            session=session,
        )
        assert rows == [(date(2026, 4, 13), 5.0)]

    def test_html_response_raises_clear_error(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_TOKEN", "tok")
        session = MagicMock()
        response = MagicMock()
        response.text = "<!doctype html><html><body>Login</body></html>"
        response.headers = {"Content-Type": "text/html; charset=utf-8"}
        response.url = "https://accounts.cloud.databricks.com/login"
        response.status_code = 200
        response.raise_for_status.return_value = None
        session.get.return_value = response

        with pytest.raises(RuntimeError, match="returned HTML instead of CSV"):
            fetch_daily_costs(
                DatabricksConfig(account_id="acct"),
                date(2026, 4, 1),
                date(2026, 4, 30),
                session=session,
            )
