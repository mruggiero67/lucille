"""Unit tests for vendor_spend.datadog_cost."""

from datetime import date
from unittest.mock import MagicMock

import pytest

from lucille.vendor_spend.config import DatadogConfig
from lucille.vendor_spend.datadog_cost import (
    date_range_inclusive,
    fetch_daily_costs,
    fetch_raw,
    parse_estimated_cost_response,
)


def _payload(rows):
    return {
        "data": [
            {
                "type": "usage_timeseries",
                "attributes": {"date": d, "total_cost": c, "org_name": "acme"},
            }
            for d, c in rows
        ]
    }


class TestParseEstimatedCostResponse:
    def test_converts_monthly_estimate_to_daily_rate(self):
        # April has 30 days. $300 monthly -> $10/day.
        # May has 31 days.   $310 monthly -> $10/day.
        payload = _payload(
            [
                ("2026-04-13T00:00:00+00:00", 300.0),
                ("2026-05-14T00:00:00+00:00", 310.0),
            ]
        )
        assert parse_estimated_cost_response(payload) == [
            (date(2026, 4, 13), 10.0),
            (date(2026, 5, 14), 10.0),
        ]

    def test_february_uses_28_or_29_days(self):
        # 2024 was a leap year; 2026 is not.
        payload = _payload(
            [
                ("2024-02-15", 290.0),  # 290/29 = 10.0
                ("2026-02-15", 280.0),  # 280/28 = 10.0
            ]
        )
        assert parse_estimated_cost_response(payload) == [
            (date(2024, 2, 15), 10.0),
            (date(2026, 2, 15), 10.0),
        ]

    def test_repeated_monthly_values_become_constant_daily_rate(self):
        # Mirrors the real-world response shape: same monthly value repeated
        # for each day of the month. After conversion each day's rate is the
        # same, and summing N of them == monthly * N / days_in_month.
        payload = _payload(
            [
                ("2026-04-01", 19292.17),
                ("2026-04-02", 19292.17),
                ("2026-04-03", 19292.17),
            ]
        )
        rows = parse_estimated_cost_response(payload)
        rates = [r[1] for r in rows]
        # All days in same month -> identical daily rate
        assert rates[0] == rates[1] == rates[2]
        # Daily rate ~= monthly / 30 (April)
        assert rates[0] == pytest.approx(19292.17 / 30)
        # And summing 7 such rates approximates the weekly equivalent
        weekly_equivalent = rates[0] * 7
        assert weekly_equivalent == pytest.approx(19292.17 * 7 / 30)

    def test_empty_data(self):
        assert parse_estimated_cost_response({"data": []}) == []

    def test_missing_data_key(self):
        assert parse_estimated_cost_response({}) == []

    def test_skips_items_without_date_or_cost(self):
        payload = {
            "data": [
                {"attributes": {"date": "2026-04-13", "total_cost": 30.0}},
                {"attributes": {"date": "2026-04-14"}},  # missing total_cost
                {"attributes": {"total_cost": 5.0}},     # missing date
                {},                                      # no attributes
            ]
        }
        # 30 / 30 days in April = 1.0
        assert parse_estimated_cost_response(payload) == [(date(2026, 4, 13), 1.0)]

    def test_coerces_total_cost_to_float(self):
        payload = _payload([("2026-04-13", "30.0")])
        assert parse_estimated_cost_response(payload) == [(date(2026, 4, 13), 1.0)]

    def test_skips_unparseable_rows(self):
        payload = {
            "data": [
                {"attributes": {"date": "not-a-date", "total_cost": 30.0}},
                {"attributes": {"date": "2026-04-13", "total_cost": "not-a-number"}},
                {"attributes": {"date": "2026-04-14", "total_cost": 30.0}},
            ]
        }
        assert parse_estimated_cost_response(payload) == [(date(2026, 4, 14), 1.0)]


class TestFetchRaw:
    def test_calls_correct_url_with_headers(self, monkeypatch):
        monkeypatch.setenv("DD_API_KEY", "api-secret")
        monkeypatch.setenv("DD_APP_KEY", "app-secret")
        cfg = DatadogConfig()

        session = MagicMock()
        response = MagicMock()
        response.ok = True
        response.json.return_value = {"data": []}
        session.get.return_value = response

        fetch_raw(cfg, date(2026, 3, 16), date(2026, 4, 26), session=session)

        session.get.assert_called_once()
        args, kwargs = session.get.call_args
        assert args[0] == "https://api.datadoghq.com/api/v2/usage/estimated_cost"
        # Hour-aware format; end is the day *after* end_date at 00 (exclusive)
        # to make the [start, end] window inclusive.
        assert kwargs["params"] == {
            "view": "summary",
            "start_date": "2026-03-16T00",
            "end_date": "2026-04-27T00",
        }
        assert kwargs["headers"]["DD-API-KEY"] == "api-secret"
        assert kwargs["headers"]["DD-APPLICATION-KEY"] == "app-secret"

    def test_400_includes_response_body_in_error(self, monkeypatch):
        monkeypatch.setenv("DD_API_KEY", "x")
        monkeypatch.setenv("DD_APP_KEY", "y")
        session = MagicMock()
        response = MagicMock()
        response.ok = False
        response.status_code = 400
        response.text = '{"errors":["Invalid date format"]}'
        session.get.return_value = response

        with pytest.raises(
            __import__("requests").HTTPError, match="Invalid date format"
        ):
            fetch_raw(
                DatadogConfig(), date(2026, 3, 16), date(2026, 4, 26), session=session
            )

    def test_uses_configured_site(self, monkeypatch):
        monkeypatch.setenv("DD_API_KEY", "x")
        monkeypatch.setenv("DD_APP_KEY", "y")
        cfg = DatadogConfig(site="datadoghq.eu")

        session = MagicMock()
        response = MagicMock()
        response.ok = True
        response.json.return_value = {"data": []}
        session.get.return_value = response

        fetch_raw(cfg, date(2026, 4, 1), date(2026, 4, 7), session=session)
        url = session.get.call_args.args[0]
        assert "datadoghq.eu" in url

    def test_missing_env_raises(self, monkeypatch):
        monkeypatch.delenv("DD_API_KEY", raising=False)
        monkeypatch.delenv("DD_APP_KEY", raising=False)
        with pytest.raises(RuntimeError, match="DD_API_KEY"):
            fetch_raw(DatadogConfig(), date(2026, 4, 1), date(2026, 4, 7))


class TestFetchDailyCosts:
    def test_end_to_end_with_mock(self, monkeypatch):
        monkeypatch.setenv("DD_API_KEY", "x")
        monkeypatch.setenv("DD_APP_KEY", "y")
        session = MagicMock()
        response = MagicMock()
        response.ok = True
        # Monthly values; April has 30 days -> daily rates are /30.
        response.json.return_value = _payload(
            [("2026-04-13", 30.0), ("2026-04-14", 60.0)]
        )
        session.get.return_value = response

        rows = fetch_daily_costs(
            DatadogConfig(), date(2026, 4, 13), date(2026, 4, 14), session=session
        )
        assert rows == [(date(2026, 4, 13), 1.0), (date(2026, 4, 14), 2.0)]


class TestDateRangeInclusive:
    def test_basic(self):
        assert date_range_inclusive(date(2026, 4, 13), date(2026, 4, 15)) == [
            date(2026, 4, 13),
            date(2026, 4, 14),
            date(2026, 4, 15),
        ]

    def test_single_day(self):
        assert date_range_inclusive(date(2026, 4, 13), date(2026, 4, 13)) == [
            date(2026, 4, 13)
        ]

    def test_inverted_raises(self):
        with pytest.raises(ValueError):
            date_range_inclusive(date(2026, 4, 15), date(2026, 4, 13))
