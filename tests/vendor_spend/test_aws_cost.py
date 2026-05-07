"""Unit tests for vendor_spend.aws_cost."""

from datetime import date
from unittest.mock import MagicMock

import pytest

from lucille.vendor_spend.aws_cost import (
    fetch_daily_costs,
    fetch_raw,
    parse_cost_and_usage_response,
)
from lucille.vendor_spend.config import AwsConfig


def _bucket(start, amount, unit="USD"):
    # AWS End is exclusive (next day) but we never read it.
    return {
        "TimePeriod": {"Start": start, "End": start},
        "Total": {"UnblendedCost": {"Amount": str(amount), "Unit": unit}},
    }


class TestParseCostAndUsageResponse:
    def test_basic(self):
        payload = {
            "ResultsByTime": [
                _bucket("2026-04-13", "100.50"),
                _bucket("2026-04-14", "200.25"),
            ]
        }
        assert parse_cost_and_usage_response(payload) == [
            (date(2026, 4, 13), 100.50),
            (date(2026, 4, 14), 200.25),
        ]

    def test_empty(self):
        assert parse_cost_and_usage_response({"ResultsByTime": []}) == []
        assert parse_cost_and_usage_response({}) == []

    def test_skips_buckets_without_start(self):
        payload = {
            "ResultsByTime": [
                {"Total": {"UnblendedCost": {"Amount": "1"}}},
                _bucket("2026-04-13", "2.0"),
            ]
        }
        assert parse_cost_and_usage_response(payload) == [(date(2026, 4, 13), 2.0)]

    def test_skips_buckets_without_amount(self):
        payload = {
            "ResultsByTime": [
                {"TimePeriod": {"Start": "2026-04-13"}, "Total": {"UnblendedCost": {}}},
                _bucket("2026-04-14", "1.0"),
            ]
        }
        assert parse_cost_and_usage_response(payload) == [(date(2026, 4, 14), 1.0)]

    def test_skips_unparseable_amount(self):
        payload = {
            "ResultsByTime": [
                {
                    "TimePeriod": {"Start": "2026-04-13"},
                    "Total": {"UnblendedCost": {"Amount": "not-a-number"}},
                },
                _bucket("2026-04-14", "3.0"),
            ]
        }
        assert parse_cost_and_usage_response(payload) == [(date(2026, 4, 14), 3.0)]


class TestFetchRaw:
    def test_calls_get_cost_and_usage_with_exclusive_end(self):
        client = MagicMock()
        client.get_cost_and_usage.return_value = {"ResultsByTime": []}
        cfg = AwsConfig(account_id="123456789012")

        fetch_raw(cfg, date(2026, 3, 16), date(2026, 4, 26), client=client)

        client.get_cost_and_usage.assert_called_once_with(
            TimePeriod={"Start": "2026-03-16", "End": "2026-04-27"},  # +1 day
            Granularity="DAILY",
            Metrics=["UnblendedCost"],
        )

    def test_inverted_range_raises(self):
        client = MagicMock()
        with pytest.raises(ValueError):
            fetch_raw(AwsConfig(account_id="x"), date(2026, 4, 26), date(2026, 3, 16), client=client)
        client.get_cost_and_usage.assert_not_called()


class TestFetchDailyCosts:
    def test_end_to_end(self):
        client = MagicMock()
        client.get_cost_and_usage.return_value = {
            "ResultsByTime": [_bucket("2026-04-13", "10.0"), _bucket("2026-04-14", "20.0")]
        }
        rows = fetch_daily_costs(
            AwsConfig(account_id="x"), date(2026, 4, 13), date(2026, 4, 14), client=client
        )
        assert rows == [(date(2026, 4, 13), 10.0), (date(2026, 4, 14), 20.0)]
