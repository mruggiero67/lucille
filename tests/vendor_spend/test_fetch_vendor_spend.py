"""Unit tests for vendor_spend.fetch_vendor_spend (orchestrator + CSV writer)."""

import csv
from datetime import date
from unittest.mock import patch

import pytest

from lucille.vendor_spend import fetch_vendor_spend as fvs
from lucille.vendor_spend.fetch_vendor_spend import (
    SpendRow,
    SOURCES,
    VENDOR_AWS,
    VENDOR_DATABRICKS,
    VENDOR_DATADOG,
    _parse_vendors,
    build_spend_rows,
    collect_daily_rows,
    csv_filename_for,
    write_csv,
)
from lucille.vendor_spend.weekly_buckets import last_n_week_starts


WEEKS = last_n_week_starts(date(2026, 5, 1), 6)


class TestBuildSpendRows:
    def test_emits_one_row_per_vendor_per_week(self):
        daily = {VENDOR_AWS: [], VENDOR_DATADOG: [], VENDOR_DATABRICKS: []}
        rows = build_spend_rows(WEEKS, daily, "2026-05-01T00:00:00Z")
        assert len(rows) == 3 * 6
        vendors = {r.vendor for r in rows}
        assert vendors == {VENDOR_AWS, VENDOR_DATABRICKS, VENDOR_DATADOG}

    def test_buckets_amounts_correctly(self):
        daily = {
            VENDOR_AWS: [
                (date(2026, 4, 13), 100.0),
                (date(2026, 4, 15), 50.0),  # same Mon-Sun bucket
                (date(2026, 4, 20), 25.0),
            ]
        }
        rows = build_spend_rows(WEEKS, daily, "2026-05-01T00:00:00Z")
        by_week = {r.week_start: r.amount_usd for r in rows if r.vendor == VENDOR_AWS}
        assert by_week[date(2026, 4, 13)] == 150.0
        assert by_week[date(2026, 4, 20)] == 25.0
        assert by_week[date(2026, 3, 16)] == 0.0  # untouched week

    def test_amounts_are_rounded(self):
        daily = {VENDOR_AWS: [(date(2026, 4, 13), 1.236), (date(2026, 4, 14), 1.111)]}
        rows = build_spend_rows(WEEKS, daily, "2026-05-01T00:00:00Z")
        amount = next(r.amount_usd for r in rows if r.week_start == date(2026, 4, 13))
        # 1.236 + 1.111 = 2.347 -> 2.35 at 2dp
        assert amount == pytest.approx(2.35)

    def test_source_field_set_per_vendor(self):
        daily = {VENDOR_DATADOG: []}
        rows = build_spend_rows(WEEKS, daily, "2026-05-01T00:00:00Z")
        assert all(r.source == SOURCES[VENDOR_DATADOG] for r in rows)

    def test_fetched_at_propagated(self):
        rows = build_spend_rows(WEEKS, {VENDOR_AWS: []}, "STAMP")
        assert {r.fetched_at for r in rows} == {"STAMP"}


class TestCsvFilenameFor:
    def test_format(self):
        assert csv_filename_for(date(2026, 5, 1)) == "2026_05_01_vendor_spend.csv"


class TestWriteCsv(object):
    def test_writes_header_and_rows(self, tmp_path):
        rows = [
            SpendRow(date(2026, 4, 13), VENDOR_AWS, 12.34, "src-a", "STAMP"),
            SpendRow(date(2026, 4, 20), VENDOR_DATADOG, 56.78, "src-d", "STAMP"),
        ]
        path = tmp_path / "out.csv"
        write_csv(rows, path)
        with open(path) as f:
            data = list(csv.reader(f))
        assert data[0] == ["week_start", "vendor", "amount_usd", "source", "fetched_at"]
        assert data[1] == ["2026-04-13", "AWS", "12.34", "src-a", "STAMP"]
        assert data[2] == ["2026-04-20", "Datadog", "56.78", "src-d", "STAMP"]

    def test_creates_parent_dir(self, tmp_path):
        path = tmp_path / "nested" / "dir" / "out.csv"
        write_csv([], path)
        assert path.exists()


class TestCollectDailyRows:
    def test_failure_in_one_vendor_does_not_abort_others(self):
        with patch.object(fvs, "_fetch_aws", side_effect=RuntimeError("kaboom")), \
             patch.object(fvs, "_fetch_datadog", return_value=[(date(2026, 4, 13), 5.0)]), \
             patch.object(fvs, "_fetch_databricks", return_value=[]):
            out = collect_daily_rows(
                cfg=None,  # not used by mocked fetchers
                vendors=[VENDOR_AWS, VENDOR_DATABRICKS, VENDOR_DATADOG],
                start=date(2026, 3, 16),
                end=date(2026, 4, 26),
            )
        assert out[VENDOR_AWS] == []
        assert out[VENDOR_DATABRICKS] == []
        assert out[VENDOR_DATADOG] == [(date(2026, 4, 13), 5.0)]


class TestParseVendors:
    def test_canonicalises_case(self):
        assert _parse_vendors("aws,DATADOG,Databricks") == [
            VENDOR_AWS, VENDOR_DATADOG, VENDOR_DATABRICKS
        ]

    def test_ignores_blank_entries(self):
        assert _parse_vendors("aws,, datadog") == [VENDOR_AWS, VENDOR_DATADOG]

    def test_unknown_raises(self):
        import argparse
        with pytest.raises(argparse.ArgumentTypeError):
            _parse_vendors("aws,gcp")
