"""Unit tests for vendor_spend.databricks_console_csv (pure helpers)."""

from datetime import date

import pytest

from lucille.vendor_spend.databricks_console_csv import (
    SOURCE_LABEL,
    VENDOR_LABEL,
    build_spend_rows_from_databricks_weekly,
    filter_from,
    parse_databricks_console_csv,
)


HEADER = "custom_tag_key_value_pairs,time_key,sum(usage_usd)\n"


class TestParseDatabricksConsoleCsv:
    def test_basic_unsorted_input_is_sorted(self):
        body = (
            HEADER
            + "<MISMATCH>,2026-04-06 00:00:00,3126.91\n"
            + "<MISMATCH>,2026-03-09 00:00:00,1000.00\n"
            + "<MISMATCH>,2026-03-23 00:00:00,2000.00\n"
        )
        result = parse_databricks_console_csv(body)
        assert result.weekly_rows == [
            (date(2026, 3, 9),  1000.00),
            (date(2026, 3, 23), 2000.00),
            (date(2026, 4, 6),  3126.91),
        ]
        assert result.skipped_rows == 0
        assert result.realigned_rows == 0

    def test_ignores_first_column(self):
        # First column varies; result should be identical.
        body = (
            HEADER
            + "<MISMATCH>,2026-03-09 00:00:00,1000.0\n"
            + "team=foo,2026-03-16 00:00:00,2000.0\n"
            + ",2026-03-23 00:00:00,3000.0\n"
        )
        result = parse_databricks_console_csv(body)
        assert [r[1] for r in result.weekly_rows] == [1000.0, 2000.0, 3000.0]

    def test_handles_date_only_time_key(self):
        body = HEADER + "<MISMATCH>,2026-03-09,1000.0\n"
        result = parse_databricks_console_csv(body)
        assert result.weekly_rows == [(date(2026, 3, 9), 1000.0)]

    def test_handles_utf8_bom(self):
        body = "\ufeff" + HEADER + "<MISMATCH>,2026-03-09,1000.0\n"
        result = parse_databricks_console_csv(body)
        assert result.weekly_rows == [(date(2026, 3, 9), 1000.0)]

    def test_realigns_non_monday_to_monday(self):
        # 2026-03-11 is a Wednesday; should snap to Mon 2026-03-09
        body = HEADER + "<MISMATCH>,2026-03-11,1000.0\n"
        result = parse_databricks_console_csv(body)
        assert result.weekly_rows == [(date(2026, 3, 9), 1000.0)]
        assert result.realigned_rows == 1

    def test_sums_duplicates_and_realigned_collisions(self):
        body = (
            HEADER
            + "<MISMATCH>,2026-03-09,500.0\n"
            + "<MISMATCH>,2026-03-09,300.0\n"
            + "<MISMATCH>,2026-03-11,200.0\n"  # snaps to 2026-03-09
        )
        result = parse_databricks_console_csv(body)
        assert result.weekly_rows == [(date(2026, 3, 9), 1000.0)]
        assert result.realigned_rows == 1

    def test_skips_rows_with_unparseable_date(self):
        body = (
            HEADER
            + "<MISMATCH>,not-a-date,123\n"
            + "<MISMATCH>,2026-03-09,1000.0\n"
        )
        result = parse_databricks_console_csv(body)
        assert result.weekly_rows == [(date(2026, 3, 9), 1000.0)]
        assert result.skipped_rows == 1

    def test_skips_rows_with_blank_time_key(self):
        body = HEADER + "<MISMATCH>,,500.0\n<MISMATCH>,2026-03-09,1000.0\n"
        result = parse_databricks_console_csv(body)
        assert result.weekly_rows == [(date(2026, 3, 9), 1000.0)]
        assert result.skipped_rows == 1

    def test_skips_rows_with_unparseable_usd(self):
        body = (
            HEADER
            + "<MISMATCH>,2026-03-09,oops\n"
            + "<MISMATCH>,2026-03-16,2000.0\n"
        )
        result = parse_databricks_console_csv(body)
        assert result.weekly_rows == [(date(2026, 3, 16), 2000.0)]
        assert result.skipped_rows == 1

    def test_treats_blank_usd_as_zero(self):
        body = HEADER + "<MISMATCH>,2026-03-09,\n"
        result = parse_databricks_console_csv(body)
        assert result.weekly_rows == [(date(2026, 3, 9), 0.0)]

    def test_missing_required_columns_raises(self):
        body = "custom_tag_key_value_pairs,time_key\n<MISMATCH>,2026-03-09\n"
        with pytest.raises(ValueError, match="missing required columns"):
            parse_databricks_console_csv(body)

    def test_empty_input(self):
        result = parse_databricks_console_csv("")
        assert result.weekly_rows == []
        assert result.skipped_rows == 0


class TestFilterFrom:
    def test_inclusive_lower_bound(self):
        rows = [
            (date(2026, 3, 2),  100.0),
            (date(2026, 3, 9),  200.0),
            (date(2026, 3, 16), 300.0),
        ]
        assert filter_from(rows, date(2026, 3, 9)) == [
            (date(2026, 3, 9),  200.0),
            (date(2026, 3, 16), 300.0),
        ]

    def test_drops_all_when_after_window(self):
        rows = [(date(2026, 3, 9), 100.0)]
        assert filter_from(rows, date(2026, 4, 1)) == []

    def test_keeps_all_when_before_window(self):
        rows = [(date(2026, 3, 9), 100.0), (date(2026, 3, 16), 200.0)]
        assert filter_from(rows, date(2026, 1, 1)) == rows


class TestBuildSpendRowsFromDatabricksWeekly:
    def test_emits_one_row_per_week(self):
        rows = [(date(2026, 3, 9), 1000.0), (date(2026, 3, 16), 2000.0)]
        spend = build_spend_rows_from_databricks_weekly(rows, "STAMP")
        assert len(spend) == 2
        assert all(r.vendor == VENDOR_LABEL for r in spend)
        assert all(r.source == SOURCE_LABEL for r in spend)
        assert all(r.fetched_at == "STAMP" for r in spend)

    def test_amounts_are_rounded(self):
        rows = [(date(2026, 3, 9), 1234.5678)]
        spend = build_spend_rows_from_databricks_weekly(rows, "STAMP")
        assert spend[0].amount_usd == pytest.approx(1234.57)

    def test_empty_input_produces_no_rows(self):
        assert build_spend_rows_from_databricks_weekly([], "STAMP") == []
