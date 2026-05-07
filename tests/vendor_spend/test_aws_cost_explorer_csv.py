"""Unit tests for vendor_spend.aws_cost_explorer_csv (pure helpers)."""

from datetime import date

import pytest

from lucille.vendor_spend.aws_cost_explorer_csv import (
    SOURCE_LABEL,
    VENDOR_LABEL,
    build_spend_rows_from_aws_daily,
    parse_aws_explorer_csv,
)
from lucille.vendor_spend.weekly_buckets import complete_week_starts


HEADER = (
    '"Service","EC2-Other($)","S3($)","Total costs($)"'
)
SUMMARY = '"Service total","100.0","50.0","150.0"'


def _csv(*rows: str, with_bom: bool = False) -> str:
    body = "\n".join([HEADER, SUMMARY, *rows]) + "\n"
    return ("\ufeff" + body) if with_bom else body


class TestParseAwsExplorerCsv:
    def test_basic(self):
        body = _csv(
            '"2026-03-09","679.43","138.81","2383.48"',
            '"2026-03-10","705.99","149.00","2474.34"',
        )
        result = parse_aws_explorer_csv(body)
        assert result.daily_rows == [
            (date(2026, 3, 9), 2383.48),
            (date(2026, 3, 10), 2474.34),
        ]
        assert result.skipped_rows == 1  # the "Service total" row

    def test_handles_utf8_bom(self):
        body = _csv('"2026-03-09","679.43","138.81","2383.48"', with_bom=True)
        result = parse_aws_explorer_csv(body)
        assert result.daily_rows == [(date(2026, 3, 9), 2383.48)]

    def test_skips_service_total_row(self):
        result = parse_aws_explorer_csv(_csv())  # only header + summary, no data
        assert result.daily_rows == []
        assert result.skipped_rows == 1

    def test_empty_total_treated_as_zero(self):
        body = _csv('"2026-03-09","","",""')
        result = parse_aws_explorer_csv(body)
        assert result.daily_rows == [(date(2026, 3, 9), 0.0)]

    def test_blank_rows_are_skipped(self):
        body = (
            HEADER + "\n"
            + SUMMARY + "\n"
            + "\n"
            + '"2026-03-09","1","2","3.5"\n'
            + "\n"
            + '"2026-03-10","1","2","4.5"\n'
        )
        result = parse_aws_explorer_csv(body)
        assert result.daily_rows == [
            (date(2026, 3, 9), 3.5),
            (date(2026, 3, 10), 4.5),
        ]

    def test_header_with_no_total_column_raises(self):
        body = '"Service","EC2-Other($)"\n"2026-03-09","1.0"\n'
        with pytest.raises(ValueError, match="Total costs"):
            parse_aws_explorer_csv(body)

    def test_total_column_without_dollar_sign_suffix_still_found(self):
        # Tolerant matching: AWS sometimes drops the "($)" suffix.
        body = (
            '"Service","EC2-Other","Total costs"\n'
            '"Service total","100","150"\n'
            '"2026-03-09","679.43","2383.48"\n'
        )
        result = parse_aws_explorer_csv(body)
        assert result.daily_rows == [(date(2026, 3, 9), 2383.48)]

    def test_empty_input(self):
        result = parse_aws_explorer_csv("")
        assert result.daily_rows == []
        assert result.skipped_rows == 0

    def test_unparseable_total_skips_row(self):
        body = _csv(
            '"2026-03-09","x","y","not-a-number"',
            '"2026-03-10","1","2","3.5"',
        )
        result = parse_aws_explorer_csv(body)
        assert result.daily_rows == [(date(2026, 3, 10), 3.5)]


class TestCompleteWeekStarts:
    def test_only_full_mon_sun_weeks_kept(self):
        # Mon 2026-03-09 .. Sun 2026-03-15 = 7 days (complete)
        # Mon 2026-03-16 .. Tue 2026-03-17 = 2 days (incomplete)
        rows = [(date(2026, 3, 9 + i), 1.0) for i in range(9)]  # 9 days
        weeks = complete_week_starts(rows)
        assert weeks == [date(2026, 3, 9)]

    def test_two_full_weeks(self):
        rows = [(date(2026, 3, 9 + i), 1.0) for i in range(14)]
        weeks = complete_week_starts(rows)
        assert weeks == [date(2026, 3, 9), date(2026, 3, 16)]

    def test_returns_empty_when_no_full_week(self):
        rows = [(date(2026, 3, 11), 1.0), (date(2026, 3, 12), 1.0)]
        assert complete_week_starts(rows) == []

    def test_partial_weeks_at_both_ends_dropped(self):
        # Sat-Sun of one week, full middle week, Mon-Tue of next week.
        rows = (
            [(date(2026, 3, 7), 1.0), (date(2026, 3, 8), 1.0)]
            + [(date(2026, 3, 9 + i), 1.0) for i in range(7)]
            + [(date(2026, 3, 16), 1.0), (date(2026, 3, 17), 1.0)]
        )
        assert complete_week_starts(rows) == [date(2026, 3, 9)]

    def test_duplicate_days_count_once(self):
        rows = [(date(2026, 3, 9 + i), 1.0) for i in range(7)]
        rows += [(date(2026, 3, 9), 1.0)]  # duplicate Monday
        # Still 7 distinct days, so the week is "complete"
        assert complete_week_starts(rows) == [date(2026, 3, 9)]


class TestBuildSpendRowsFromAwsDaily:
    def test_emits_one_row_per_complete_week(self):
        rows = [(date(2026, 3, 9 + i), 100.0) for i in range(14)]
        spend = build_spend_rows_from_aws_daily(rows, "STAMP")
        assert len(spend) == 2
        assert all(r.vendor == VENDOR_LABEL for r in spend)
        assert all(r.source == SOURCE_LABEL for r in spend)
        assert all(r.fetched_at == "STAMP" for r in spend)

    def test_amounts_are_summed_and_rounded(self):
        # 7 days * 1.234 = 8.638 -> rounded to 2dp
        rows = [(date(2026, 3, 9 + i), 1.234) for i in range(7)]
        spend = build_spend_rows_from_aws_daily(rows, "STAMP")
        assert spend[0].week_start == date(2026, 3, 9)
        assert spend[0].amount_usd == pytest.approx(8.64)

    def test_drops_partial_weeks(self):
        # Full week + 3 extra days (Mon-Wed of next week) => 1 row, not 2
        rows = [(date(2026, 3, 9 + i), 100.0) for i in range(10)]
        spend = build_spend_rows_from_aws_daily(rows, "STAMP")
        assert [r.week_start for r in spend] == [date(2026, 3, 9)]
        assert spend[0].amount_usd == pytest.approx(700.0)

    def test_empty_input_produces_no_rows(self):
        assert build_spend_rows_from_aws_daily([], "STAMP") == []
