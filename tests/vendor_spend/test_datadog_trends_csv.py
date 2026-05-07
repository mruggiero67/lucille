"""Unit tests for vendor_spend.datadog_trends_csv (pure helpers)."""

from datetime import date

import pytest

from lucille.vendor_spend.datadog_trends_csv import (
    SOURCE_LABEL,
    VENDOR_LABEL,
    build_spend_rows_from_datadog_daily,
    parse_datadog_trends_csv,
    parse_short_date_headers,
)


class TestParseShortDateHeaders:
    def test_basic_no_year_wrap(self):
        dates, breaks = parse_short_date_headers(
            ["Mar 7", "Mar 8", "Mar 9"], base_year=2026
        )
        assert dates == [date(2026, 3, 7), date(2026, 3, 8), date(2026, 3, 9)]
        assert breaks == 0

    def test_crosses_month_boundary(self):
        dates, _ = parse_short_date_headers(
            ["Mar 30", "Mar 31", "Apr 1"], base_year=2026
        )
        assert dates == [date(2026, 3, 30), date(2026, 3, 31), date(2026, 4, 1)]

    def test_increments_year_when_month_wraps(self):
        dates, breaks = parse_short_date_headers(
            ["Dec 30", "Dec 31", "Jan 1", "Jan 2"], base_year=2025
        )
        assert dates == [
            date(2025, 12, 30),
            date(2025, 12, 31),
            date(2026, 1, 1),
            date(2026, 1, 2),
        ]
        assert breaks == 1

    def test_handles_two_year_wraps(self):
        # Synthetic: one full year of monthly headers crossing two boundaries
        headers = ["Dec 1", "Jan 1", "Dec 1", "Jan 1"]
        dates, breaks = parse_short_date_headers(headers, base_year=2024)
        assert [d.year for d in dates] == [2024, 2025, 2025, 2026]
        assert breaks == 2

    def test_strips_whitespace(self):
        dates, _ = parse_short_date_headers(["  Mar 7 ", "Mar 8"], base_year=2026)
        assert dates == [date(2026, 3, 7), date(2026, 3, 8)]

    def test_empty_header_raises(self):
        with pytest.raises(ValueError):
            parse_short_date_headers(["Mar 7", "", "Mar 9"], base_year=2026)


class TestParseDatadogTrendsCsv:
    def _csv(self, dates_csv: str, total_row: str, *extra_rows: str) -> str:
        # Build a minimal valid trends CSV. `dates_csv` is the comma-separated
        # date headers (no leading 'dimension,Total,').
        header = f"dimension,Total,{dates_csv}\n"
        return header + total_row + "\n" + "".join(r + "\n" for r in extra_rows)

    def test_extracts_total_row(self):
        body = self._csv(
            "Mar 7,Mar 8,Mar 9",
            "__TOTAL__,1500.0,495.97,487.90,533.92",
            "audit_trail,15.0,5,5,5",  # noise; should be ignored
        )
        result = parse_datadog_trends_csv(body, base_year=2026)
        assert result.daily_rows == [
            (date(2026, 3, 7), 495.97),
            (date(2026, 3, 8), 487.90),
            (date(2026, 3, 9), 533.92),
        ]

    def test_skips_empty_cells(self):
        body = self._csv(
            "May 4,May 5,May 6",
            "__TOTAL__,281.25,140.84,140.41,",
        )
        result = parse_datadog_trends_csv(body, base_year=2026)
        assert result.daily_rows == [
            (date(2026, 5, 4), 140.84),
            (date(2026, 5, 5), 140.41),
        ]
        assert result.skipped_columns == 1

    def test_zero_cells_are_kept(self):
        body = self._csv(
            "May 5,May 6",
            "__TOTAL__,140.41,140.41,0",
        )
        result = parse_datadog_trends_csv(body, base_year=2026)
        # 0 is a valid value, not a missing one
        assert result.daily_rows == [
            (date(2026, 5, 5), 140.41),
            (date(2026, 5, 6), 0.0),
        ]
        assert result.skipped_columns == 0

    def test_unparseable_cells_skipped(self):
        body = self._csv(
            "Mar 7,Mar 8",
            "__TOTAL__,1.0,oops,2.0",
        )
        result = parse_datadog_trends_csv(body, base_year=2026)
        assert result.daily_rows == [(date(2026, 3, 8), 2.0)]
        assert result.skipped_columns == 1

    def test_handles_year_wrap_in_headers(self):
        body = self._csv(
            "Dec 31,Jan 1",
            "__TOTAL__,2.0,1.0,1.0",
        )
        result = parse_datadog_trends_csv(body, base_year=2025)
        assert result.daily_rows == [
            (date(2025, 12, 31), 1.0),
            (date(2026, 1, 1), 1.0),
        ]
        assert result.inferred_year_breaks == 1

    def test_handles_utf8_bom(self):
        body = "\ufeff" + self._csv(
            "Mar 7", "__TOTAL__,1.0,5.0"
        )
        result = parse_datadog_trends_csv(body, base_year=2026)
        assert result.daily_rows == [(date(2026, 3, 7), 5.0)]

    def test_missing_total_row_raises(self):
        body = self._csv(
            "Mar 7,Mar 8",
            "audit_trail,1.0,1,1",  # no __TOTAL__
        )
        with pytest.raises(ValueError, match="__TOTAL__"):
            parse_datadog_trends_csv(body, base_year=2026)

    def test_bad_header_raises(self):
        body = "dimension\nfoo\n"
        with pytest.raises(ValueError, match="dimension,Total"):
            parse_datadog_trends_csv(body, base_year=2026)

    def test_wrong_first_column_raises(self):
        body = "category,Total,Mar 7\n__TOTAL__,1,1\n"
        with pytest.raises(ValueError, match="dimension,Total"):
            parse_datadog_trends_csv(body, base_year=2026)

    def test_empty_input(self):
        result = parse_datadog_trends_csv("", base_year=2026)
        assert result.daily_rows == []


class TestBuildSpendRowsFromDatadogDaily:
    def test_keeps_only_complete_weeks_at_or_after_from_date(self):
        # Sat-Sun (incomplete) + 2 full weeks + Mon (incomplete)
        rows = (
            [(date(2026, 3, 7), 100.0), (date(2026, 3, 8), 100.0)]
            + [(date(2026, 3, 9 + i), 100.0) for i in range(14)]
            + [(date(2026, 3, 23), 100.0)]
        )
        spend = build_spend_rows_from_datadog_daily(
            rows, "STAMP", from_date=date(2026, 3, 9)
        )
        assert [r.week_start for r in spend] == [
            date(2026, 3, 9),
            date(2026, 3, 16),
        ]
        assert all(r.amount_usd == pytest.approx(700.0) for r in spend)
        assert all(r.vendor == VENDOR_LABEL for r in spend)
        assert all(r.source == SOURCE_LABEL for r in spend)

    def test_drops_weeks_before_from_date(self):
        rows = [(date(2026, 3, 9 + i), 100.0) for i in range(21)]  # 3 full weeks
        spend = build_spend_rows_from_datadog_daily(
            rows, "STAMP", from_date=date(2026, 3, 16)
        )
        assert [r.week_start for r in spend] == [
            date(2026, 3, 16),
            date(2026, 3, 23),
        ]

    def test_amounts_rounded(self):
        rows = [(date(2026, 3, 9 + i), 1.234) for i in range(7)]
        spend = build_spend_rows_from_datadog_daily(
            rows, "STAMP", from_date=date(2026, 3, 9)
        )
        assert spend[0].amount_usd == pytest.approx(8.64)

    def test_empty_input_produces_no_rows(self):
        assert build_spend_rows_from_datadog_daily(
            [], "STAMP", from_date=date(2026, 3, 9)
        ) == []
