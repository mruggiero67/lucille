"""Unit tests for vendor_spend.weekly_buckets (pure functions)."""

from datetime import date, datetime, timedelta

import pytest

from lucille.vendor_spend.weekly_buckets import (
    bucket_into_weeks,
    last_n_week_starts,
    monday_of,
    to_date,
    week_start_for,
)


class TestMondayOf:
    @pytest.mark.parametrize(
        "given,expected",
        [
            (date(2026, 5, 1), date(2026, 4, 27)),   # Friday
            (date(2026, 4, 27), date(2026, 4, 27)),  # Monday itself
            (date(2026, 5, 3), date(2026, 4, 27)),   # Sunday (end of same week)
            (date(2026, 5, 4), date(2026, 5, 4)),    # next Monday
        ],
    )
    def test_returns_monday_of_iso_week(self, given, expected):
        assert monday_of(given) == expected


class TestLastNWeekStarts:
    def test_six_weeks_from_friday(self):
        # today = Fri 2026-05-01, current week's Monday = 2026-04-27.
        # Excluded; last complete week starts 2026-04-20. Six back from there.
        result = last_n_week_starts(date(2026, 5, 1), 6)
        assert result == [
            date(2026, 3, 16),
            date(2026, 3, 23),
            date(2026, 3, 30),
            date(2026, 4, 6),
            date(2026, 4, 13),
            date(2026, 4, 20),
        ]

    def test_results_are_ascending(self):
        result = last_n_week_starts(date(2026, 5, 1), 6)
        assert result == sorted(result)

    def test_all_returned_dates_are_mondays(self):
        for d in last_n_week_starts(date(2026, 5, 1), 6):
            assert d.weekday() == 0

    def test_excludes_current_in_progress_week(self):
        today = date(2026, 5, 1)  # Friday
        this_monday = monday_of(today)
        assert this_monday not in last_n_week_starts(today, 6)

    def test_today_is_monday_excludes_that_monday(self):
        today = date(2026, 4, 27)
        result = last_n_week_starts(today, 1)
        assert result == [date(2026, 4, 20)]

    def test_n_one(self):
        assert last_n_week_starts(date(2026, 5, 1), 1) == [date(2026, 4, 20)]

    @pytest.mark.parametrize("n", [0, -1, -10])
    def test_non_positive_n_raises(self, n):
        with pytest.raises(ValueError):
            last_n_week_starts(date(2026, 5, 1), n)


class TestWeekStartFor:
    def setup_method(self):
        self.weeks = last_n_week_starts(date(2026, 5, 1), 6)
        # weeks span 2026-03-16 .. 2026-04-26 inclusive

    def test_inside_window_returns_correct_monday(self):
        # Wed 2026-04-15 -> Mon 2026-04-13
        assert week_start_for(date(2026, 4, 15), self.weeks) == date(2026, 4, 13)

    def test_first_day_of_window(self):
        assert week_start_for(date(2026, 3, 16), self.weeks) == date(2026, 3, 16)

    def test_last_day_of_window(self):
        # Sun 2026-04-26 is the final day of the last bucket
        assert week_start_for(date(2026, 4, 26), self.weeks) == date(2026, 4, 20)

    def test_before_window(self):
        assert week_start_for(date(2026, 3, 15), self.weeks) is None

    def test_after_window(self):
        # Mon 2026-04-27 is the start of the (excluded) current week
        assert week_start_for(date(2026, 4, 27), self.weeks) is None

    def test_empty_weeks(self):
        assert week_start_for(date(2026, 4, 15), []) is None


class TestBucketIntoWeeks:
    def setup_method(self):
        self.weeks = last_n_week_starts(date(2026, 5, 1), 6)

    def test_every_week_present_even_when_empty(self):
        result = bucket_into_weeks([], self.weeks)
        assert list(result.keys()) == self.weeks
        assert all(v == 0.0 for v in result.values())

    def test_sums_amounts_in_same_week(self):
        rows = [
            (date(2026, 4, 13), 10.0),  # Mon
            (date(2026, 4, 15), 5.5),   # Wed, same week
            (date(2026, 4, 19), 2.5),   # Sun, same week
        ]
        result = bucket_into_weeks(rows, self.weeks)
        assert result[date(2026, 4, 13)] == 18.0

    def test_separates_distinct_weeks(self):
        rows = [
            (date(2026, 4, 13), 10.0),
            (date(2026, 4, 20), 7.0),
        ]
        result = bucket_into_weeks(rows, self.weeks)
        assert result[date(2026, 4, 13)] == 10.0
        assert result[date(2026, 4, 20)] == 7.0

    def test_drops_out_of_window_rows(self):
        rows = [
            (date(2025, 1, 1), 999.0),   # way before
            (date(2026, 4, 27), 999.0),  # current (excluded) week
            (date(2026, 4, 15), 1.0),    # in-window
        ]
        result = bucket_into_weeks(rows, self.weeks)
        assert sum(result.values()) == 1.0

    def test_accepts_int_amounts(self):
        rows = [(date(2026, 4, 15), 3)]
        result = bucket_into_weeks(rows, self.weeks)
        assert result[date(2026, 4, 13)] == 3.0


class TestToDate:
    def test_from_date(self):
        assert to_date(date(2026, 4, 15)) == date(2026, 4, 15)

    def test_from_datetime(self):
        assert to_date(datetime(2026, 4, 15, 13, 30)) == date(2026, 4, 15)

    def test_from_iso_date_string(self):
        assert to_date("2026-04-15") == date(2026, 4, 15)

    def test_from_iso_datetime_string(self):
        assert to_date("2026-04-15T13:30:00") == date(2026, 4, 15)

    def test_from_iso_z_string(self):
        assert to_date("2026-04-15T13:30:00Z") == date(2026, 4, 15)

    def test_from_unsupported_type_raises(self):
        with pytest.raises(TypeError):
            to_date(12345)
