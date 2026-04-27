"""Unit tests for lucille.lead_time.aggregations (pure functions only)."""
from datetime import datetime, timezone

import pandas as pd
from context import lucille  # noqa: F401
from lucille.lead_time.aggregations import (
    categorize_performance,
    compute_repo_stats,
    compute_weekly_stats,
    filter_valid_records,
    week_label,
)


def _dt(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, 12, 0, tzinfo=timezone.utc)


def _make_df(records):
    return pd.DataFrame(
        records,
        columns=["repo", "deployment_id", "deployed_at", "lead_time_hours", "jira_project"],
    )


class TestFilterValidRecords:
    def test_removes_nulls(self):
        df = _make_df([
            ("r", "d1", _dt(2026, 1, 1), None, "OOT"),
            ("r", "d2", _dt(2026, 1, 2), 24.0, "OOT"),
        ])
        clean, excluded = filter_valid_records(df)
        assert len(clean) == 1
        assert excluded == 1

    def test_removes_negatives(self):
        df = _make_df([
            ("r", "d1", _dt(2026, 1, 1), -1.0, "OOT"),
            ("r", "d2", _dt(2026, 1, 2), 24.0, "OOT"),
        ])
        clean, excluded = filter_valid_records(df)
        assert len(clean) == 1
        assert excluded == 1

    def test_removes_over_365_days(self):
        df = _make_df([
            ("r", "d1", _dt(2026, 1, 1), 365 * 24 + 1.0, "OOT"),
            ("r", "d2", _dt(2026, 1, 2), 24.0, "OOT"),
        ])
        clean, excluded = filter_valid_records(df)
        assert len(clean) == 1
        assert excluded == 1

    def test_zero_hours_is_valid(self):
        df = _make_df([("r", "d1", _dt(2026, 1, 1), 0.0, "OOT")])
        clean, excluded = filter_valid_records(df)
        assert len(clean) == 1
        assert excluded == 0

    def test_exactly_365_days_is_valid(self):
        df = _make_df([("r", "d1", _dt(2026, 1, 1), 365 * 24.0, "OOT")])
        clean, excluded = filter_valid_records(df)
        assert len(clean) == 1
        assert excluded == 0

    def test_excluded_count_is_accurate(self):
        df = _make_df([
            ("r", "d1", _dt(2026, 1, 1), None,   "OOT"),
            ("r", "d2", _dt(2026, 1, 1), -5.0,   "OOT"),
            ("r", "d3", _dt(2026, 1, 1), 24.0,   "OOT"),
        ])
        clean, excluded = filter_valid_records(df)
        assert excluded == 2
        assert len(clean) == 1

    def test_all_valid_returns_original_length(self):
        df = _make_df([
            ("r", "d1", _dt(2026, 1, 1), 12.0, "OOT"),
            ("r", "d2", _dt(2026, 1, 2), 48.0, "OOT"),
        ])
        clean, excluded = filter_valid_records(df)
        assert excluded == 0
        assert len(clean) == 2


class TestCategorizePerformance:
    def test_fast_at_zero(self):
        assert categorize_performance(0.0) == "Fast"

    def test_fast_just_below_3(self):
        assert categorize_performance(2.9) == "Fast"

    def test_normal_at_3(self):
        assert categorize_performance(3.0) == "Normal"

    def test_normal_just_below_7(self):
        assert categorize_performance(6.9) == "Normal"

    def test_slow_at_7(self):
        assert categorize_performance(7.0) == "Slow"

    def test_slow_just_below_14(self):
        assert categorize_performance(13.9) == "Slow"

    def test_critical_at_14(self):
        assert categorize_performance(14.0) == "Critical"

    def test_critical_at_30(self):
        assert categorize_performance(30.0) == "Critical"


class TestWeekLabel:
    def test_starts_with_week_of(self):
        assert week_label(_dt(2026, 1, 7)).startswith("Week of ")

    def test_sunday_and_wednesday_same_week(self):
        # 2026-01-04 is Sunday; 2026-01-07 is the Wednesday of the same week
        assert week_label(_dt(2026, 1, 4)) == week_label(_dt(2026, 1, 7))

    def test_adjacent_sundays_differ(self):
        assert week_label(_dt(2026, 1, 4)) != week_label(_dt(2026, 1, 11))


class TestComputeWeeklyStats:
    def _df(self):
        return _make_df([
            # Week of 01/04 (Sunday Jan 4): Jan 5 (Mon) and Jan 6 (Tue)
            ("r", "d1", _dt(2026, 1, 5), 24.0, "OOT"),
            ("r", "d2", _dt(2026, 1, 6), 48.0, "OOT"),
            # Week of 01/11 (Sunday Jan 11): Jan 12 (Mon)
            ("r", "d3", _dt(2026, 1, 12), 72.0, "OOT"),
        ])

    def test_produces_two_weeks(self):
        result = compute_weekly_stats(self._df())
        assert len(result) == 2

    def test_sorted_chronologically(self):
        result = compute_weekly_stats(self._df())
        assert result.iloc[0]["week_start"] < result.iloc[1]["week_start"]

    def test_first_week_change_count(self):
        result = compute_weekly_stats(self._df())
        assert result.iloc[0]["change_count"] == 2

    def test_second_week_change_count(self):
        result = compute_weekly_stats(self._df())
        assert result.iloc[1]["change_count"] == 1

    def test_median_days_first_week(self):
        result = compute_weekly_stats(self._df())
        # median of [24, 48] hours = 36h = 1.5 days
        assert result.iloc[0]["median_days"] == 1.5

    def test_median_days_second_week(self):
        result = compute_weekly_stats(self._df())
        # single record 72h = 3.0 days
        assert result.iloc[1]["median_days"] == 3.0

    def test_week_label_format(self):
        result = compute_weekly_stats(self._df())
        for label in result["week_label"]:
            assert label.startswith("Week of ")


class TestComputeRepoStats:
    def _df(self):
        return _make_df([
            ("repo-a", "d1", _dt(2026, 1, 5), 24.0,  "OOT"),
            ("repo-a", "d1", _dt(2026, 1, 5), 48.0,  "OOT"),
            ("repo-b", "d2", _dt(2026, 1, 6), 168.0, "SSJ"),
        ])

    def test_one_row_per_repo(self):
        result = compute_repo_stats(self._df())
        data_rows = result[result["repository"] != "TOTAL"]
        assert len(data_rows) == 2

    def test_totals_row_appended(self):
        result = compute_repo_stats(self._df())
        assert "TOTAL" in result["repository"].values

    def test_sorted_by_changes_count_desc(self):
        result = compute_repo_stats(self._df())
        data_rows = result[result["repository"] != "TOTAL"]
        counts = data_rows["changes_count"].tolist()
        assert counts == sorted(counts, reverse=True)

    def test_repo_a_median(self):
        result = compute_repo_stats(self._df())
        row = result[result["repository"] == "repo-a"].iloc[0]
        # median of [24, 48] = 36h = 1.5 days
        assert row["median_lead_time_days"] == 1.5

    def test_performance_categories_are_valid(self):
        valid = {"Fast", "Normal", "Slow", "Critical"}
        result = compute_repo_stats(self._df())
        for cat in result[result["repository"] != "TOTAL"]["performance_category"]:
            assert cat in valid

    def test_jira_projects_listed(self):
        result = compute_repo_stats(self._df())
        repo_a = result[result["repository"] == "repo-a"].iloc[0]
        assert "OOT" in repo_a["jira_projects"]

    def test_totals_changes_count(self):
        result = compute_repo_stats(self._df())
        total = result[result["repository"] == "TOTAL"].iloc[0]
        assert total["changes_count"] == 3
