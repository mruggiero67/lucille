"""Unit tests for lucille.ai_metrics.analyze."""

from datetime import datetime, timezone

import pytest

from context import lucille  # noqa: F401
from lucille.ai_metrics.analyze import (
    Ratio,
    ai_touched_share,
    by_repo_summary,
    chart_worthy_weeks,
    compare_ticket_cycle_times,
    format_week,
    merge_rate,
    revert_rate,
    snap_to_monday,
    split_by_ai,
    summarize_bucket,
    top_repos_by_ai_share,
    week_start,
    weekly_trend,
)
from lucille.ai_metrics.fetch import PRRecord


def _pr(
    number: int,
    *,
    created: datetime,
    state: str = "closed",
    merged: bool = True,
    ai: bool = False,
    repo: str = "org/repo",
) -> PRRecord:
    return PRRecord(
        repo=repo,
        number=number,
        title=f"PR #{number}",
        author_login="alice",
        author_type="User",
        state=state,
        merged=merged,
        created_at=created,
        closed_at=created if state == "closed" else None,
        merged_at=created if merged else None,
        head_sha=f"sha{number:04d}",
        ai_touched=ai,
    )


# ---------------------------------------------------------------------------
# Ratio
# ---------------------------------------------------------------------------


class TestRatio:
    def test_value(self):
        assert Ratio(3, 6).value == 0.5

    def test_zero_denominator(self):
        assert Ratio(0, 0).value is None

    def test_as_percent(self):
        assert Ratio(1, 4).as_percent() == "25.0%"
        assert Ratio(0, 0).as_percent() == "n/a"


# ---------------------------------------------------------------------------
# Bucket helpers
# ---------------------------------------------------------------------------


class TestWeekBucketing:
    def test_week_start_is_monday(self):
        # 2026-04-15 is a Wednesday
        assert week_start(datetime(2026, 4, 15)).isoweekday() == 1

    def test_week_start_of_monday_is_same_day(self):
        assert week_start(datetime(2026, 4, 13)) == datetime(2026, 4, 13).date()

    def test_format_week(self):
        # 2026-04-15 is in ISO week 16 of 2026
        assert format_week(datetime(2026, 4, 15).date()) == "2026-W16"


# ---------------------------------------------------------------------------
# PR ratios
# ---------------------------------------------------------------------------


T0 = datetime(2026, 4, 15, tzinfo=timezone.utc)


class TestAiTouchedShare:
    def test_mixed(self):
        prs = [_pr(1, created=T0, ai=True), _pr(2, created=T0), _pr(3, created=T0, ai=True)]
        r = ai_touched_share(prs)
        assert r.numerator == 2 and r.denominator == 3

    def test_empty(self):
        assert ai_touched_share([]).value is None


class TestMergeRate:
    def test_open_prs_excluded_from_denominator(self):
        prs = [
            _pr(1, created=T0, state="open", merged=False),
            _pr(2, created=T0, state="closed", merged=True),
            _pr(3, created=T0, state="closed", merged=False),
        ]
        r = merge_rate(prs)
        assert r.numerator == 1 and r.denominator == 2

    def test_all_open(self):
        prs = [_pr(1, created=T0, state="open", merged=False)]
        assert merge_rate(prs).value is None


class TestRevertRate:
    def test_basic(self):
        prs = [_pr(1, created=T0, merged=True), _pr(2, created=T0, merged=True), _pr(3, created=T0, merged=True)]
        # PR 2 was reverted later
        assert revert_rate(prs, [2]).numerator == 1
        assert revert_rate(prs, [2]).denominator == 3

    def test_no_reverts(self):
        prs = [_pr(1, created=T0, merged=True)]
        assert revert_rate(prs, []).numerator == 0


class TestSplitByAi:
    def test_partitions(self):
        prs = [_pr(1, created=T0, ai=True), _pr(2, created=T0), _pr(3, created=T0, ai=True)]
        ai, human = split_by_ai(prs)
        assert [p.number for p in ai] == [1, 3]
        assert [p.number for p in human] == [2]


# ---------------------------------------------------------------------------
# Weekly trend
# ---------------------------------------------------------------------------


class TestWeeklyTrend:
    def test_two_weeks(self):
        # 2026-04-15 is week W16, 2026-04-22 is W17
        prs = [
            _pr(1, created=datetime(2026, 4, 15, tzinfo=timezone.utc), ai=True, merged=True),
            _pr(2, created=datetime(2026, 4, 16, tzinfo=timezone.utc), ai=False, merged=True),
            _pr(3, created=datetime(2026, 4, 22, tzinfo=timezone.utc), ai=True, merged=False, state="closed"),
        ]
        rows = weekly_trend(prs)
        assert len(rows) == 2
        w16, w17 = rows
        assert w16.week == "2026-W16"
        assert w16.prs_opened == 2
        assert w16.ai_touched == 1
        assert w16.ai_share == pytest.approx(0.5)
        assert w16.merged == 2
        assert w17.week == "2026-W17"
        assert w17.prs_opened == 1 and w17.ai_touched == 1
        assert w17.ai_merge_rate == pytest.approx(0.0)  # AI PR opened and closed unmerged

    def test_empty(self):
        assert weekly_trend([]) == []


# ---------------------------------------------------------------------------
# Ticket bucket
# ---------------------------------------------------------------------------


class TestSummarizeBucket:
    def test_median_mean_p90(self):
        b = summarize_bucket([1.0, 2.0, 3.0, 4.0, 100.0], "ai")
        assert b.n == 5
        assert b.median_days == 3.0
        assert b.mean_days == pytest.approx(22.0)
        assert b.p90_days == pytest.approx(61.6)  # linear interp between 4 and 100

    def test_empty(self):
        b = summarize_bucket([], "ai")
        assert b.n == 0 and b.median_days is None and b.p90_days is None

    def test_single_value(self):
        b = summarize_bucket([7.5], "ai")
        assert b.median_days == 7.5 and b.p90_days == 7.5


# ---------------------------------------------------------------------------
# Per-repo aggregation
# ---------------------------------------------------------------------------


class TestByRepoSummary:
    def test_one_row_per_repo(self):
        prs = [
            _pr(1, created=T0, ai=True,  repo="org/alpha"),
            _pr(2, created=T0, ai=False, repo="org/alpha"),
            _pr(3, created=T0, ai=True,  repo="org/beta"),
        ]
        rows = by_repo_summary(prs)
        assert [r.repo for r in rows] == ["org/beta", "org/alpha"]  # beta 100% > alpha 50%

    def test_ai_share_computed(self):
        prs = [
            _pr(1, created=T0, ai=True,  repo="org/alpha"),
            _pr(2, created=T0, ai=True,  repo="org/alpha"),
            _pr(3, created=T0, ai=False, repo="org/alpha"),
            _pr(4, created=T0, ai=False, repo="org/beta"),
        ]
        rows = {r.repo: r for r in by_repo_summary(prs)}
        assert rows["org/alpha"].ai_share == pytest.approx(2/3)
        assert rows["org/alpha"].ai_touched == 2
        assert rows["org/alpha"].human_only == 1
        assert rows["org/beta"].ai_share == 0.0

    def test_sort_tiebreak_by_ai_touched_count(self):
        # Two repos, both 100% AI; the busier one should come first.
        prs = [
            _pr(1, created=T0, ai=True, repo="org/small"),
            _pr(2, created=T0, ai=True, repo="org/big"),
            _pr(3, created=T0, ai=True, repo="org/big"),
            _pr(4, created=T0, ai=True, repo="org/big"),
        ]
        rows = by_repo_summary(prs)
        assert [r.repo for r in rows] == ["org/big", "org/small"]

    def test_merge_rate_per_repo(self):
        prs = [
            _pr(1, created=T0, ai=True,  merged=True,  repo="org/alpha"),
            _pr(2, created=T0, ai=False, merged=False, repo="org/alpha"),
        ]
        row = by_repo_summary(prs)[0]
        # 1 merged / 2 closed
        assert row.merge_rate == pytest.approx(0.5)
        assert row.merged == 1
        assert row.ai_merged == 1

    def test_empty_input(self):
        assert by_repo_summary([]) == []


class TestTopReposByAiShare:
    def test_min_prs_filters_out_noise(self):
        prs = [
            _pr(1, created=T0, ai=True, repo="org/tiny"),          # 1 PR, 100% AI
            *[_pr(i, created=T0, ai=(i % 2 == 0), repo="org/big")  # 10 PRs, 50% AI
              for i in range(10, 20)],
        ]
        rows = by_repo_summary(prs)
        top = top_repos_by_ai_share(rows, min_prs=5)
        # tiny is filtered out, only big remains
        assert [r.repo for r in top] == ["org/big"]

    def test_limit(self):
        prs = []
        for r in range(15):
            prs.extend(
                _pr(r * 100 + i, created=T0, ai=(i < 3), repo=f"org/r{r:02d}")
                for i in range(5)
            )
        rows = by_repo_summary(prs)
        top = top_repos_by_ai_share(rows, min_prs=5, limit=10)
        assert len(top) == 10

    def test_defaults(self):
        # A repo with exactly 5 PRs should qualify at the default threshold.
        prs = [_pr(i, created=T0, ai=True, repo="org/x") for i in range(5)]
        rows = by_repo_summary(prs)
        assert len(top_repos_by_ai_share(rows)) == 1


class TestCompareTicketCycleTimes:
    def test_two_buckets_returned(self):
        ai, human = compare_ticket_cycle_times([1.0, 2.0, 3.0], [10.0, 20.0])
        assert ai.label == "ai" and ai.n == 3
        assert human.label == "human" and human.n == 2


# ---------------------------------------------------------------------------
# snap_to_monday
# ---------------------------------------------------------------------------


class TestSnapToMonday:
    def test_monday_snaps_to_itself_midnight(self):
        # 2026-04-13 is a Monday.
        d = datetime(2026, 4, 13, 15, 30, tzinfo=timezone.utc)
        out = snap_to_monday(d)
        assert out.year == 2026 and out.month == 4 and out.day == 13
        assert out.hour == 0 and out.minute == 0

    def test_sunday_snaps_back_six_days_to_previous_monday(self):
        # 2026-04-19 is a Sunday. Snap back to 2026-04-13 (Monday).
        d = datetime(2026, 4, 19, 23, 59, tzinfo=timezone.utc)
        out = snap_to_monday(d)
        assert out.day == 13

    def test_saturday_snaps_back_five_days(self):
        # 2026-04-18 is a Saturday.
        d = datetime(2026, 4, 18, 10, tzinfo=timezone.utc)
        out = snap_to_monday(d)
        assert out.day == 13

    def test_preserves_timezone(self):
        d = datetime(2026, 4, 18, tzinfo=timezone.utc)
        assert snap_to_monday(d).tzinfo == timezone.utc

    def test_only_snaps_backward(self):
        # No matter what day of the week, the snapped date is <= the input.
        for day in range(13, 20):  # Mon 2026-04-13 through Sun 2026-04-19
            d = datetime(2026, 4, day, 12, tzinfo=timezone.utc)
            assert snap_to_monday(d).date() <= d.date()


# ---------------------------------------------------------------------------
# chart_worthy_weeks
# ---------------------------------------------------------------------------


class TestChartWorthyWeeks:
    def _rows_with_counts(self, counts):
        """Build weekly rows via weekly_trend from synthetic PR counts.

        ``counts[i]`` is the number of PRs to place in the i-th week,
        starting from Monday 2026-04-13. Each PR is placed at noon UTC
        on that week's Monday to keep the fixture simple.
        """
        from datetime import timedelta
        first_monday = datetime(2026, 4, 13, 12, tzinfo=timezone.utc)
        prs = []
        for week_i, n in enumerate(counts):
            when = first_monday + timedelta(weeks=week_i)
            for _ in range(n):
                prs.append(_pr(number=len(prs), repo="r", created=when))
        return weekly_trend(prs)

    def test_filters_out_low_count_weeks(self):
        rows = self._rows_with_counts([1, 20, 3, 15])
        kept = chart_worthy_weeks(rows, min_prs=10)
        assert [r.prs_opened for r in kept] == [20, 15]

    def test_threshold_boundary_is_inclusive(self):
        rows = self._rows_with_counts([9, 10, 11])
        kept = chart_worthy_weeks(rows, min_prs=10)
        assert [r.prs_opened for r in kept] == [10, 11]

    def test_zero_threshold_keeps_all(self):
        rows = self._rows_with_counts([1, 2, 3])
        assert len(chart_worthy_weeks(rows, min_prs=0)) == 3

    def test_empty_input_returns_empty_list(self):
        assert chart_worthy_weeks([], min_prs=5) == []

    def test_preserves_row_order(self):
        # weekly_trend returns chronological order; the filter must not
        # perturb it.
        rows = self._rows_with_counts([15, 3, 20, 5, 30])
        kept = chart_worthy_weeks(rows, min_prs=10)
        assert [r.prs_opened for r in kept] == [15, 20, 30]
