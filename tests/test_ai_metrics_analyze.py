"""Unit tests for lucille.ai_metrics.analyze."""

from datetime import datetime, timezone

import pytest

from context import lucille  # noqa: F401
from lucille.ai_metrics.analyze import (
    Ratio,
    ai_touched_share,
    compare_ticket_cycle_times,
    format_week,
    merge_rate,
    revert_rate,
    split_by_ai,
    summarize_bucket,
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
) -> PRRecord:
    return PRRecord(
        repo="org/repo",
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


class TestCompareTicketCycleTimes:
    def test_two_buckets_returned(self):
        ai, human = compare_ticket_cycle_times([1.0, 2.0, 3.0], [10.0, 20.0])
        assert ai.label == "ai" and ai.n == 3
        assert human.label == "human" and human.n == 2
