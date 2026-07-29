"""Tests for lucille.github.pr_review_latency.

Follows the mock-the-network pattern from tests/test_github_utils.py: pure
transforms are exercised directly, fetchers get their `paginate` / session
calls patched.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from context import lucille  # noqa: F401
from lucille.github import pr_review_latency as prl


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestReviewLatencyHours:
    def test_positive(self):
        a = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
        b = datetime(2025, 1, 2, 12, 0, tzinfo=timezone.utc)
        assert prl.review_latency_hours(a, b) == pytest.approx(24.0)

    def test_subhour(self):
        a = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
        b = datetime(2025, 1, 1, 12, 30, tzinfo=timezone.utc)
        assert prl.review_latency_hours(a, b) == pytest.approx(0.5)


class TestIsBotLogin:
    def test_none(self):
        assert prl.is_bot_login(None, None, [])

    def test_user_type_bot(self):
        assert prl.is_bot_login("someone", "Bot", [])

    def test_bracket_bot_suffix(self):
        assert prl.is_bot_login("copilot-swe-agent[bot]", "User", [])

    def test_configured_login(self):
        assert prl.is_bot_login("dependabot", "User", ["dependabot"])

    def test_human(self):
        assert not prl.is_bot_login("alice", "User", ["dependabot"])


class TestPickFirstReview:
    BOTS = ["dependabot"]

    def _review(self, login, submitted_at, user_type="User"):
        return {
            "user": {"login": login, "type": user_type},
            "submitted_at": submitted_at,
        }

    def test_earliest_wins(self):
        reviews = [
            self._review("bob", "2025-01-02T10:00:00Z"),
            self._review("carol", "2025-01-01T10:00:00Z"),
            self._review("dave", "2025-01-03T10:00:00Z"),
        ]
        at, login = prl.pick_first_review(reviews, author="alice", bot_logins=self.BOTS)
        assert login == "carol"
        assert at == datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)

    def test_ignores_author(self):
        reviews = [
            self._review("alice", "2025-01-01T10:00:00Z"),
            self._review("bob", "2025-01-02T10:00:00Z"),
        ]
        _, login = prl.pick_first_review(reviews, author="alice", bot_logins=self.BOTS)
        assert login == "bob"

    def test_ignores_bots(self):
        reviews = [
            self._review("dependabot", "2025-01-01T09:00:00Z"),
            self._review("copilot[bot]", "2025-01-01T09:30:00Z", user_type="Bot"),
            self._review("bob", "2025-01-01T10:00:00Z"),
        ]
        _, login = prl.pick_first_review(reviews, author="alice", bot_logins=self.BOTS)
        assert login == "bob"

    def test_no_qualifying_reviews(self):
        reviews = [
            self._review("alice", "2025-01-01T10:00:00Z"),  # author
            self._review("dependabot", "2025-01-01T11:00:00Z"),  # bot
        ]
        at, login = prl.pick_first_review(reviews, author="alice", bot_logins=self.BOTS)
        assert at is None and login is None

    def test_empty_list(self):
        assert prl.pick_first_review([], author="alice", bot_logins=self.BOTS) == (
            None,
            None,
        )

    def test_missing_submitted_at_skipped(self):
        reviews = [
            {"user": {"login": "bob", "type": "User"}, "submitted_at": None},
            self._review("carol", "2025-01-02T10:00:00Z"),
        ]
        _, login = prl.pick_first_review(reviews, author="alice", bot_logins=self.BOTS)
        assert login == "carol"


# ---------------------------------------------------------------------------
# compute_weekly_stats
# ---------------------------------------------------------------------------


class TestComputeWeeklyStats:
    def test_empty(self):
        df = prl.compute_weekly_stats([])
        assert df.empty
        assert list(df.columns) == [
            "week_start",
            "week_label",
            "n",
            "awaiting_review",
            "p50_hours",
            "p90_hours",
        ]

    def test_pools_across_repos_and_percentiles(self):
        # Two PRs in the same Sunday-week across two repos.
        # 2025-01-05 is a Sunday, 2025-01-06 is a Monday → both in same bucket.
        rows = [
            {
                "repo": "a",
                "created_at": "2025-01-06T09:00:00+00:00",
                "review_latency_hours": 4.0,
            },
            {
                "repo": "b",
                "created_at": "2025-01-07T09:00:00+00:00",
                "review_latency_hours": 20.0,
            },
        ]
        df = prl.compute_weekly_stats(rows)
        assert len(df) == 1
        row = df.iloc[0]
        assert row["n"] == 2
        assert row["awaiting_review"] == 0
        # p50 of [4, 20] = 12 ; p90 = 18.4
        assert row["p50_hours"] == pytest.approx(12.0)
        assert row["p90_hours"] == pytest.approx(18.4)

    def test_unreviewed_counted_as_awaiting(self):
        rows = [
            {
                "repo": "a",
                "created_at": "2025-01-06T09:00:00+00:00",
                "review_latency_hours": 10.0,
            },
            {
                "repo": "a",
                "created_at": "2025-01-07T09:00:00+00:00",
                "review_latency_hours": None,
            },
        ]
        df = prl.compute_weekly_stats(rows)
        assert len(df) == 1
        row = df.iloc[0]
        assert row["n"] == 1
        assert row["awaiting_review"] == 1
        assert row["p50_hours"] == pytest.approx(10.0)  # single-record fallback
        assert row["p90_hours"] == pytest.approx(10.0)

    def test_sunday_bucketing_separates_weeks(self):
        # 2025-01-04 (Sat) → week_start 2024-12-29 (Sun)
        # 2025-01-05 (Sun) → week_start 2025-01-05
        rows = [
            {
                "repo": "a",
                "created_at": "2025-01-04T12:00:00+00:00",
                "review_latency_hours": 1.0,
            },
            {
                "repo": "a",
                "created_at": "2025-01-05T12:00:00+00:00",
                "review_latency_hours": 2.0,
            },
        ]
        df = prl.compute_weekly_stats(rows)
        assert len(df) == 2
        # Sorted chronologically.
        assert df.iloc[0]["week_start"].isoformat() == "2024-12-29"
        assert df.iloc[1]["week_start"].isoformat() == "2025-01-05"

    def test_accepts_datetime_objects_for_created_at(self):
        rows = [
            {
                "repo": "a",
                "created_at": datetime(2025, 1, 6, 9, tzinfo=timezone.utc),
                "review_latency_hours": 6.0,
            },
        ]
        df = prl.compute_weekly_stats(rows)
        assert len(df) == 1
        assert df.iloc[0]["p50_hours"] == pytest.approx(6.0)


# ---------------------------------------------------------------------------
# fetch_merged_prs — window stop and merged-only filter
# ---------------------------------------------------------------------------


class TestFetchMergedPRs:
    @patch("lucille.github.pr_review_latency.paginate")
    def test_stops_at_cutoff_and_filters_unmerged(self, mock_paginate):
        # Newest-first sequence spanning cutoff. The paginator should be
        # short-circuited by the caller once we cross the cutoff.
        cutoff = datetime(2025, 1, 15, tzinfo=timezone.utc)
        prs = [
            {
                "number": 3,
                "created_at": "2025-01-20T00:00:00Z",
                "merged_at": "2025-01-21T00:00:00Z",
            },
            {
                "number": 2,
                "created_at": "2025-01-18T00:00:00Z",
                "merged_at": None,
            },  # closed, unmerged
            {
                "number": 1,
                "created_at": "2025-01-16T00:00:00Z",
                "merged_at": "2025-01-17T00:00:00Z",
            },
            # Below the cutoff — must be excluded even though newer PRs above.
            {
                "number": 0,
                "created_at": "2025-01-10T00:00:00Z",
                "merged_at": "2025-01-11T00:00:00Z",
            },
        ]
        mock_paginate.return_value = iter(prs)

        out = prl.fetch_merged_prs(MagicMock(), "org", "repo", cutoff)
        numbers = [p["number"] for p in out]
        assert numbers == [3, 1]  # #2 unmerged, #0 pre-cutoff

    @patch("lucille.github.pr_review_latency.paginate")
    def test_empty(self, mock_paginate):
        mock_paginate.return_value = iter([])
        cutoff = datetime(2025, 1, 1, tzinfo=timezone.utc)
        assert prl.fetch_merged_prs(MagicMock(), "org", "repo", cutoff) == []


# ---------------------------------------------------------------------------
# fetch_review_required — protection + fallback + cache
# ---------------------------------------------------------------------------


def _mock_resp(status, body=None):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = body or {}
    r.raise_for_status = MagicMock()
    return r


class TestFetchReviewRequired:
    def test_required_when_count_ge_1(self):
        session = MagicMock()
        session.get.return_value = _mock_resp(
            200,
            {"required_pull_request_reviews": {"required_approving_review_count": 1}},
        )
        cache = {}
        assert prl.fetch_review_required(session, "o", "r", "main", cache, True) is True
        assert cache[("r", "main")] is True

    def test_not_required_when_count_0(self):
        session = MagicMock()
        session.get.return_value = _mock_resp(
            200,
            {"required_pull_request_reviews": {"required_approving_review_count": 0}},
        )
        assert prl.fetch_review_required(session, "o", "r", "main", {}, True) is False

    def test_cache_hit_avoids_second_call(self):
        session = MagicMock()
        cache = {("r", "main"): True}
        assert prl.fetch_review_required(session, "o", "r", "main", cache, True) is True
        session.get.assert_not_called()

    def test_403_with_require_true_skips(self):
        session = MagicMock()
        session.get.return_value = _mock_resp(403)
        assert prl.fetch_review_required(session, "o", "r", "main", {}, True) is False

    def test_403_with_require_false_falls_back_to_true(self):
        session = MagicMock()
        session.get.return_value = _mock_resp(403)
        # require_branch_protection=False → count all merged PRs anyway.
        assert prl.fetch_review_required(session, "o", "r", "main", {}, False) is True

    def test_404_behaves_like_403(self):
        session = MagicMock()
        session.get.return_value = _mock_resp(404)
        # No protection → skip when strict.
        assert prl.fetch_review_required(session, "o", "r", "main", {}, True) is False


# ---------------------------------------------------------------------------
# fetch_first_review — thin wrapper over pick_first_review + paginate
# ---------------------------------------------------------------------------


class TestFetchFirstReview:
    @patch("lucille.github.pr_review_latency.paginate")
    def test_returns_earliest_non_author_non_bot(self, mock_paginate):
        mock_paginate.return_value = iter(
            [
                {
                    "user": {"login": "alice", "type": "User"},
                    "submitted_at": "2025-01-01T09:00:00Z",
                },
                {
                    "user": {"login": "dependabot", "type": "Bot"},
                    "submitted_at": "2025-01-01T09:30:00Z",
                },
                {
                    "user": {"login": "bob", "type": "User"},
                    "submitted_at": "2025-01-01T10:00:00Z",
                },
            ]
        )
        at, login = prl.fetch_first_review(
            MagicMock(), "o", "r", 42, author="alice", bot_logins=["dependabot"]
        )
        assert login == "bob"
        assert at == datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)

    @patch("lucille.github.pr_review_latency.paginate")
    def test_no_reviews(self, mock_paginate):
        mock_paginate.return_value = iter([])
        assert prl.fetch_first_review(MagicMock(), "o", "r", 1, "alice", []) == (
            None,
            None,
        )


# ---------------------------------------------------------------------------
# build_pr_rows — integration of the above at the row-assembly layer
# ---------------------------------------------------------------------------


class TestBuildPRRows:
    def _pr(self, number, base="main", author="alice", title="t"):
        return {
            "number": number,
            "title": title,
            "user": {"login": author, "type": "User"},
            "base": {"ref": base},
            "created_at": "2025-01-06T09:00:00Z",
            "merged_at": "2025-01-07T09:00:00Z",
            "html_url": f"https://x/{number}",
        }

    @patch("lucille.github.pr_review_latency.fetch_first_review")
    @patch("lucille.github.pr_review_latency.fetch_review_required")
    def test_drops_unprotected_prs(self, mock_required, mock_first):
        mock_required.side_effect = [True, False]
        mock_first.return_value = (datetime(2025, 1, 6, 15, tzinfo=timezone.utc), "bob")
        rows = prl.build_pr_rows(
            MagicMock(),
            "o",
            "r",
            [self._pr(1, base="main"), self._pr(2, base="feature")],
            bot_logins=[],
            require_branch_protection=True,
            protection_cache={},
        )
        assert [r["number"] for r in rows] == [1]

    @patch("lucille.github.pr_review_latency.fetch_first_review")
    @patch("lucille.github.pr_review_latency.fetch_review_required")
    def test_populates_latency_and_none_for_unreviewed(self, mock_required, mock_first):
        mock_required.return_value = True
        mock_first.side_effect = [
            (datetime(2025, 1, 6, 15, tzinfo=timezone.utc), "bob"),
            (None, None),
        ]
        rows = prl.build_pr_rows(
            MagicMock(),
            "o",
            "r",
            [self._pr(1), self._pr(2)],
            bot_logins=[],
            require_branch_protection=True,
            protection_cache={},
        )
        assert rows[0]["first_reviewer"] == "bob"
        assert rows[0]["review_latency_hours"] == pytest.approx(6.0)
        assert rows[1]["first_reviewer"] == ""
        assert rows[1]["review_latency_hours"] is None
