"""Tests for lucille.github.pr_review_latency_top_repos.

Pure-transform tests only — the module has no network I/O.
"""

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from context import lucille  # noqa: F401
from lucille.github import pr_review_latency_top_repos as tops


def _row(repo, hours, created_at=None):
    """Build a minimal CSV-shaped row dict."""
    return {
        "repo": repo,
        "review_latency_hours": hours,
        "created_at": created_at or "2025-01-06T09:00:00+00:00",
    }


def _df(rows):
    df = pd.DataFrame(rows)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    df["review_latency_hours"] = pd.to_numeric(
        df["review_latency_hours"], errors="coerce"
    )
    return df


class TestComputeRepoAverages:
    def test_mean_and_ranking(self):
        df = _df(
            [
                _row("slow", 40),
                _row("slow", 60),
                _row("slow", 50),
                _row("fast", 4),
                _row("fast", 6),
                _row("fast", 5),
            ]
        )
        result = tops.compute_repo_averages(df, min_prs=1)
        # Sorted longest-first.
        assert list(result["repo"]) == ["slow", "fast"]
        assert result.iloc[0]["mean_hours"] == pytest.approx(50.0)
        assert result.iloc[1]["mean_hours"] == pytest.approx(5.0)
        assert result.iloc[0]["n"] == 3
        assert result.iloc[0]["median_hours"] == pytest.approx(50.0)
        assert result.iloc[0]["max_hours"] == pytest.approx(60.0)

    def test_min_prs_filter(self):
        df = _df(
            [
                _row("noisy", 100),  # only 1 PR — should be dropped at min_prs=3
                _row("solid", 10),
                _row("solid", 12),
                _row("solid", 14),
            ]
        )
        result = tops.compute_repo_averages(df, min_prs=3)
        assert list(result["repo"]) == ["solid"]

    def test_ignores_unreviewed_rows(self):
        df = _df(
            [
                _row("a", 10),
                _row("a", 20),
                _row("a", None),  # unreviewed — excluded from mean AND count
            ]
        )
        result = tops.compute_repo_averages(df, min_prs=1)
        row = result.iloc[0]
        assert row["n"] == 2
        assert row["mean_hours"] == pytest.approx(15.0)

    def test_empty_input(self):
        result = tops.compute_repo_averages(pd.DataFrame(), min_prs=1)
        assert result.empty
        assert list(result.columns) == [
            "repo",
            "n",
            "mean_hours",
            "median_hours",
            "max_hours",
        ]

    def test_all_unreviewed(self):
        df = _df([_row("a", None), _row("a", None)])
        assert tops.compute_repo_averages(df, min_prs=1).empty


class TestPickTop:
    def test_head(self):
        df = pd.DataFrame(
            {"repo": [f"r{i}" for i in range(15)], "mean_hours": list(range(15, 0, -1))}
        )
        assert list(tops.pick_top(df, 10)["repo"]) == [f"r{i}" for i in range(10)]

    def test_handles_shorter_than_top(self):
        df = pd.DataFrame({"repo": ["r0", "r1"], "mean_hours": [10, 5]})
        assert len(tops.pick_top(df, 10)) == 2


class TestFilterWindow:
    def test_no_weeks_returns_unchanged(self):
        df = _df([_row("a", 10)])
        assert len(tops.filter_window(df, None)) == 1
        assert len(tops.filter_window(df, 0)) == 1  # falsy → no filter

    def test_drops_old_rows(self):
        now = datetime.now(timezone.utc)
        recent = (now - timedelta(days=2)).isoformat()
        ancient = (now - timedelta(days=90)).isoformat()
        df = _df(
            [
                _row("a", 10, created_at=recent),
                _row("a", 999, created_at=ancient),
            ]
        )
        filtered = tops.filter_window(df, weeks=4)
        assert len(filtered) == 1
        assert filtered.iloc[0]["review_latency_hours"] == 10
