"""Aggregations for ai_metrics: pure functions over PR / ticket records."""

from __future__ import annotations

import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from lucille.ai_metrics.fetch import PRRecord
from lucille.ai_metrics.jira_cycle import TicketCycle


# ---------------------------------------------------------------------------
# Bucket helpers
# ---------------------------------------------------------------------------


def week_start(dt: datetime) -> date:
    """Monday of the ISO week containing ``dt``."""
    d = dt.date()
    return d - timedelta(days=d.weekday())


def format_week(d: date) -> str:
    """Chart-friendly week label like ``"2026-W15"``."""
    iso_year, iso_week, _ = d.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


# ---------------------------------------------------------------------------
# Ratio & rate primitives
# ---------------------------------------------------------------------------


@dataclass
class Ratio:
    """A ``numerator / denominator`` pair carried around together for clarity."""
    numerator: int
    denominator: int

    @property
    def value(self) -> Optional[float]:
        return self.numerator / self.denominator if self.denominator else None

    def as_percent(self) -> str:
        v = self.value
        return f"{v * 100:.1f}%" if v is not None else "n/a"


# ---------------------------------------------------------------------------
# PR-level aggregations
# ---------------------------------------------------------------------------


def ai_touched_share(prs: Sequence[PRRecord]) -> Ratio:
    """% of PRs (opened in window) that are AI-touched."""
    num = sum(1 for p in prs if p.ai_touched)  # type: ignore[attr-defined]
    return Ratio(num, len(prs))


def merge_rate(prs: Sequence[PRRecord]) -> Ratio:
    """Merged / (merged + closed_unmerged). Open PRs excluded from denom."""
    finished = [p for p in prs if p.state == "closed"]
    merged = sum(1 for p in finished if p.merged)
    return Ratio(merged, len(finished))


def revert_rate(prs: Sequence[PRRecord], reverted_numbers: Iterable[int]) -> Ratio:
    """# merged PRs later reverted / # merged PRs."""
    merged = [p for p in prs if p.merged]
    reverted_set = set(reverted_numbers)
    reverted_hit = sum(1 for p in merged if p.number in reverted_set)
    return Ratio(reverted_hit, len(merged))


def split_by_ai(prs: Sequence[PRRecord]) -> Tuple[List[PRRecord], List[PRRecord]]:
    """Return ``(ai_prs, human_prs)``."""
    ai: List[PRRecord] = []
    human: List[PRRecord] = []
    for p in prs:
        (ai if p.ai_touched else human).append(p)  # type: ignore[attr-defined]
    return ai, human


# ---------------------------------------------------------------------------
# Weekly trend
# ---------------------------------------------------------------------------


@dataclass
class WeeklyRow:
    week: str                  # e.g. "2026-W15"
    prs_opened: int
    ai_touched: int
    ai_share: Optional[float]  # 0..1
    merged: int
    merge_rate: Optional[float]
    ai_merged: int
    ai_merge_rate: Optional[float]
    human_merged: int
    human_merge_rate: Optional[float]


def weekly_trend(prs: Sequence[PRRecord]) -> List[WeeklyRow]:
    """One row per calendar week, ordered chronologically."""
    by_week: Dict[str, List[PRRecord]] = defaultdict(list)
    for p in prs:
        by_week[format_week(week_start(p.created_at))].append(p)

    rows: List[WeeklyRow] = []
    for week in sorted(by_week):
        wk_prs = by_week[week]
        ai_prs, human_prs = split_by_ai(wk_prs)
        merge_all = merge_rate(wk_prs)
        merge_ai = merge_rate(ai_prs)
        merge_hu = merge_rate(human_prs)
        share = ai_touched_share(wk_prs)
        rows.append(WeeklyRow(
            week=week,
            prs_opened=len(wk_prs),
            ai_touched=len(ai_prs),
            ai_share=share.value,
            merged=merge_all.numerator,
            merge_rate=merge_all.value,
            ai_merged=merge_ai.numerator,
            ai_merge_rate=merge_ai.value,
            human_merged=merge_hu.numerator,
            human_merge_rate=merge_hu.value,
        ))
    return rows


# ---------------------------------------------------------------------------
# Ticket-level aggregation
# ---------------------------------------------------------------------------


@dataclass
class TicketBucket:
    label: str                 # "ai" or "human"
    n: int
    median_days: Optional[float]
    mean_days: Optional[float]
    p90_days: Optional[float]


def _pctl(sorted_vals: Sequence[float], q: float) -> Optional[float]:
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    idx = q * (len(sorted_vals) - 1)
    lo, hi = int(idx), min(int(idx) + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac


def summarize_bucket(values: Sequence[float], label: str) -> TicketBucket:
    vs = sorted(v for v in values if v is not None)
    return TicketBucket(
        label=label,
        n=len(vs),
        median_days=statistics.median(vs) if vs else None,
        mean_days=statistics.fmean(vs) if vs else None,
        p90_days=_pctl(vs, 0.9),
    )


def compare_ticket_cycle_times(
    ai_cycle_days: Sequence[float],
    human_cycle_days: Sequence[float],
) -> Tuple[TicketBucket, TicketBucket]:
    return (
        summarize_bucket(ai_cycle_days, "ai"),
        summarize_bucket(human_cycle_days, "human"),
    )
