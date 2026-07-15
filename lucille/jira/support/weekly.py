"""Pure, side-effect-free helpers for weekly SUP analyses."""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, List, Tuple


def get_week_label(date: datetime) -> str:
    """Return a Sunday-based week label like ``"2026-W06"``."""
    return date.strftime("%Y-W%U")


def get_date_range(weeks_back: int) -> Tuple[str, str]:
    """Return ``(start_date, end_date)`` in YYYY-MM-DD, ending today."""
    end_date = datetime.now()
    start_date = end_date - timedelta(weeks=weeks_back)
    return (
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d"),
    )


def group_by_week(issues: List[Dict], week_key: str) -> Dict[str, List[Dict]]:
    """Group issues by the value of ``week_key`` on each row.

    Args:
        issues: List of processed issue dicts.
        week_key: Attribute name holding the week label (e.g. ``"resolved_week"``,
            ``"created_week"``).

    Returns:
        Mapping of week-label -> list of issues, in insertion order.
    """
    grouped: Dict[str, List[Dict]] = defaultdict(list)
    for issue in issues:
        week = issue.get(week_key)
        if week:
            grouped[week].append(issue)
    return dict(grouped)


def classify_trend(
    values: List[float],
    variability_threshold: float = 0.4,
    change_threshold: float = 0.1,
) -> str:
    """Classify a time-ordered sequence of weekly values as a trend.

    Uses coefficient of variation (CV) to detect high variability first, then
    compares the first-half vs second-half average to determine direction.

    Args:
        values: Weekly metric values in chronological order.
        variability_threshold: CV above this → ``'highly variable'``.
        change_threshold: Fractional half-over-half change required for
            ``'growing'`` / ``'shrinking'`` (default 0.1 = 10%).

    Returns:
        One of ``'growing'``, ``'shrinking'``, ``'stable'``,
        ``'highly variable'``, or ``'insufficient data'``.
    """
    if len(values) < 2:
        return "insufficient data"

    mean = sum(values) / len(values)
    if mean == 0:
        return "stable"

    variance = sum((v - mean) ** 2 for v in values) / len(values)
    cv = math.sqrt(variance) / mean
    if cv > variability_threshold:
        return "highly variable"

    mid = len(values) // 2
    first_avg = sum(values[:mid]) / mid
    second_avg = sum(values[mid:]) / len(values[mid:])

    if first_avg == 0:
        return "growing" if second_avg > 0 else "stable"

    ratio = second_avg / first_avg
    if ratio > 1 + change_threshold:
        return "growing"
    if ratio < 1 - change_threshold:
        return "shrinking"
    return "stable"
