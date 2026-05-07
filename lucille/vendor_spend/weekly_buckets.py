"""
Pure helpers for Mon-Sun week math used by the vendor-spend report.

No I/O, no network. Heavy unit-test target.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Iterable


def monday_of(d: date) -> date:
    """Return the Monday of the ISO week containing ``d`` (Mon=0..Sun=6)."""
    return d - timedelta(days=d.weekday())


def last_n_week_starts(today: date, n: int) -> list[date]:
    """
    Return the Monday-dates for the last ``n`` complete weeks ending *before*
    the week containing ``today``.

    Example: today = Fri 2026-05-01 (week of Mon 2026-04-27), n=6 ->
        [2026-03-16, 2026-03-23, 2026-03-30, 2026-04-06, 2026-04-13, 2026-04-20]

    The current (in-progress) week is intentionally excluded so every bar in
    the chart represents a complete Mon-Sun window.
    """
    if n <= 0:
        raise ValueError("n must be a positive integer")
    this_monday = monday_of(today)
    last_complete_monday = this_monday - timedelta(days=7)
    return [last_complete_monday - timedelta(days=7 * i) for i in range(n - 1, -1, -1)]


def week_start_for(d: date, week_starts: Iterable[date]) -> date | None:
    """
    Map a single date ``d`` to the Monday of the week-start it falls into,
    or ``None`` if it falls outside the provided ``week_starts`` window.

    ``week_starts`` must be Mondays.
    """
    starts = sorted(week_starts)
    if not starts:
        return None
    window_end = starts[-1] + timedelta(days=7)  # exclusive
    if d < starts[0] or d >= window_end:
        return None
    return monday_of(d)


def bucket_into_weeks(
    rows: Iterable[tuple[date, float]],
    week_starts: Iterable[date],
) -> dict[date, float]:
    """
    Sum ``(date, amount)`` rows into Mon-Sun buckets keyed by week-start.

    Rows whose date falls outside the supplied weeks are dropped.
    Every week in ``week_starts`` is present in the result, even if zero.
    """
    starts = sorted(week_starts)
    totals: dict[date, float] = {ws: 0.0 for ws in starts}
    for d, amount in rows:
        ws = week_start_for(d, starts)
        if ws is None:
            continue
        totals[ws] += float(amount)
    return totals


def complete_week_starts(daily_rows: Iterable[tuple[date, float]]) -> list[date]:
    """
    Return the Monday-dates for every Mon-Sun week that has all 7 days
    present in ``daily_rows``. Pure.

    Use when importing a console CSV export that may start or end mid-week:
    showing a half-week bar next to full-week bars is misleading, so we
    drop the partial windows at both ends.
    """
    by_week: dict[date, set[date]] = {}
    for d, _ in daily_rows:
        by_week.setdefault(monday_of(d), set()).add(d)
    return sorted(
        ws for ws, days in by_week.items()
        if len(days) == 7
    )


def to_date(value) -> date:
    """Coerce a ``date`` / ``datetime`` / ISO-8601 string to a ``date``."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        # Tolerate trailing "Z" or time component
        s = value.replace("Z", "")
        if "T" in s:
            return datetime.fromisoformat(s).date()
        return date.fromisoformat(s)
    raise TypeError(f"Cannot coerce {type(value).__name__} to date")
