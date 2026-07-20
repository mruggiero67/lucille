"""Alert-noise analysis for OpsGenie CSVs.

The core insight: most OpsGenie noise is concentrated in a small number
of monitors that fire repeatedly and get auto-closed without a human
ever acknowledging them. Rank monitors by fire count, expose ack rate
and auto-close rate, and let the on-call team see which monitors to
tune first.

Grouping is by OpsGenie ``Alias`` \u2014 the client-provided dedup key that
integrations (Datadog, etc.) set consistently for the same underlying
condition. When two alerts share an alias, they're the same monitor
firing twice.

All functions in this module are pure. I/O lives in ``main.py``.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Iterable, List, Optional, Sequence, Set

from lucille.opsgenie.io import Alert


@dataclass(frozen=True)
class NoiseRow:
    """One aggregated group \u2014 the noise profile of a single monitor.

    Sort key: ``fires`` desc, then ``alias`` for a stable tiebreak.
    """

    alias: str
    sample_message: str          # first Message seen for this alias
    fires: int                   # total rows in the CSV for this alias
    ack_count: int
    ack_rate: float              # ack_count / fires, in [0, 1]
    auto_closed_no_ack: int      # closed + never acknowledged
    auto_close_rate: float       # auto_closed_no_ack / fires, in [0, 1]
    days_active: int             # unique UTC dates on which it fired
    fires_per_active_day: float  # fires / days_active
    first_seen: datetime
    last_seen: datetime
    teams: str                   # comma-separated, sorted, deduped


@dataclass(frozen=True)
class NoiseSummary:
    """Aggregate view of an entire CSV's alert-noise profile."""

    total_alerts: int
    unique_aliases: int
    overall_ack_rate: float
    overall_auto_close_rate: float
    top_5_share: float           # fraction of total alerts from the top 5 aliases
    top_10_share: float
    top_20_share: float
    window_start: Optional[datetime]
    window_end: Optional[datetime]


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------


def coarse_alias(raw: str) -> str:
    """Collapse per-instance tags off Datadog aliases; pass others through.

    Datadog OpsGenie aliases look like::

        org_id:305115|metric:X|monitor_id:143823196|#job:foo,env:prod-us

    A single logical monitor fragments into dozens of rows because the
    trailing ``#tag`` list varies per firing instance (per-job, per-host,
    per-service). The ``monitor_id`` is Datadog's stable identifier for
    the monitor object itself, so collapsing on it merges all fragments
    of the same underlying monitor.

    Non-Datadog aliases — typically opaque UUIDs from other integrations
    — pass through unchanged, because for those the whole alias *is* the
    identifier and there's nothing to collapse.

    Malformed inputs (blank, no ``monitor_id:`` segment, non-numeric id)
    also pass through: better to preserve the raw alias than to silently
    misgroup unrelated alerts under a synthetic key.
    """
    if not raw or "monitor_id:" not in raw:
        return raw
    for part in raw.split("|"):
        part = part.strip()
        if part.startswith("monitor_id:"):
            # ``monitor_id`` may be followed by a comma-joined sub-tag list
            # in some exports; take just the id itself.
            mid = part[len("monitor_id:"):].split(",")[0].strip()
            if mid.isdigit():
                return f"dd:monitor_{mid}"
            break
    return raw


def group_by_alias(alerts: Iterable[Alert]) -> "dict[str, List[Alert]]":
    """Return alerts bucketed by ``alias``. Alerts with no alias go to ``\"\"``."""
    groups: "dict[str, List[Alert]]" = defaultdict(list)
    for a in alerts:
        groups[a.alias].append(a)
    return dict(groups)


def compute_noise_rows(
    alerts: Sequence[Alert],
    *,
    key_fn: Optional[Callable[[Alert], str]] = None,
) -> List[NoiseRow]:
    """Compute a ``NoiseRow`` per group, sorted by fire count desc.

    ``key_fn`` controls how alerts are grouped. When ``None`` (default),
    alerts are grouped by their raw ``Alias``. Pass a function returning
    a coarser key (e.g. ``lambda a: coarse_alias(a.alias)``) to collapse
    Datadog per-instance fragmentation. The ``NoiseRow.alias`` field
    then holds that coarser key.

    Tiebreak: key ascending, so runs are deterministic.
    """
    key = key_fn if key_fn is not None else (lambda a: a.alias)
    groups: "dict[str, List[Alert]]" = defaultdict(list)
    for a in alerts:
        groups[key(a)].append(a)

    rows: List[NoiseRow] = []
    for alias, group in groups.items():
        fires = len(group)
        ack_count = sum(1 for a in group if a.acknowledged)
        auto_closed_no_ack = sum(
            1 for a in group if a.status == "closed" and not a.acknowledged
        )
        days: Set = {a.created_at.date() for a in group}
        teams: Set[str] = {a.team for a in group if a.team}
        # Sample message: the first non-empty message we see, falling back to
        # the very first row's message if all are empty (which shouldn't
        # happen but let's not KeyError on it).
        sample = next((a.message for a in group if a.message), group[0].message)
        rows.append(
            NoiseRow(
                alias=alias,
                sample_message=sample,
                fires=fires,
                ack_count=ack_count,
                ack_rate=ack_count / fires if fires else 0.0,
                auto_closed_no_ack=auto_closed_no_ack,
                auto_close_rate=auto_closed_no_ack / fires if fires else 0.0,
                days_active=len(days),
                fires_per_active_day=fires / len(days) if days else 0.0,
                first_seen=min(a.created_at for a in group),
                last_seen=max(a.created_at for a in group),
                teams=", ".join(sorted(teams)),
            )
        )
    rows.sort(key=lambda r: (-r.fires, r.alias))
    return rows


def filter_by_min_fires(rows: Sequence[NoiseRow], min_fires: int) -> List[NoiseRow]:
    """Return only rows that fired at least ``min_fires`` times."""
    return [r for r in rows if r.fires >= min_fires]


def top_n(rows: Sequence[NoiseRow], n: int) -> List[NoiseRow]:
    """Return the first ``n`` rows (rows are assumed pre-sorted).

    A non-positive ``n`` returns an empty list \u2014 avoids Python's
    ``list[:-1]`` trap where a caller passing 0 would get the whole list
    minus one item.
    """
    if n <= 0:
        return []
    return list(rows[:n])


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


def summarize(alerts: Sequence[Alert], rows: Sequence[NoiseRow]) -> NoiseSummary:
    """Compute the whole-CSV noise summary.

    ``rows`` must already be sorted by fires desc (as returned by
    ``compute_noise_rows``).
    """
    total = len(alerts)
    if total == 0:
        return NoiseSummary(
            total_alerts=0,
            unique_aliases=0,
            overall_ack_rate=0.0,
            overall_auto_close_rate=0.0,
            top_5_share=0.0,
            top_10_share=0.0,
            top_20_share=0.0,
            window_start=None,
            window_end=None,
        )

    ack = sum(1 for a in alerts if a.acknowledged)
    auto = sum(1 for a in alerts if a.status == "closed" and not a.acknowledged)

    def _share(k: int) -> float:
        return sum(r.fires for r in rows[:k]) / total

    return NoiseSummary(
        total_alerts=total,
        unique_aliases=len(rows),
        overall_ack_rate=ack / total,
        overall_auto_close_rate=auto / total,
        top_5_share=_share(5),
        top_10_share=_share(10),
        top_20_share=_share(20),
        window_start=min(a.created_at for a in alerts),
        window_end=max(a.created_at for a in alerts),
    )
