"""
Datadog estimated-cost fetcher.

Uses the v2 Usage > Estimated Cost API:
    GET https://api.{site}/api/v2/usage/estimated_cost?view=summary
        &start_date=YYYY-MM-DDTHH&end_date=YYYY-MM-DDTHH

The ``start_date`` / ``end_date`` params **must** include an hour component;
Datadog returns 400 if you pass plain ``YYYY-MM-DD`` (those are reserved
for the monthly form ``start_month`` / ``end_month``).

Returns daily (date, USD) rows. Bucketing into Mon-Sun weeks is the
caller's job (``weekly_buckets.bucket_into_weeks``).
"""

from __future__ import annotations

import calendar
import logging
from datetime import date, timedelta
from typing import Any

import requests

from lucille.vendor_spend.config import DatadogConfig, require_env
from lucille.vendor_spend.weekly_buckets import to_date

logger = logging.getLogger(__name__)


# ---- pure ------------------------------------------------------------------

def parse_estimated_cost_response(payload: dict[str, Any]) -> list[tuple[date, float]]:
    """
    Convert the JSON:API ``estimated_cost`` response into ``(date, usd)`` rows
    where the USD value is an **implied daily rate**.

    Important Datadog quirk: ``attributes.total_cost`` is the **estimated
    full-month cost** as of ``attributes.date``, *not* the cost incurred on
    that day. Within a month the same monthly figure is repeated on every
    day (subject to small drift as the projection refines). Naively summing
    the daily rows therefore overcounts by roughly the number of days in
    the month (~30×).

    To make the orchestrator's ``bucket_into_weeks`` produce sensible weekly
    totals, we convert each row's ``total_cost`` into an implied daily rate:

        daily_rate = monthly_total_cost / days_in_that_month

    Summing 7 of those daily rates gives ``monthly_total_cost × 7 / days``,
    i.e. the week's pro-rata share of the estimated month — which is what
    a Mon–Sun bar should represent for spike detection.
    """
    rows: list[tuple[date, float]] = []
    for item in payload.get("data", []):
        attrs = item.get("attributes") or {}
        if "date" not in attrs or "total_cost" not in attrs:
            continue
        try:
            d = to_date(attrs["date"])
            monthly = float(attrs["total_cost"])
        except (TypeError, ValueError):
            logger.warning("Skipping malformed Datadog row: %s", attrs)
            continue
        days_in_month = calendar.monthrange(d.year, d.month)[1]
        rows.append((d, monthly / days_in_month))
    return rows


# ---- side-effecting --------------------------------------------------------

def fetch_raw(
    cfg: DatadogConfig,
    start: date,
    end: date,
    *,
    session: requests.Session | None = None,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """
    Hit the Datadog estimated-cost endpoint for ``[start, end]`` (inclusive).
    Reads API/APP keys from the env vars named in ``cfg``.
    """
    api_key = require_env(cfg.api_key_env)
    app_key = require_env(cfg.app_key_env)
    url = f"https://api.{cfg.site}/api/v2/usage/estimated_cost"
    # Datadog requires hour-aware ISO timestamps on this endpoint. Use start
    # of the start day and start of the day after the end day for an
    # inclusive [start, end] window.
    params = {
        "view": "summary",
        "start_date": f"{start.isoformat()}T00",
        "end_date": f"{(end + timedelta(days=1)).isoformat()}T00",
    }
    headers = {
        "DD-API-KEY": api_key,
        "DD-APPLICATION-KEY": app_key,
        "Accept": "application/json",
    }
    s = session or requests.Session()
    logger.info(
        "Datadog: GET %s start_date=%s end_date=%s",
        url, params["start_date"], params["end_date"],
    )
    r = s.get(url, params=params, headers=headers, timeout=timeout)
    if not r.ok:
        # Surface Datadog's JSON error body in the exception message so the
        # log line above isn't the only signal.
        body = (r.text or "")[:500]
        raise requests.HTTPError(
            f"{r.status_code} from Datadog estimated_cost: {body}",
            response=r,
        )
    return r.json()


def fetch_daily_costs(
    cfg: DatadogConfig,
    start: date,
    end: date,
    *,
    session: requests.Session | None = None,
) -> list[tuple[date, float]]:
    """End-to-end: fetch + parse. Returns daily ``(date, usd)`` rows."""
    payload = fetch_raw(cfg, start, end, session=session)
    rows = parse_estimated_cost_response(payload)
    logger.info("Datadog: parsed %d daily cost rows", len(rows))
    return rows


def date_range_inclusive(start: date, end: date) -> list[date]:
    """Convenience pure helper used by tests and orchestrator."""
    if end < start:
        raise ValueError("end must be >= start")
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]
