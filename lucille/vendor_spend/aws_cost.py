"""
AWS Cost Explorer fetcher.

Queries the payer/master account at DAILY granularity and lets the caller
bucket the daily rows into Mon-Sun weeks. We avoid Cost Explorer's WEEKLY
granularity because it doesn't align to ISO weeks.

Cost basis: ``UnblendedCost`` (per project decisions).
Auth: standard boto3 credential chain (``~/.aws/credentials``, env vars,
SSO, etc.). The config carries only the payer account id (informational)
and region (must be ``us-east-1``).
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

import boto3

from lucille.vendor_spend.config import AwsConfig
from lucille.vendor_spend.weekly_buckets import to_date

logger = logging.getLogger(__name__)


# ---- pure ------------------------------------------------------------------

def parse_cost_and_usage_response(payload: dict[str, Any]) -> list[tuple[date, float]]:
    """
    Convert a ``GetCostAndUsage`` response (DAILY, UnblendedCost) into
    ``(date, usd)`` rows.

    AWS returns each bucket as ``{'TimePeriod': {'Start': 'YYYY-MM-DD',
    'End': 'YYYY-MM-DD'}, 'Total': {'UnblendedCost': {'Amount': '12.34',
    'Unit': 'USD'}}}`` where ``Start`` is inclusive and ``End`` exclusive.
    """
    rows: list[tuple[date, float]] = []
    for bucket in payload.get("ResultsByTime", []):
        period = bucket.get("TimePeriod") or {}
        if "Start" not in period:
            continue
        total = (bucket.get("Total") or {}).get("UnblendedCost") or {}
        amount = total.get("Amount")
        if amount is None:
            continue
        try:
            rows.append((to_date(period["Start"]), float(amount)))
        except (TypeError, ValueError):
            logger.warning("Skipping malformed AWS bucket: %s", bucket)
    return rows


# ---- side-effecting --------------------------------------------------------

def fetch_raw(
    cfg: AwsConfig,
    start: date,
    end: date,
    *,
    client=None,
) -> dict[str, Any]:
    """
    Call ``GetCostAndUsage`` for the inclusive range ``[start, end]``.

    AWS uses an exclusive end-date, so we add one day before sending.
    """
    if end < start:
        raise ValueError("end must be >= start")
    ce = client or boto3.client("ce", region_name=cfg.region)
    aws_end_exclusive = (end + timedelta(days=1)).isoformat()
    logger.info(
        "AWS Cost Explorer: GetCostAndUsage Start=%s End=%s (exclusive) account=%s",
        start.isoformat(),
        aws_end_exclusive,
        cfg.account_id,
    )
    return ce.get_cost_and_usage(
        TimePeriod={"Start": start.isoformat(), "End": aws_end_exclusive},
        Granularity="DAILY",
        Metrics=["UnblendedCost"],
    )


def fetch_daily_costs(
    cfg: AwsConfig,
    start: date,
    end: date,
    *,
    client=None,
) -> list[tuple[date, float]]:
    """End-to-end fetch + parse."""
    payload = fetch_raw(cfg, start, end, client=client)
    rows = parse_cost_and_usage_response(payload)
    logger.info("AWS: parsed %d daily cost rows", len(rows))
    return rows
