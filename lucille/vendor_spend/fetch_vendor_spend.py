"""
Fetch the last N weeks of spend for AWS, Databricks, and Datadog and write a
long-format CSV to ~/Desktop/debris/YYYY_MM_DD_vendor_spend.csv.

Usage:
    python -m lucille.vendor_spend.fetch_vendor_spend
    python -m lucille.vendor_spend.fetch_vendor_spend --vendors aws,datadog
    python -m lucille.vendor_spend.fetch_vendor_spend --weeks 8 --output-dir /tmp
"""

from __future__ import annotations

import argparse
import csv
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable

from lucille.vendor_spend import aws_cost, databricks_cost, datadog_cost
from lucille.vendor_spend.config import (
    DEFAULT_CONFIG_PATH,
    VendorSpendConfig,
    load_config,
)
from lucille.vendor_spend.weekly_buckets import (
    bucket_into_weeks,
    last_n_week_starts,
)
from lucille.common.logging import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


VENDOR_AWS = "AWS"
VENDOR_DATABRICKS = "Databricks"
VENDOR_DATADOG = "Datadog"
ALL_VENDORS = (VENDOR_AWS, VENDOR_DATABRICKS, VENDOR_DATADOG)

SOURCES = {
    VENDOR_AWS: "aws_cost_explorer_unblended",
    VENDOR_DATABRICKS: "databricks_billable_usage_effective_list",
    VENDOR_DATADOG: "datadog_estimated_cost",
}


@dataclass(frozen=True)
class SpendRow:
    week_start: date
    vendor: str
    amount_usd: float
    source: str
    fetched_at: str


# ---- pure ------------------------------------------------------------------

def build_spend_rows(
    week_starts: list[date],
    daily_rows_by_vendor: dict[str, list[tuple[date, float]]],
    fetched_at: str,
) -> list[SpendRow]:
    """
    Bucket each vendor's daily ``(date, usd)`` rows into the supplied Mon-Sun
    weeks and emit a long-format list of ``SpendRow`` objects.
    """
    rows: list[SpendRow] = []
    for vendor, daily in daily_rows_by_vendor.items():
        weekly = bucket_into_weeks(daily, week_starts)
        for ws in week_starts:
            rows.append(
                SpendRow(
                    week_start=ws,
                    vendor=vendor,
                    amount_usd=round(weekly[ws], 2),
                    source=SOURCES.get(vendor, vendor.lower()),
                    fetched_at=fetched_at,
                )
            )
    return rows


def csv_filename_for(today: date) -> str:
    return f"{today.strftime('%Y_%m_%d')}_vendor_spend.csv"


# ---- side-effecting --------------------------------------------------------

def write_csv(rows: list[SpendRow], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["week_start", "vendor", "amount_usd", "source", "fetched_at"])
        for r in rows:
            writer.writerow(
                [r.week_start.isoformat(), r.vendor, f"{r.amount_usd:.2f}", r.source, r.fetched_at]
            )
    logger.info("Wrote %d rows to %s", len(rows), path)


# Vendor dispatch -----------------------------------------------------------

VendorFetcher = Callable[[VendorSpendConfig, date, date], list[tuple[date, float]]]


def _fetch_aws(cfg: VendorSpendConfig, start: date, end: date) -> list[tuple[date, float]]:
    return aws_cost.fetch_daily_costs(cfg.aws, start, end)


def _fetch_databricks(cfg: VendorSpendConfig, start: date, end: date) -> list[tuple[date, float]]:
    return databricks_cost.fetch_daily_costs(cfg.databricks, start, end)


def _fetch_datadog(cfg: VendorSpendConfig, start: date, end: date) -> list[tuple[date, float]]:
    return datadog_cost.fetch_daily_costs(cfg.datadog, start, end)


def _vendor_fetchers() -> dict[str, VendorFetcher]:
    # Resolved on each call so unit tests can patch ``_fetch_*`` symbols.
    return {
        VENDOR_AWS: _fetch_aws,
        VENDOR_DATABRICKS: _fetch_databricks,
        VENDOR_DATADOG: _fetch_datadog,
    }


def collect_daily_rows(
    cfg: VendorSpendConfig,
    vendors: list[str],
    start: date,
    end: date,
) -> dict[str, list[tuple[date, float]]]:
    """
    Call each requested vendor's fetcher. If one vendor fails, log and continue
    with an empty series so the report still produces a graph for the others.
    """
    fetchers = _vendor_fetchers()
    out: dict[str, list[tuple[date, float]]] = {}
    for v in vendors:
        fetcher = fetchers[v]
        try:
            out[v] = fetcher(cfg, start, end)
        except Exception:  # noqa: BLE001 - we want to keep going on per-vendor failure
            logger.exception("Failed to fetch spend for vendor=%s", v)
            out[v] = []
    return out


# ---- CLI -------------------------------------------------------------------

def _parse_vendors(arg: str) -> list[str]:
    requested = [v.strip() for v in arg.split(",") if v.strip()]
    canonical = {v.lower(): v for v in ALL_VENDORS}
    out: list[str] = []
    for v in requested:
        key = v.lower()
        if key not in canonical:
            raise argparse.ArgumentTypeError(
                f"Unknown vendor {v!r}. Choose from {', '.join(ALL_VENDORS)}."
            )
        out.append(canonical[key])
    return out


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Fetch last-N-weeks vendor spend (AWS, Databricks, Datadog) into a CSV."
    )
    p.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to YAML config (default: {DEFAULT_CONFIG_PATH}).",
    )
    p.add_argument("--weeks", type=int, default=None, help="Override number of weeks (default from YAML, typically 6).")
    p.add_argument(
        "--output-dir",
        default=None,
        help="Override output directory for the CSV (default from YAML, typically ~/Desktop/debris).",
    )
    p.add_argument(
        "--vendors",
        type=_parse_vendors,
        default=list(ALL_VENDORS),
        help="Comma-separated subset of vendors to fetch (default: all three).",
    )
    p.add_argument(
        "--today",
        default=None,
        help="Override 'today' as YYYY-MM-DD (useful for backfills/tests).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    cfg = load_config(args.config)
    weeks = args.weeks or cfg.weeks
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else cfg.output_dir
    today = date.fromisoformat(args.today) if args.today else date.today()

    week_starts = last_n_week_starts(today, weeks)
    start, end = week_starts[0], week_starts[-1] + timedelta(days=6)
    logger.info(
        "Today is %s; fetching %d weeks of spend (%s..%s) for vendors=%s",
        today, weeks, start, end, args.vendors,
    )

    daily = collect_daily_rows(cfg, args.vendors, start, end)
    fetched_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    rows = build_spend_rows(week_starts, daily, fetched_at)

    out_path = output_dir / csv_filename_for(today)
    write_csv(rows, out_path)
    logger.info("Done. CSV at %s", out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
