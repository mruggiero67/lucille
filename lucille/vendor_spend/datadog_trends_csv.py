"""
Roll up a Datadog Cost Analysis "Spend Trends" CSV (with daily breakdown
columns) into Mon-Sun weekly totals, in the same long format as
``fetch_vendor_spend`` writes. Companion comparison tool to
``aws_cost_explorer_csv`` and ``databricks_console_csv``.

Expected CSV shape (from Datadog's UI: Plan & Usage > Spend Analysis >
Trends, with the "Show daily breakdown" option enabled when downloading):

    dimension,Total,Mar 7,Mar 8,Mar 9,...,May 6
    __TOTAL__,38145.51,495.97,487.90,533.92,...,0
    audit_trail,730.77,...
    logs_indexed_15day,7703.88,...
    ...

We only consume the ``__TOTAL__`` row (account-wide daily totals) and the
date headers in row 1. Per-product rows are ignored. Date headers carry no
year, so the parser walks them in order and increments the year whenever
the month wraps backwards (Dec -> Jan); the starting year defaults to
``date.today().year`` and can be overridden with ``--base-year``.

Usage:
    python -m lucille.vendor_spend.datadog_trends_csv \\
        --csv ~/Desktop/debris/2026_05_06_3-months-spend-trends-2026-03-07-2026-05-06.csv

    # Old export from a previous year
    python -m lucille.vendor_spend.datadog_trends_csv --csv ... --base-year 2025

    # CSV only, no PNG
    python -m lucille.vendor_spend.datadog_trends_csv --csv ... --no-render
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from lucille.vendor_spend.config import DEFAULT_CONFIG_PATH, load_config
from lucille.vendor_spend.fetch_vendor_spend import (
    SpendRow,
    VENDOR_DATADOG,
    write_csv as write_long_format_csv,
)
from lucille.vendor_spend.weekly_buckets import (
    bucket_into_weeks,
    complete_week_starts,
)
from lucille.common.logging import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

VENDOR_LABEL = f"{VENDOR_DATADOG} (Console export)"
SOURCE_LABEL = "datadog_trends_csv"
DEFAULT_FROM_DATE = date(2026, 3, 9)  # matches AWS / Databricks comparison window
TOTAL_DIMENSION = "__TOTAL__"


# ---- pure ------------------------------------------------------------------

@dataclass(frozen=True)
class ParsedDatadogTrends:
    daily_rows: list[tuple[date, float]]
    skipped_columns: int  # cells that were blank or unparseable as numbers
    inferred_year_breaks: int  # number of times the parser bumped the year


def _strip_bom(s: str) -> str:
    return s.lstrip("\ufeff")


def parse_short_date_headers(
    headers: list[str],
    base_year: int,
) -> tuple[list[date], int]:
    """
    Parse a list of short date headers like ``["Mar 7", "Mar 8", ..., "Jan 3"]``
    into ``date`` objects. Pure.

    ``base_year`` is the year of the first header. The parser walks left to
    right and increments the year whenever the month wraps backwards
    (e.g. Dec -> Jan), so a window that crosses a year boundary still parses
    correctly.

    Returns ``(dates, year_break_count)``.
    """
    dates: list[date] = []
    year = base_year
    breaks = 0
    prev_month: int | None = None
    for raw in headers:
        h = (raw or "").strip()
        if not h:
            raise ValueError("Empty date header in Datadog trends CSV")
        # Use a non-leap-year temp parse to extract month/day safely; then
        # re-attach the running year. If a date doesn't exist in the chosen
        # year (e.g. Feb 29 on a non-leap year), strptime will raise.
        parsed = datetime.strptime(h, "%b %d").date()
        if prev_month is not None and parsed.month < prev_month:
            year += 1
            breaks += 1
        d = date(year, parsed.month, parsed.day)
        dates.append(d)
        prev_month = parsed.month
    return dates, breaks


def parse_datadog_trends_csv(
    csv_text: str,
    *,
    base_year: int,
) -> ParsedDatadogTrends:
    """
    Parse a Datadog Trends CSV with daily-breakdown columns into
    ``(date, total_usd)`` per-day rows from the ``__TOTAL__`` row. Pure.
    """
    text = _strip_bom(csv_text)
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        return ParsedDatadogTrends(daily_rows=[], skipped_columns=0, inferred_year_breaks=0)

    if len(header) < 3 or header[0] != "dimension" or header[1] != "Total":
        raise ValueError(
            "Datadog trends CSV header must start with 'dimension,Total,...'; "
            f"got: {header[:3]}"
        )

    date_headers = header[2:]
    dates, breaks = parse_short_date_headers(date_headers, base_year)

    total_row: list[str] | None = None
    for row in reader:
        if row and row[0] == TOTAL_DIMENSION:
            total_row = row
            break
    if total_row is None:
        raise ValueError(
            f"Datadog trends CSV has no '{TOTAL_DIMENSION}' row; cannot derive "
            "account-wide daily totals."
        )

    cells = total_row[2:]  # align with date_headers
    daily: list[tuple[date, float]] = []
    skipped = 0
    for d, raw_cell in zip(dates, cells):
        cell = (raw_cell or "").strip()
        if not cell:
            skipped += 1
            continue
        try:
            usd = float(cell)
        except ValueError:
            logger.warning("Skipping unparseable Datadog cell: %r", cell)
            skipped += 1
            continue
        daily.append((d, usd))
    return ParsedDatadogTrends(
        daily_rows=daily,
        skipped_columns=skipped,
        inferred_year_breaks=breaks,
    )


def build_spend_rows_from_datadog_daily(
    daily_rows: list[tuple[date, float]],
    fetched_at: str,
    *,
    from_date: date,
) -> list[SpendRow]:
    """Bucket daily rows into complete Mon-Sun weeks >= ``from_date``."""
    weeks = [w for w in complete_week_starts(daily_rows) if w >= from_date]
    bucketed = bucket_into_weeks(daily_rows, weeks)
    return [
        SpendRow(
            week_start=ws,
            vendor=VENDOR_LABEL,
            amount_usd=round(bucketed[ws], 2),
            source=SOURCE_LABEL,
            fetched_at=fetched_at,
        )
        for ws in weeks
    ]


# ---- side-effecting --------------------------------------------------------

def read_csv_file(path: Path) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def derive_output_csv_path(input_csv: Path, output_dir: Path) -> Path:
    today = date.today().strftime("%Y_%m_%d")
    return output_dir / f"{today}_datadog_trends_weekly.csv"


# ---- CLI -------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Roll up a Datadog Spend Trends CSV (daily breakdown columns) "
            "into Mon-Sun weekly totals matching the AWS / Databricks "
            "comparison charts."
        )
    )
    p.add_argument("--csv", required=True, help="Path to the Datadog trends CSV.")
    p.add_argument(
        "--config",
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to YAML config (default: {DEFAULT_CONFIG_PATH}). Used only for output_dir.",
    )
    p.add_argument(
        "--output-dir",
        default=None,
        help="Override output directory (default: from YAML).",
    )
    p.add_argument(
        "--from-date",
        default=DEFAULT_FROM_DATE.isoformat(),
        help=(
            f"Drop weeks starting before this date (default: "
            f"{DEFAULT_FROM_DATE.isoformat()}, matching the AWS comparison window)."
        ),
    )
    p.add_argument(
        "--base-year",
        type=int,
        default=date.today().year,
        help=(
            "Year of the first date column (Datadog trends headers carry no "
            f"year). Default: current year ({date.today().year})."
        ),
    )
    p.add_argument(
        "--no-render",
        action="store_true",
        help="Skip PNG rendering; only write the rolled-up CSV.",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    csv_path = Path(args.csv).expanduser().resolve()
    cfg = load_config(args.config)
    output_dir = (
        Path(args.output_dir).expanduser().resolve()
        if args.output_dir
        else cfg.output_dir
    )
    from_date = date.fromisoformat(args.from_date)

    logger.info("Reading Datadog trends CSV from %s", csv_path)
    parsed = parse_datadog_trends_csv(read_csv_file(csv_path), base_year=args.base_year)
    logger.info(
        "Parsed %d daily rows (skipped %d empty/bad cells, %d year wraps)",
        len(parsed.daily_rows),
        parsed.skipped_columns,
        parsed.inferred_year_breaks,
    )
    if not parsed.daily_rows:
        logger.error("No daily rows found in %s; aborting.", csv_path)
        return 1

    fetched_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    spend_rows = build_spend_rows_from_datadog_daily(
        parsed.daily_rows, fetched_at, from_date=from_date
    )
    if not spend_rows:
        logger.error(
            "No complete Mon-Sun weeks on or after %s in %s.",
            from_date, csv_path,
        )
        return 1

    logger.info(
        "Rolled up into %d complete weeks (%s..%s)",
        len(spend_rows),
        spend_rows[0].week_start,
        spend_rows[-1].week_start,
    )
    for r in spend_rows:
        logger.info("  %s  $%10.2f", r.week_start, r.amount_usd)

    out_csv = derive_output_csv_path(csv_path, output_dir)
    write_long_format_csv(spend_rows, out_csv)

    if args.no_render:
        logger.info("Skipping render (--no-render).")
        return 0

    from lucille.vendor_spend.graph_vendor_spend import (
        build_dataframe,
        render_chart,
    )
    png_path = out_csv.with_suffix(".png")
    df = build_dataframe(out_csv)
    summary = render_chart(
        df,
        png_path,
        title=(
            f"Datadog weekly spend (Trends console export) — "
            f"{len(spend_rows)} weeks since {from_date}"
        ),
    )
    logger.info("Wrote PNG to %s", png_path)
    logger.info("Total across window: $%.2f", sum(summary["totals_by_vendor"].values()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
