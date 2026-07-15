"""
Roll up an AWS Cost Explorer "Daily costs by Service" CSV export into
Mon-Sun weekly totals, matching the shape of the long-format CSV that
``fetch_vendor_spend`` writes (so ``graph_vendor_spend`` can render it
without modification).

This script is a comparison tool: it lets you sanity-check the AWS bar in
the main vendor-spend chart against numbers exported directly from the
Cost Explorer UI for the same Mon-Sun weeks.

Expected CSV shape (from the AWS Cost Explorer "Download CSV" button):

    "Service","EC2-Other($)","EC2-Instances($)",...,"Total costs($)"
    "Service total","55122.52","54646.84",...,"178755.10"
    "2026-03-09","679.43","511.96",...,"2383.48"
    "2026-03-10","705.99","528.48",...,"2474.34"
    ...

We only consume the first column (the date) and the last column (the
``Total costs($)`` per day). Per-service columns are ignored.

Usage:
    python -m lucille.vendor_spend.aws_cost_explorer_csv \\
        --csv ~/Desktop/debris/2026_05_06_aws_costs_since_9_mar.csv

    # skip the chart, just write the rollup CSV
    python -m lucille.vendor_spend.aws_cost_explorer_csv --csv ... --no-render
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

from lucille.vendor_spend.config import DEFAULT_CONFIG_PATH, load_config
from lucille.vendor_spend.fetch_vendor_spend import (
    SpendRow,
    VENDOR_AWS,
    write_csv as write_long_format_csv,
)
from lucille.vendor_spend.weekly_buckets import (
    bucket_into_weeks,
    complete_week_starts,
    monday_of,
    to_date,
)
from lucille.common.logging import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

# Source label so the vendor field on the chart distinguishes this from the
# live-API AWS rows produced by fetch_vendor_spend.
VENDOR_LABEL = f"{VENDOR_AWS} (Console export)"
SOURCE_LABEL = "aws_cost_explorer_console_csv"

DATE_COLUMN_HEADER_VALUES = {"Service", "service"}  # first cell in header row
TOTAL_COLUMN_HEADER_SUFFIX = "Total costs"          # tolerant of "Total costs($)"
SUMMARY_ROW_FIRST_CELL = "Service total"


# ---- pure ------------------------------------------------------------------

@dataclass(frozen=True)
class ParsedAwsCsv:
    daily_rows: list[tuple[date, float]]
    skipped_rows: int  # rows that weren't date-prefixed (summary row, blanks)


def _strip_bom(s: str) -> str:
    return s.lstrip("\ufeff")


def _find_total_column_index(header: list[str]) -> int:
    """
    Locate the ``Total costs($)`` column by suffix match. AWS sometimes adds
    or removes the ``($)`` so we match on the prefix to be safe.
    """
    for i, col in enumerate(header):
        if (col or "").strip().startswith(TOTAL_COLUMN_HEADER_SUFFIX):
            return i
    raise ValueError(
        f"AWS CSV header has no 'Total costs' column; got headers: {header}"
    )


def _maybe_parse_date(cell: str) -> date | None:
    """Return the parsed date if the cell looks like an ISO date, else None."""
    s = (cell or "").strip()
    if not s or s == SUMMARY_ROW_FIRST_CELL:
        return None
    try:
        return to_date(s)
    except (ValueError, TypeError):
        return None


def parse_aws_explorer_csv(csv_text: str) -> ParsedAwsCsv:
    """
    Parse the body of a Cost Explorer "Daily costs by Service" export and
    return ``(date, total_usd)`` rows. Pure function.

    * Skips the ``Service total`` summary row.
    * Skips any leading blanks or non-date rows.
    * Tolerates the UTF-8 BOM AWS sometimes prepends.
    * Treats empty totals as 0.0.
    """
    text = _strip_bom(csv_text)
    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration:
        return ParsedAwsCsv(daily_rows=[], skipped_rows=0)

    if not header:
        return ParsedAwsCsv(daily_rows=[], skipped_rows=0)

    total_idx = _find_total_column_index(header)

    rows: list[tuple[date, float]] = []
    skipped = 0
    for raw in reader:
        if not raw:
            skipped += 1
            continue
        d = _maybe_parse_date(raw[0])
        if d is None:
            skipped += 1
            continue
        try:
            total = float((raw[total_idx] or "0").strip() or 0.0)
        except (IndexError, ValueError):
            logger.warning("Skipping AWS row with bad total: %s", raw[:3])
            skipped += 1
            continue
        rows.append((d, total))
    return ParsedAwsCsv(daily_rows=rows, skipped_rows=skipped)


def build_spend_rows_from_aws_daily(
    daily_rows: list[tuple[date, float]],
    fetched_at: str,
) -> list[SpendRow]:
    """Bucket daily rows into Mon-Sun weeks and emit ``SpendRow`` records."""
    weeks = complete_week_starts(daily_rows)
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
    """
    Date-prefix the output filename per the project convention:
        ~/Desktop/debris/YYYY_MM_DD_aws_explorer_weekly.csv
    """
    today = date.today().strftime("%Y_%m_%d")
    return output_dir / f"{today}_aws_explorer_weekly.csv"


# ---- CLI -------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Roll up an AWS Cost Explorer console CSV export into Mon-Sun "
            "weekly totals and (optionally) render a bar chart matching the "
            "main vendor-spend chart's style."
        )
    )
    p.add_argument(
        "--csv",
        required=True,
        help="Path to the AWS Cost Explorer 'Daily costs by Service' CSV.",
    )
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

    logger.info("Reading AWS Cost Explorer export from %s", csv_path)
    parsed = parse_aws_explorer_csv(read_csv_file(csv_path))
    logger.info(
        "Parsed %d daily rows (skipped %d non-date rows)",
        len(parsed.daily_rows), parsed.skipped_rows,
    )
    if not parsed.daily_rows:
        logger.error("No daily rows found in %s; aborting.", csv_path)
        return 1

    fetched_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    spend_rows = build_spend_rows_from_aws_daily(parsed.daily_rows, fetched_at)
    if not spend_rows:
        logger.error(
            "No complete Mon-Sun weeks in %s. The export needs at least one "
            "full Mon-Sun span; got %d daily rows from %s to %s.",
            csv_path,
            len(parsed.daily_rows),
            parsed.daily_rows[0][0],
            parsed.daily_rows[-1][0],
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

    # Lazy import so unit tests don't need matplotlib unless they touch this.
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
            f"AWS weekly spend (Cost Explorer console export) — "
            f"{len(spend_rows)} complete weeks"
        ),
    )
    logger.info("Wrote PNG to %s", png_path)
    logger.info("Total across window: $%.2f", sum(summary["totals_by_vendor"].values()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
