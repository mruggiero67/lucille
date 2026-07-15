"""
Roll up a Databricks console weekly-spend CSV export into the same long-
format CSV that ``fetch_vendor_spend`` writes, so ``graph_vendor_spend``
can render it. Companion comparison tool to ``aws_cost_explorer_csv``.

Expected CSV shape (from the Databricks usage dashboard's "Download CSV"):

    custom_tag_key_value_pairs,time_key,sum(usage_usd)
    <MISMATCH>,2025-10-13 00:00:00,1138.255016
    <MISMATCH>,2025-12-08 00:00:00,2585.713899
    ...

The first column is ignored (it's a tag-pair placeholder). ``time_key`` is
the Monday of each Mon-Sun week. Rows are typically not date-sorted in the
export, so we sort them. ``sum(usage_usd)`` is the week's USD total.

By default we keep weeks starting on or after 2026-03-09 to match the AWS
comparison window; override with ``--from-date``.

Usage:
    python -m lucille.vendor_spend.databricks_console_csv \\
        --csv ~/Desktop/debris/2026_05_06_databricks_spend.csv

    # Different start date
    python -m lucille.vendor_spend.databricks_console_csv \\
        --csv ... --from-date 2026-01-05

    # CSV only, no PNG
    python -m lucille.vendor_spend.databricks_console_csv \\
        --csv ... --no-render
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
    VENDOR_DATABRICKS,
    write_csv as write_long_format_csv,
)
from lucille.vendor_spend.weekly_buckets import monday_of, to_date
from lucille.common.logging import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

VENDOR_LABEL = f"{VENDOR_DATABRICKS} (Console export)"
SOURCE_LABEL = "databricks_console_csv"
DEFAULT_FROM_DATE = date(2026, 3, 9)  # Mon, matches AWS comparison window

TIME_COLUMN = "time_key"
USD_COLUMN = "sum(usage_usd)"


# ---- pure ------------------------------------------------------------------

@dataclass(frozen=True)
class ParsedDatabricksCsv:
    weekly_rows: list[tuple[date, float]]  # (week_start_monday, usd)
    skipped_rows: int
    realigned_rows: int  # rows whose time_key wasn't a Monday and got snapped


def _strip_bom(s: str) -> str:
    return s.lstrip("\ufeff")


def parse_databricks_console_csv(csv_text: str) -> ParsedDatabricksCsv:
    """
    Parse the Databricks console weekly-spend CSV into ``(week_start, usd)``
    rows. Pure function.

    * The first column (``custom_tag_key_value_pairs``) is ignored.
    * Rows whose ``time_key`` doesn't parse as a date are skipped.
    * ``time_key`` values that aren't a Monday are snapped to the Monday of
      their ISO week and counted in ``realigned_rows`` so the caller can
      decide whether to warn.
    * If two rows share the same week-start (e.g. after realignment) the
      USD values are summed.
    * Output is sorted ascending by week-start.
    """
    text = _strip_bom(csv_text)
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return ParsedDatabricksCsv(weekly_rows=[], skipped_rows=0, realigned_rows=0)

    fields = set(reader.fieldnames)
    if TIME_COLUMN not in fields or USD_COLUMN not in fields:
        raise ValueError(
            f"Databricks CSV missing required columns; need {TIME_COLUMN!r} "
            f"and {USD_COLUMN!r}, got {sorted(fields)}"
        )

    totals: dict[date, float] = {}
    skipped = 0
    realigned = 0
    for raw in reader:
        ts = raw.get(TIME_COLUMN)
        if not ts:
            skipped += 1
            continue
        try:
            d = to_date(ts.split(" ")[0])  # tolerate "YYYY-MM-DD HH:MM:SS"
        except (TypeError, ValueError):
            skipped += 1
            continue
        try:
            usd = float(raw.get(USD_COLUMN) or 0.0)
        except ValueError:
            logger.warning("Skipping row with bad USD value: %s", raw.get(USD_COLUMN))
            skipped += 1
            continue
        ws = monday_of(d)
        if ws != d:
            realigned += 1
        totals[ws] = totals.get(ws, 0.0) + usd

    weekly = sorted(totals.items())
    return ParsedDatabricksCsv(
        weekly_rows=weekly,
        skipped_rows=skipped,
        realigned_rows=realigned,
    )


def filter_from(
    weekly_rows: list[tuple[date, float]],
    from_date: date,
) -> list[tuple[date, float]]:
    """Keep only rows with ``week_start >= from_date``. Pure."""
    return [(ws, usd) for ws, usd in weekly_rows if ws >= from_date]


def build_spend_rows_from_databricks_weekly(
    weekly_rows: list[tuple[date, float]],
    fetched_at: str,
) -> list[SpendRow]:
    """Wrap each ``(week_start, usd)`` pair in a long-format ``SpendRow``."""
    return [
        SpendRow(
            week_start=ws,
            vendor=VENDOR_LABEL,
            amount_usd=round(usd, 2),
            source=SOURCE_LABEL,
            fetched_at=fetched_at,
        )
        for ws, usd in weekly_rows
    ]


# ---- side-effecting --------------------------------------------------------

def read_csv_file(path: Path) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def derive_output_csv_path(input_csv: Path, output_dir: Path) -> Path:
    today = date.today().strftime("%Y_%m_%d")
    return output_dir / f"{today}_databricks_console_weekly.csv"


# ---- CLI -------------------------------------------------------------------

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Sort, filter, and re-emit a Databricks console weekly-spend CSV "
            "in the same long format as fetch_vendor_spend, then render a "
            "comparison bar chart."
        )
    )
    p.add_argument("--csv", required=True, help="Path to the Databricks console CSV.")
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

    logger.info("Reading Databricks console export from %s", csv_path)
    parsed = parse_databricks_console_csv(read_csv_file(csv_path))
    logger.info(
        "Parsed %d weekly rows (skipped %d, realigned %d non-Monday)",
        len(parsed.weekly_rows), parsed.skipped_rows, parsed.realigned_rows,
    )
    if parsed.realigned_rows:
        logger.warning(
            "Some rows had a time_key that wasn't a Monday; snapped to ISO-week "
            "start. Verify the export is producing Mon-Sun weeks."
        )

    in_window = filter_from(parsed.weekly_rows, from_date)
    logger.info(
        "Kept %d rows on or after %s (dropped %d earlier weeks)",
        len(in_window), from_date, len(parsed.weekly_rows) - len(in_window),
    )
    if not in_window:
        logger.error("No weeks on or after %s; aborting.", from_date)
        return 1

    fetched_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    spend_rows = build_spend_rows_from_databricks_weekly(in_window, fetched_at)
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
            f"Databricks weekly spend (console export) — "
            f"{len(spend_rows)} weeks since {from_date}"
        ),
    )
    logger.info("Wrote PNG to %s", png_path)
    logger.info("Total across window: $%.2f", sum(summary["totals_by_vendor"].values()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
