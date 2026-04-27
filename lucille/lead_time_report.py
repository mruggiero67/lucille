"""
Lead Time Visualization Report

Reads a lead time changes CSV and produces:
  1. Lead time distribution bar chart  (PNG)
  2. Weekly trends line chart          (PNG)
  3. Per-repository performance table  (CSV)

Usage:
    python -m lucille.lead_time_report \\
        --input ~/Desktop/debris/2026_04_23_lead_time_changes_detailed.csv \\
        [--output-dir ~/Desktop/debris] \\
        [--since 2026-01-01] \\
        [--until 2026-04-24] \\
        [--weeks 12]
"""
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

try:
    from lucille.lead_time.aggregations import (
        compute_repo_stats,
        compute_weekly_stats,
        filter_valid_records,
    )
    from lucille.lead_time.visualizations import (
        render_distribution_chart,
        render_trends_chart,
    )
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from lucille.lead_time.aggregations import (
        compute_repo_stats,
        compute_weekly_stats,
        filter_valid_records,
    )
    from lucille.lead_time.visualizations import (
        render_distribution_chart,
        render_trends_chart,
    )

logging.basicConfig(
    format="%(levelname)-8s %(asctime)s %(filename)s:%(lineno)d %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = Path.home() / "Desktop" / "debris"
DEFAULT_INPUT = Path.home() / "Desktop" / "debris" / "2026_04_23_lead_time_changes_detailed.csv"


def _datestamp() -> str:
    return datetime.now().strftime("%Y_%m_%d")


def load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8")
    for col in ("deployed_at", "ticket_started"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    logger.info(f"Loaded {len(df)} records from {path}")
    return df


def write_repo_csv(repo_df: pd.DataFrame, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{_datestamp()}_lead_time_repo_performance.csv"
    repo_df.to_csv(path, index=False, encoding="utf-8-sig")
    logger.info(f"Repo performance CSV: {path}  ({len(repo_df)} rows including totals)")
    return path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate lead time visualizations and repo performance report."
    )
    p.add_argument(
        "--input", "-i",
        type=Path,
        default=DEFAULT_INPUT,
        metavar="PATH",
        help=f"Path to lead time changes CSV (default: {DEFAULT_INPUT})",
    )
    p.add_argument(
        "--output-dir", "-o",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        metavar="DIR",
        help=f"Output directory for PNGs and CSVs (default: {DEFAULT_OUTPUT_DIR})",
    )
    p.add_argument(
        "--since",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Filter to records deployed on or after this date",
    )
    p.add_argument(
        "--until",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Filter to records deployed on or before this date",
    )
    p.add_argument(
        "--weeks",
        type=int,
        default=12,
        metavar="N",
        help="Number of most-recent weeks to show in the trends chart (default: 12)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    input_path = args.input.expanduser()
    output_dir = args.output_dir.expanduser()

    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        raise SystemExit(1)

    df = load_csv(input_path)

    if args.since:
        since = pd.Timestamp(args.since)
        df = df[df["deployed_at"] >= since]
        logger.info(f"Filtered to records on/after {args.since}: {len(df)} remaining")

    if args.until:
        until = pd.Timestamp(args.until)
        df = df[df["deployed_at"] <= until]
        logger.info(f"Filtered to records on/before {args.until}: {len(df)} remaining")

    df, excluded = filter_valid_records(df)
    if excluded:
        logger.info(f"{excluded} records excluded due to invalid lead_time_hours")

    if df.empty:
        logger.error("No valid records remaining after filtering — nothing to render")
        raise SystemExit(1)

    logger.info(f"Processing {len(df)} valid records")
    ds = _datestamp()

    # 1. Distribution chart
    date_label = f"through {args.until}" if args.until else ds.replace("_", "-")
    dist_path = output_dir / f"{ds}_lead_time_distribution.png"
    render_distribution_chart(df, dist_path, date_label=date_label)

    # 2. Trends chart (cap to --weeks most recent)
    weekly_df = compute_weekly_stats(df)
    if len(weekly_df) > args.weeks:
        weekly_df = weekly_df.tail(args.weeks).reset_index(drop=True)
    trends_path = output_dir / f"{ds}_lead_time_trends.png"
    render_trends_chart(weekly_df, trends_path)

    # 3. Repo performance CSV
    repo_df = compute_repo_stats(df)
    write_repo_csv(repo_df, output_dir)

    total_changes = int(weekly_df["change_count"].sum()) if not weekly_df.empty else 0
    logger.info(
        f"Done — {len(df)} records across {len(weekly_df)} weeks "
        f"({total_changes} in trends window)"
    )


if __name__ == "__main__":
    main()
