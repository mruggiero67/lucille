"""
Compute percentage of story points and ticket count delivered per cost category,
per project, from a pre-filtered Jira epic CSV.

Usage:
    python cost_category_breakdown.py <CSV_PATH> [--as-of YYYY-MM-DD]

Output:
    Console report + YYYY_MM_DD_cost_category_breakdown.csv in ~/Desktop/debris
"""

import argparse
import csv
import logging
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

DEBRIS_DIR = Path.home() / "Desktop" / "debris"


def parse_date(s: str) -> Optional[date]:
    try:
        return date.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def load_epics(path: Path) -> list:
    rows = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows


def date_range(rows: list) -> tuple:
    """Return (min_date, max_date) from earliest_resolved / latest_resolved columns."""
    dates = []
    for row in rows:
        for col in ("earliest_resolved", "latest_resolved"):
            d = parse_date(row.get(col, ""))
            if d:
                dates.append(d)
    return (min(dates), max(dates)) if dates else (None, None)


def aggregate(rows: list) -> dict:
    """
    Returns:
        {project: {cost_category: {"story_points": int, "tickets": int}}}
    """
    data = defaultdict(lambda: defaultdict(lambda: {"story_points": 0, "tickets": 0}))
    for row in rows:
        project = row["project"].strip() or "(none)"
        category = row["cost_category"].strip() or "(uncategorized)"
        sp = int(row.get("story_points_sum") or 0)
        tc = int(row.get("ticket_count") or 0)
        data[project][category]["story_points"] += sp
        data[project][category]["tickets"] += tc
    return data


def compute_percentages(data: dict) -> list:
    """Flatten aggregated data into rows with percentage columns."""
    results = []
    for project in sorted(data):
        cats = data[project]
        total_sp = sum(v["story_points"] for v in cats.values())
        total_tc = sum(v["tickets"] for v in cats.values())
        for category in sorted(cats):
            sp = cats[category]["story_points"]
            tc = cats[category]["tickets"]
            results.append(
                {
                    "project": project,
                    "cost_category": category,
                    "story_points": sp,
                    "story_points_pct": round(sp / total_sp * 100, 1) if total_sp else 0.0,
                    "tickets": tc,
                    "tickets_pct": round(tc / total_tc * 100, 1) if total_tc else 0.0,
                    "total_story_points": total_sp,
                    "total_tickets": total_tc,
                }
            )
    return results


def log_table(results: list, min_date: Optional[date], max_date: Optional[date]) -> None:
    logger.info("\nEngineering Cost Category Breakdown")
    if min_date and max_date:
        logger.info("Period : %s → %s", min_date, max_date)
    logger.info("")

    col_w = {"project": 8, "cost_category": 16, "sp": 6, "sp_pct": 7, "tc": 7, "tc_pct": 7}
    header = (
        f"{'Project':<{col_w['project']}}  "
        f"{'Cost Category':<{col_w['cost_category']}}  "
        f"{'  SP':>{col_w['sp']}}  {'SP %':>{col_w['sp_pct']}}  "
        f"{'Tickets':>{col_w['tc']}}  {'Tkt %':>{col_w['tc_pct']}}"
    )
    sep = "-" * len(header)

    current_project = None
    for idx, row in enumerate(results):
        if row["project"] != current_project:
            current_project = row["project"]
            logger.info(sep)
            logger.info(header)
            logger.info(sep)

        logger.info(
            "%s  %-*s  %*d  %*.1f%%  %*d  %*.1f%%",
            f"{row['project']:<{col_w['project']}}",
            col_w["cost_category"], row["cost_category"],
            col_w["sp"], row["story_points"],
            col_w["sp_pct"], row["story_points_pct"],
            col_w["tc"], row["tickets"],
            col_w["tc_pct"], row["tickets_pct"],
        )

        next_idx = idx + 1
        if next_idx == len(results) or results[next_idx]["project"] != current_project:
            logger.info(
                "%s  %-*s  %*d  %*.1f%%  %*d  %*.1f%%",
                f"{'':>{col_w['project']}}",
                col_w["cost_category"], "TOTAL",
                col_w["sp"], row["total_story_points"],
                col_w["sp_pct"], 100.0,
                col_w["tc"], row["total_tickets"],
                col_w["tc_pct"], 100.0,
            )

    logger.info(sep)


def write_csv(results: list, as_of: date) -> Path:
    DEBRIS_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"{as_of.strftime('%Y_%m_%d')}_cost_category_breakdown.csv"
    out_path = DEBRIS_DIR / filename
    fields = [
        "project",
        "cost_category",
        "story_points",
        "story_points_pct",
        "tickets",
        "tickets_pct",
        "total_story_points",
        "total_tickets",
    ]
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("csv_path", help="Path to input Jira epic CSV")
    parser.add_argument(
        "--as-of",
        dest="as_of",
        default=None,
        help="Reference date for output filename YYYY-MM-DD (default: today)",
    )
    args = parser.parse_args()

    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        logger.error("File not found: %s", csv_path)
        sys.exit(1)

    as_of = parse_date(args.as_of) if args.as_of else date.today()

    rows = load_epics(csv_path)
    if not rows:
        logger.warning("No rows found in %s", csv_path)
        sys.exit(0)

    min_date, max_date = date_range(rows)
    data = aggregate(rows)
    results = compute_percentages(data)
    log_table(results, min_date, max_date)

    out_path = write_csv(results, as_of)
    logger.info("\nCSV written to: %s", out_path)


if __name__ == "__main__":
    main()
