#!/usr/bin/env python3
"""
SUP Project Ticket Volume Analysis

Analyzes the volume of tickets created per week for the Engineering Support
(SUP) project over the last N weeks. Generates a CSV and a bar chart showing
ticket count per week.

Most of the machinery is shared with sup_cycle_time via lucille.jira.support;
this file contains only the pieces genuinely specific to volume
(JQL, per-issue field extraction, summary phrasing, chart tuning).
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from dateutil import parser as date_parser

from lucille.common.config import load_yaml_config
from lucille.common.logging import setup_logging
from lucille.jira.support.charts import create_weekly_bar_chart
from lucille.jira.support.cli import build_common_parser, resolve_jira_credentials
from lucille.jira.support.io import save_issues_csv, save_summary_txt
from lucille.jira.support.weekly import (
    classify_trend,
    get_date_range,
    get_week_label,
    group_by_week,
)
from lucille.jira.utils import create_jira_session, fetch_all_issues

setup_logging()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pure functions specific to volume
# ---------------------------------------------------------------------------


def extract_issue_fields(issue: Dict) -> Optional[Dict]:
    """Extract relevant fields from a raw Jira issue; return None if unusable."""
    fields = issue.get("fields", {})
    created_str = fields.get("created")
    if not created_str:
        return None
    try:
        created = date_parser.parse(created_str)
    except (ValueError, TypeError):
        return None

    assignee = fields.get("assignee") or {}
    reporter = fields.get("reporter") or {}
    status = fields.get("status") or {}

    return {
        "key": issue.get("key", ""),
        "summary": fields.get("summary", ""),
        "created": created.strftime("%Y-%m-%d %H:%M"),
        "created_week": get_week_label(created),
        "status": status.get("name", ""),
        "assignee": assignee.get("displayName", "Unassigned"),
        "reporter": reporter.get("displayName", "Unknown"),
    }


def process_issues(issues: List[Dict]) -> List[Dict]:
    """Turn raw Jira issues into rows with a ``created_week`` column."""
    logger.info(f"Processing {len(issues)} issues")
    processed: List[Dict] = []
    for issue in issues:
        try:
            record = extract_issue_fields(issue)
            if record is None:
                logger.warning(f"Skipping {issue.get('key')}: missing or invalid created date")
                continue
            processed.append(record)
        except Exception as e:
            logger.error(f"Error processing issue {issue.get('key', 'UNKNOWN')}: {e}")
    logger.info(f"Successfully processed {len(processed)} issues")
    return processed


def calculate_weekly_counts(grouped: Dict[str, List[Dict]]) -> List[Tuple[str, int]]:
    """Return ``[(week, count), ...]`` sorted chronologically."""
    return [(week, len(issues)) for week, issues in sorted(grouped.items())]


def build_volume_summary(
    weekly_counts: List[Tuple[str, int]],
    start_date: str,
    end_date: str,
) -> List[str]:
    """Build the plain-text summary lines for the volume report."""
    if not weekly_counts:
        return [
            f"Date range: {start_date} to {end_date}",
            "No tickets found in this period.",
        ]

    counts = [c for _, c in weekly_counts]
    total = sum(counts)
    avg = total / len(counts)
    trend = classify_trend([float(c) for c in counts])

    if trend in ("growing", "shrinking") and len(counts) >= 2:
        mid = len(counts) // 2
        first_avg = sum(counts[:mid]) / mid
        second_avg = sum(counts[mid:]) / len(counts[mid:])
        trend_detail = (
            f"{trend.capitalize()} — recent weeks averaged {second_avg:.1f} tickets "
            f"vs. {first_avg:.1f} tickets in earlier weeks"
        )
    elif trend == "highly variable":
        trend_detail = (
            f"Highly variable — weekly counts ranged from "
            f"{min(counts)} to {max(counts)} tickets"
        )
    elif trend == "stable":
        trend_detail = f"Stable — weekly counts consistent around {avg:.1f} tickets per week"
    else:
        trend_detail = trend.capitalize()

    return [
        f"Date range: {start_date} to {end_date} // Total tickets created: {total}",
        f"Average tickets per week: {avg:.1f}",
        f"Trend: {trend_detail}",
    ]


# ---------------------------------------------------------------------------
# I/O: Jira fetch specific to volume
# ---------------------------------------------------------------------------


CSV_COLUMNS = [
    "key", "summary", "created", "created_week",
    "status", "assignee", "reporter",
]


def fetch_sup_issues(
    session: requests.Session,
    base_url: str,
    start_date: str,
    end_date: str,
) -> List[Dict]:
    """Fetch all SUP tickets created within the window (any status)."""
    jql = (
        f'project = SUP AND '
        f'created >= "{start_date}" AND '
        f'created <= "{end_date}"'
    )
    logger.info(f"Fetching SUP issues with JQL: {jql}")
    fields = ["key", "summary", "created", "status", "assignee", "reporter"]
    issues = fetch_all_issues(session=session, base_url=base_url, jql=jql, fields=fields)
    logger.info(f"Fetched {len(issues)} SUP issues")
    return issues


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = build_common_parser(
        description="Analyze SUP project ticket volume over the last N weeks",
        epilog=(
            "Example:\n"
            "  python -m lucille.jira.sup_ticket_volume -w 12 -o ~/Desktop/debris"
        ),
    )
    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))
    logger.info(f"Starting SUP Ticket Volume Analysis (last {args.weeks} weeks)")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {output_dir}")

    config = load_yaml_config(args.config, on_missing="raise")
    base_url, username, api_token = resolve_jira_credentials(config)
    if not username or not api_token:
        logger.error("Missing Jira credentials in config file")
        return

    session = create_jira_session(base_url=base_url, username=username, api_token=api_token)

    start_date, end_date = get_date_range(args.weeks)
    logger.info(f"Analyzing period: {start_date} to {end_date}")

    issues = fetch_sup_issues(session, base_url, start_date, end_date)
    if not issues:
        logger.warning("No issues found for the given criteria")
        return

    issues_data = process_issues(issues)
    if not issues_data:
        logger.warning("No valid issues to analyze")
        return

    grouped = group_by_week(issues_data, "created_week")
    weekly_counts = calculate_weekly_counts(grouped)

    timestamp = datetime.now().strftime("%Y_%m_%d")
    csv_path = output_dir / f"{timestamp}_sup_ticket_volume.csv"
    chart_path = output_dir / f"{timestamp}_sup_ticket_volume_chart.png"
    summary_path = output_dir / f"{timestamp}_sup_ticket_volume_summary.txt"

    save_issues_csv(issues_data, str(csv_path), columns=CSV_COLUMNS, sort_by="created")

    weeks, counts = zip(*weekly_counts) if weekly_counts else ([], [])
    create_weekly_bar_chart(
        weeks, counts,
        output_path=str(chart_path),
        color="#FF9800",
        ylabel="Tickets Created",
        title="SUP Project: Ticket Volume by Week (Created Date)",
        bar_labels=[str(c) for c in counts],
        bar_label_fontsize=10,
        bar_label_fontweight="bold",
        y_integer=True,
    )

    save_summary_txt(
        build_volume_summary(weekly_counts, start_date, end_date),
        str(summary_path),
    )

    _log_summary(issues_data, weekly_counts, csv_path, chart_path, summary_path)


def _log_summary(
    issues_data: List[Dict],
    weekly_counts: List[Tuple[str, int]],
    csv_path: Path,
    chart_path: Path,
    summary_path: Path,
) -> None:
    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY STATISTICS")
    logger.info("=" * 60)
    logger.info(f"Total issues analyzed: {len(issues_data)}")
    if weekly_counts:
        counts = [c for _, c in weekly_counts]
        logger.info(f"Average tickets/week : {sum(counts) / len(counts):.1f}")
        logger.info(f"Max tickets in a week: {max(counts)}")
        logger.info(f"Min tickets in a week: {min(counts)}")
    logger.info("\nWeekly Breakdown:")
    for week, count in weekly_counts:
        logger.info(f"  {week}: {count} tickets")
    logger.info("=" * 60)
    logger.info("\nAnalysis complete! Generated files:")
    logger.info(f"  1. CSV    : {csv_path}")
    logger.info(f"  2. Chart  : {chart_path}")
    logger.info(f"  3. Summary: {summary_path}")


if __name__ == "__main__":
    main()
