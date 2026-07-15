#!/usr/bin/env python3
"""
SUP Project Cycle Time Analysis

Analyzes cycle time for Engineering Support (SUP) tickets over the last N weeks.
Generates a CSV of all issues and a bar chart showing average cycle time per week.

Most of the machinery is shared with sup_ticket_volume via lucille.jira.support;
this file contains only the pieces genuinely specific to cycle time
(JQL, per-issue cycle-time computation, summary phrasing, chart tuning).
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

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
# Pure functions specific to cycle time
# ---------------------------------------------------------------------------


def calculate_cycle_time_days(created: datetime, resolved: datetime) -> float:
    """Days between creation and resolution as a float."""
    if not created or not resolved:
        return 0.0
    return (resolved - created).total_seconds() / 86400


def calculate_weekly_averages(
    grouped: Dict[str, List[Dict]],
) -> List[Tuple[str, float, int]]:
    """Return ``[(week, avg_cycle_time_days, count), ...]`` sorted by week."""
    result: List[Tuple[str, float, int]] = []
    for week, issues in sorted(grouped.items()):
        cycle_times = [i["cycle_time_days"] for i in issues if i["cycle_time_days"] > 0]
        avg = sum(cycle_times) / len(cycle_times) if cycle_times else 0.0
        result.append((week, avg, len(issues)))
    return result


def build_cycle_time_summary(
    weekly_stats: List[Tuple[str, float, int]],
    start_date: str,
    end_date: str,
) -> List[str]:
    """Build the plain-text summary lines for the cycle-time report."""
    if not weekly_stats:
        return [
            f"Date range: {start_date} to {end_date}",
            "No resolved tickets found in this period.",
        ]

    total = sum(count for _, _, count in weekly_stats)
    avg_values = [avg for _, avg, _ in weekly_stats if avg > 0]

    if not avg_values:
        return [
            f"Date range: {start_date} to {end_date} // Tickets resolved: {total}",
            "Average cycle time: N/A",
        ]

    overall_avg = sum(avg_values) / len(avg_values)
    median = sorted(avg_values)[len(avg_values) // 2]
    trend = classify_trend(avg_values)

    if trend in ("growing", "shrinking") and len(avg_values) >= 2:
        mid = len(avg_values) // 2
        first_avg = sum(avg_values[:mid]) / mid
        second_avg = sum(avg_values[mid:]) / len(avg_values[mid:])
        trend_detail = (
            f"{trend.capitalize()} — recent weeks averaged {second_avg:.1f} days "
            f"vs. {first_avg:.1f} days in earlier weeks"
        )
    elif trend == "highly variable":
        trend_detail = (
            f"Highly variable — weekly averages ranged from "
            f"{min(avg_values):.1f} to {max(avg_values):.1f} days"
        )
    elif trend == "stable":
        trend_detail = f"Stable — weekly averages consistent around {overall_avg:.1f} days"
    else:
        trend_detail = trend.capitalize()

    return [
        f"Date range: {start_date} to {end_date} // Tickets resolved: {total}",
        f"Average cycle time: {overall_avg:.1f} days // Median: {median:.1f} days",
        f"Trend: {trend_detail}",
    ]


def process_issues(issues: List[Dict]) -> List[Dict]:
    """Turn raw Jira issues into rows with cycle-time and week fields."""
    logger.info(f"Processing {len(issues)} issues")
    processed: List[Dict] = []
    for issue in issues:
        try:
            fields = issue.get("fields", {})
            created_str = fields.get("created")
            resolved_str = fields.get("resolutiondate")
            if not created_str or not resolved_str:
                logger.warning(f"Skipping {issue.get('key')}: missing dates")
                continue
            created = date_parser.parse(created_str)
            resolved = date_parser.parse(resolved_str)

            assignee = fields.get("assignee") or {}
            reporter = fields.get("reporter") or {}

            processed.append({
                "key": issue.get("key"),
                "summary": fields.get("summary", ""),
                "created": created.strftime("%Y-%m-%d %H:%M"),
                "resolved": resolved.strftime("%Y-%m-%d %H:%M"),
                "cycle_time_days": round(calculate_cycle_time_days(created, resolved), 2),
                "resolved_week": get_week_label(resolved),
                "status": (fields.get("status") or {}).get("name", ""),
                "assignee": assignee.get("displayName", "Unassigned"),
                "reporter": reporter.get("displayName", "Unknown"),
            })
        except Exception as e:
            logger.error(f"Error processing issue {issue.get('key', 'UNKNOWN')}: {e}")
    logger.info(f"Successfully processed {len(processed)} issues")
    return processed


# ---------------------------------------------------------------------------
# I/O: Jira fetch specific to cycle-time
# ---------------------------------------------------------------------------


CSV_COLUMNS = [
    "key", "summary", "created", "resolved", "cycle_time_days",
    "resolved_week", "status", "assignee", "reporter",
]


def fetch_sup_issues(
    session: requests.Session,
    base_url: str,
    start_date: str,
    end_date: str,
) -> List[Dict]:
    """Fetch SUP tickets resolved within the window."""
    jql = (
        f'project = SUP AND '
        f'status IN (Done, Resolved, Closed) AND '
        f'resolutiondate >= "{start_date}" AND '
        f'resolutiondate <= "{end_date}"'
    )
    logger.info(f"Fetching SUP issues with JQL: {jql}")
    fields = ["key", "summary", "created", "resolutiondate", "status", "assignee", "reporter"]
    issues = fetch_all_issues(session=session, base_url=base_url, jql=jql, fields=fields)
    logger.info(f"Fetched {len(issues)} SUP issues")
    return issues


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = build_common_parser(
        description="Analyze SUP project cycle time over the last N weeks",
    )
    args = parser.parse_args()

    logging.getLogger().setLevel(getattr(logging, args.log_level))
    logger.info(f"Starting SUP Cycle Time Analysis (last {args.weeks} weeks)")

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

    grouped = group_by_week(issues_data, "resolved_week")
    weekly_stats = calculate_weekly_averages(grouped)

    timestamp = datetime.now().strftime("%Y_%m_%d")
    csv_path = output_dir / f"{timestamp}_sup_cycle_time.csv"
    chart_path = output_dir / f"{timestamp}_sup_cycle_time_chart.png"
    summary_path = output_dir / f"{timestamp}_sup_cycle_time_summary.txt"

    save_issues_csv(issues_data, str(csv_path), columns=CSV_COLUMNS, sort_by="resolved")

    weeks, avgs, counts = zip(*weekly_stats) if weekly_stats else ([], [], [])
    bar_labels = [f"{avg:.1f}d\n(n={c})" for avg, c in zip(avgs, counts)]
    create_weekly_bar_chart(
        weeks, avgs,
        output_path=str(chart_path),
        color="#2196F3",
        ylabel="Average Cycle Time (days)",
        title="SUP Project: Average Cycle Time by Week",
        bar_labels=bar_labels,
    )

    save_summary_txt(
        build_cycle_time_summary(weekly_stats, start_date, end_date),
        str(summary_path),
    )

    _log_summary(issues_data, weekly_stats, csv_path, chart_path, summary_path)


def _log_summary(
    issues_data: List[Dict],
    weekly_stats: List[Tuple[str, float, int]],
    csv_path: Path,
    chart_path: Path,
    summary_path: Path,
) -> None:
    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY STATISTICS")
    logger.info("=" * 60)
    logger.info(f"Total issues analyzed: {len(issues_data)}")
    cycle_times = [i["cycle_time_days"] for i in issues_data]
    if cycle_times:
        logger.info(f"Average cycle time: {sum(cycle_times) / len(cycle_times):.2f} days")
        logger.info(f"Median cycle time: {sorted(cycle_times)[len(cycle_times) // 2]:.2f} days")
        logger.info(f"Min cycle time: {min(cycle_times):.2f} days")
        logger.info(f"Max cycle time: {max(cycle_times):.2f} days")
    logger.info("\nWeekly Breakdown:")
    for week, avg, count in weekly_stats:
        logger.info(f"  {week}: {avg:.2f} days (n={count})")
    logger.info("=" * 60)
    logger.info("\nAnalysis complete! Generated files:")
    logger.info(f"  1. CSV    : {csv_path}")
    logger.info(f"  2. Chart  : {chart_path}")
    logger.info(f"  3. Summary: {summary_path}")


if __name__ == "__main__":
    main()
