#!/usr/bin/env python3
"""
SUP Project Ticket Volume Analysis

Analyzes the volume of tickets created per week for the Engineering Support (SUP)
project over the last N weeks. Generates a CSV of all issues and a bar chart
showing ticket count per week. Useful for correlating ticket volume with cycle
time trends.
"""

import argparse
import logging
import math
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from pathlib import Path
from collections import defaultdict

import yaml
import pandas as pd
import matplotlib.pyplot as plt
import requests
from dateutil import parser as date_parser

try:
    from .utils import create_jira_session, fetch_all_issues
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from lucille.jira.utils import create_jira_session, fetch_all_issues

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Pure Functions (No Side Effects)
# ============================================================================

def get_week_label(date: datetime) -> str:
    """
    Get week label for a given date (e.g., "2026-W06").

    Args:
        date: Datetime object

    Returns:
        Week label in ISO week format
    """
    return date.strftime("%Y-W%U")


def get_date_range(weeks_back: int) -> Tuple[str, str]:
    """
    Calculate start and end dates for the analysis period.

    Args:
        weeks_back: Number of weeks to look back

    Returns:
        Tuple of (start_date, end_date) in YYYY-MM-DD format
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(weeks=weeks_back)
    return (
        start_date.strftime("%Y-%m-%d"),
        end_date.strftime("%Y-%m-%d")
    )


def extract_issue_fields(issue: Dict) -> Optional[Dict]:
    """
    Extract relevant fields from a raw Jira issue dictionary.

    Args:
        issue: Raw Jira issue dict from API

    Returns:
        Processed dict with key fields, or None if required fields are missing
        or the created date cannot be parsed.
    """
    fields = issue.get('fields', {})
    created_str = fields.get('created')

    if not created_str:
        return None

    try:
        created = date_parser.parse(created_str)
    except (ValueError, TypeError):
        return None

    week = get_week_label(created)

    assignee = fields.get('assignee') or {}
    assignee_name = assignee.get('displayName', 'Unassigned')

    reporter = fields.get('reporter') or {}
    reporter_name = reporter.get('displayName', 'Unknown')

    status = fields.get('status') or {}

    return {
        'key': issue.get('key', ''),
        'summary': fields.get('summary', ''),
        'created': created.strftime('%Y-%m-%d %H:%M'),
        'created_week': week,
        'status': status.get('name', ''),
        'assignee': assignee_name,
        'reporter': reporter_name,
    }


def group_issues_by_created_week(issues_data: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Group issues by the week they were created.

    Args:
        issues_data: List of processed issue dictionaries

    Returns:
        Dictionary mapping week labels to lists of issues
    """
    grouped: Dict[str, List[Dict]] = defaultdict(list)
    for issue in issues_data:
        week = issue.get('created_week')
        if week:
            grouped[week].append(issue)
    return dict(grouped)


def calculate_weekly_counts(grouped_issues: Dict[str, List[Dict]]) -> List[Tuple[str, int]]:
    """
    Calculate ticket count per week, sorted chronologically.

    Args:
        grouped_issues: Dictionary of week -> list of issues

    Returns:
        List of (week_label, ticket_count) tuples sorted by week
    """
    return [
        (week, len(issues))
        for week, issues in sorted(grouped_issues.items())
    ]


def classify_trend(
    values: List[float],
    variability_threshold: float = 0.4,
    change_threshold: float = 0.1,
) -> str:
    """
    Classify a time-ordered sequence of weekly values as a trend.

    Uses coefficient of variation (CV) to detect high variability first, then
    compares the first-half vs second-half average to determine direction.

    Args:
        values: Weekly metric values in chronological order.
        variability_threshold: CV above this → 'highly variable' (default 0.4).
        change_threshold: Fractional half-over-half change required for
                          'growing'/'shrinking' (default 0.1 = 10%).

    Returns:
        One of: 'growing', 'shrinking', 'stable', 'highly variable',
        or 'insufficient data'.
    """
    if len(values) < 2:
        return 'insufficient data'

    mean = sum(values) / len(values)
    if mean == 0:
        return 'stable'

    variance = sum((v - mean) ** 2 for v in values) / len(values)
    cv = math.sqrt(variance) / mean
    if cv > variability_threshold:
        return 'highly variable'

    mid = len(values) // 2
    first_avg = sum(values[:mid]) / mid
    second_avg = sum(values[mid:]) / len(values[mid:])

    if first_avg == 0:
        return 'growing' if second_avg > 0 else 'stable'

    ratio = second_avg / first_avg
    if ratio > 1 + change_threshold:
        return 'growing'
    if ratio < 1 - change_threshold:
        return 'shrinking'
    return 'stable'


def build_volume_summary(
    weekly_counts: List[Tuple[str, int]],
    start_date: str,
    end_date: str,
) -> List[str]:
    """
    Build plain-text summary lines for the ticket volume report.

    Args:
        weekly_counts: List of (week_label, ticket_count) tuples.
        start_date: Analysis period start date string (YYYY-MM-DD).
        end_date: Analysis period end date string (YYYY-MM-DD).

    Returns:
        List of plain-text lines suitable for a summary .txt file.
    """
    if not weekly_counts:
        return [
            f"Date range: {start_date} to {end_date}",
            "No tickets found in this period.",
        ]

    counts = [c for _, c in weekly_counts]
    total = sum(counts)
    avg = total / len(counts)

    trend = classify_trend([float(c) for c in counts])

    if trend in ('growing', 'shrinking') and len(counts) >= 2:
        mid = len(counts) // 2
        first_avg = sum(counts[:mid]) / mid
        second_avg = sum(counts[mid:]) / len(counts[mid:])
        trend_detail = (
            f"{trend.capitalize()} — recent weeks averaged {second_avg:.1f} tickets "
            f"vs. {first_avg:.1f} tickets in earlier weeks"
        )
    elif trend == 'highly variable':
        trend_detail = (
            f"Highly variable — weekly counts ranged from "
            f"{min(counts)} to {max(counts)} tickets"
        )
    elif trend == 'stable':
        trend_detail = f"Stable — weekly counts consistent around {avg:.1f} tickets per week"
    else:
        trend_detail = trend.capitalize()

    return [
        f"Date range: {start_date} to {end_date} // Total tickets created: {total}",
        f"Average tickets per week: {avg:.1f}",
        f"Trend: {trend_detail}",
    ]


def process_issues(issues: List[Dict]) -> List[Dict]:
    """
    Process raw Jira issues and extract ticket volume data.

    Args:
        issues: List of Jira issue dictionaries from API

    Returns:
        List of processed issue dictionaries
    """
    logger.info(f"Processing {len(issues)} issues")
    processed_data = []

    for issue in issues:
        try:
            record = extract_issue_fields(issue)
            if record is None:
                logger.warning(f"Skipping {issue.get('key')}: missing or invalid created date")
                continue
            processed_data.append(record)
        except Exception as e:
            issue_key = issue.get('key', 'UNKNOWN')
            logger.error(f"Error processing issue {issue_key}: {e}")

    logger.info(f"Successfully processed {len(processed_data)} issues")
    return processed_data


# ============================================================================
# Side-Effecting Functions (I/O, Network, File Operations)
# ============================================================================

def load_config(config_path: str) -> Dict:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Configuration dictionary
    """
    logger.info(f"Loading configuration from {config_path}")
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        logger.error(f"Config file {config_path} not found")
        raise


def fetch_sup_issues(
    session: requests.Session,
    base_url: str,
    start_date: str,
    end_date: str
) -> List[Dict]:
    """
    Fetch all SUP project issues created within the date range.

    Unlike the cycle-time query, this uses the 'created' date (not resolutiondate)
    and applies no status filter, so every ticket generated in the period is counted.

    Args:
        session: Authenticated requests session
        base_url: Jira base URL
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        List of Jira issue dictionaries
    """
    jql = (
        f'project = SUP AND '
        f'created >= "{start_date}" AND '
        f'created <= "{end_date}"'
    )

    logger.info(f"Fetching SUP issues with JQL: {jql}")

    fields = ['key', 'summary', 'created', 'status', 'assignee', 'reporter']

    issues = fetch_all_issues(
        session=session,
        base_url=base_url,
        jql=jql,
        fields=fields
    )

    logger.info(f"Fetched {len(issues)} SUP issues")
    return issues


def save_issues_csv(issues_data: List[Dict], output_path: str) -> None:
    """
    Save issues data to CSV file.

    Args:
        issues_data: List of processed issue dictionaries
        output_path: Path to save CSV file
    """
    logger.info(f"Saving issues CSV to {output_path}")

    df = pd.DataFrame(issues_data)

    column_order = [
        'key', 'summary', 'created', 'created_week',
        'status', 'assignee', 'reporter'
    ]
    df = df[column_order]
    df = df.sort_values('created', ascending=False)

    df.to_csv(output_path, index=False)
    logger.info(f"CSV saved successfully with {len(df)} rows")


def save_summary_txt(lines: List[str], output_path: str) -> None:
    """
    Write summary lines to a plain text file.

    Args:
        lines: List of text lines to write.
        output_path: Destination file path.
    """
    logger.info(f"Saving summary to {output_path}")
    Path(output_path).write_text('\n'.join(lines) + '\n')
    logger.info("Summary saved successfully")


def create_volume_chart(
    weekly_counts: List[Tuple[str, int]],
    output_path: str
) -> None:
    """
    Create bar chart showing ticket volume (count) per week.

    Args:
        weekly_counts: List of (week_label, ticket_count) tuples
        output_path: Path to save chart PNG
    """
    logger.info(f"Creating ticket volume chart at {output_path}")

    if not weekly_counts:
        logger.warning("No weekly counts to chart")
        return

    weeks, counts = zip(*weekly_counts)

    fig, ax = plt.subplots(figsize=(12, 6))

    bars = ax.bar(weeks, counts, color='#FF9800', alpha=0.8)

    ax.set_xlabel('Week', fontsize=12, fontweight='bold')
    ax.set_ylabel('Tickets Created', fontsize=12, fontweight='bold')
    ax.set_title('SUP Project: Ticket Volume by Week (Created Date)', fontsize=14, fontweight='bold')
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    ax.yaxis.set_major_locator(plt.MaxNLocator(integer=True))

    plt.xticks(rotation=45, ha='right')

    for bar, count in zip(bars, counts):
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            height,
            str(count),
            ha='center',
            va='bottom',
            fontsize=10,
            fontweight='bold'
        )

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

    logger.info("Chart saved successfully")


# ============================================================================
# Main Execution
# ============================================================================

def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description='Analyze SUP project ticket volume over the last N weeks',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'Example:\n'
            '  python sup_ticket_volume.py -w 12 -o ~/Desktop/debris'
        )
    )

    parser.add_argument(
        '-c', '--config',
        default='/Users/michael@jaris.io/bin/jira.yaml',
        help='Path to Jira configuration YAML file'
    )

    parser.add_argument(
        '-o', '--output-dir',
        default='/Users/michael@jaris.io/Desktop/debris',
        help='Output directory for generated files'
    )

    parser.add_argument(
        '-w', '--weeks',
        type=int,
        default=8,
        help='Number of weeks to analyze (default: 8)'
    )

    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )

    return parser.parse_args()


def main() -> None:
    """Main execution function."""
    args = parse_arguments()

    logging.getLogger().setLevel(getattr(logging, args.log_level))

    logger.info(f"Starting SUP Ticket Volume Analysis (last {args.weeks} weeks)")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)
    logger.info(f"Output directory: {output_dir}")

    config = load_config(args.config)

    base_url = config.get('url', '').replace('/rest/api/3/search/jql', '')
    if not base_url:
        base_url = 'https://jarisinc.atlassian.net'

    username = config.get('email', '')
    api_token = config.get('api_token', '')

    if not username or not api_token:
        logger.error("Missing Jira credentials in config file")
        return

    session = create_jira_session(
        base_url=base_url,
        username=username,
        api_token=api_token
    )

    start_date, end_date = get_date_range(args.weeks)
    logger.info(f"Analyzing period: {start_date} to {end_date}")

    issues = fetch_sup_issues(
        session=session,
        base_url=base_url,
        start_date=start_date,
        end_date=end_date
    )

    if not issues:
        logger.warning("No issues found for the given criteria")
        return

    issues_data = process_issues(issues)

    if not issues_data:
        logger.warning("No valid issues to analyze")
        return

    grouped = group_issues_by_created_week(issues_data)
    weekly_counts = calculate_weekly_counts(grouped)

    timestamp = datetime.now().strftime("%Y_%m_%d")
    csv_path = output_dir / f"{timestamp}_sup_ticket_volume.csv"
    chart_path = output_dir / f"{timestamp}_sup_ticket_volume_chart.png"
    summary_path = output_dir / f"{timestamp}_sup_ticket_volume_summary.txt"

    save_issues_csv(issues_data, str(csv_path))
    create_volume_chart(weekly_counts, str(chart_path))
    summary_lines = build_volume_summary(weekly_counts, start_date, end_date)
    save_summary_txt(summary_lines, str(summary_path))

    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY STATISTICS")
    logger.info("=" * 60)
    logger.info(f"Total issues analyzed: {len(issues_data)}")

    if weekly_counts:
        ticket_counts = [c for _, c in weekly_counts]
        logger.info(f"Average tickets/week : {sum(ticket_counts) / len(ticket_counts):.1f}")
        logger.info(f"Max tickets in a week: {max(ticket_counts)}")
        logger.info(f"Min tickets in a week: {min(ticket_counts)}")

    logger.info("\nWeekly Breakdown:")
    for week, count in weekly_counts:
        logger.info(f"  {week}: {count} tickets")

    logger.info("=" * 60)
    logger.info("\nAnalysis complete! Generated files:")
    logger.info(f"  1. CSV    : {csv_path}")
    logger.info(f"  2. Chart  : {chart_path}")
    logger.info(f"  3. Summary: {summary_path}")


if __name__ == '__main__':
    main()
