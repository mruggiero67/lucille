#!/usr/bin/env python3
"""
SUP Project Cycle Time Analysis

Analyzes cycle time for Engineering Support (SUP) tickets over the last 8 weeks.
Generates CSV of all issues and a bar chart showing average cycle time per week.
"""

import argparse
import logging
import math
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
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

def calculate_cycle_time_days(created: datetime, resolved: datetime) -> float:
    """
    Calculate cycle time in days between creation and resolution.

    Args:
        created: Issue creation timestamp
        resolved: Issue resolution timestamp

    Returns:
        Cycle time in days (float)
    """
    if not created or not resolved:
        return 0.0
    delta = resolved - created
    return delta.total_seconds() / 86400  # Convert to days


def get_week_label(date: datetime) -> str:
    """
    Get week label for a given date (e.g., "2026-W06").

    Args:
        date: Datetime object

    Returns:
        Week label in ISO week format
    """
    return date.strftime("%Y-W%U")


def group_issues_by_week(issues_data: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Group issues by the week they were resolved.

    Args:
        issues_data: List of issue dictionaries with cycle time data

    Returns:
        Dictionary mapping week labels to lists of issues
    """
    grouped = defaultdict(list)
    for issue in issues_data:
        week = issue.get('resolved_week')
        if week:
            grouped[week].append(issue)
    return dict(grouped)


def calculate_weekly_averages(grouped_issues: Dict[str, List[Dict]]) -> List[Tuple[str, float, int]]:
    """
    Calculate average cycle time per week.

    Args:
        grouped_issues: Dictionary of week -> list of issues

    Returns:
        List of tuples (week_label, average_cycle_time, issue_count)
    """
    weekly_stats = []
    for week, issues in sorted(grouped_issues.items()):
        cycle_times = [issue['cycle_time_days'] for issue in issues if issue['cycle_time_days'] > 0]
        if cycle_times:
            avg_cycle_time = sum(cycle_times) / len(cycle_times)
        else:
            avg_cycle_time = 0.0
        weekly_stats.append((week, avg_cycle_time, len(issues)))
    return weekly_stats


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


def build_cycle_time_summary(
    weekly_stats: List[Tuple[str, float, int]],
    start_date: str,
    end_date: str,
) -> List[str]:
    """
    Build plain-text summary lines for the cycle time report.

    Args:
        weekly_stats: List of (week_label, avg_cycle_time_days, issue_count).
        start_date: Analysis period start date string (YYYY-MM-DD).
        end_date: Analysis period end date string (YYYY-MM-DD).

    Returns:
        List of plain-text lines suitable for a summary .txt file.
    """
    if not weekly_stats:
        return [
            f"Date range: {start_date} to {end_date}",
            "No resolved tickets found in this period.",
        ]

    total_tickets = sum(count for _, _, count in weekly_stats)
    avg_values = [avg for _, avg, _ in weekly_stats if avg > 0]

    if not avg_values:
        return [
            f"Date range: {start_date} to {end_date} // Tickets resolved: {total_tickets}",
            "Average cycle time: N/A",
        ]

    overall_avg = sum(avg_values) / len(avg_values)
    sorted_avgs = sorted(avg_values)
    median = sorted_avgs[len(sorted_avgs) // 2]

    trend = classify_trend(avg_values)

    if trend in ('growing', 'shrinking') and len(avg_values) >= 2:
        mid = len(avg_values) // 2
        first_avg = sum(avg_values[:mid]) / mid
        second_avg = sum(avg_values[mid:]) / len(avg_values[mid:])
        trend_detail = (
            f"{trend.capitalize()} — recent weeks averaged {second_avg:.1f} days "
            f"vs. {first_avg:.1f} days in earlier weeks"
        )
    elif trend == 'highly variable':
        trend_detail = (
            f"Highly variable — weekly averages ranged from "
            f"{min(avg_values):.1f} to {max(avg_values):.1f} days"
        )
    elif trend == 'stable':
        trend_detail = f"Stable — weekly averages consistent around {overall_avg:.1f} days"
    else:
        trend_detail = trend.capitalize()

    return [
        f"Date range: {start_date} to {end_date} // Tickets resolved: {total_tickets}",
        f"Average cycle time: {overall_avg:.1f} days // Median: {median:.1f} days",
        f"Trend: {trend_detail}",
    ]


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
    Fetch SUP project issues that were resolved in the date range.

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
        f'status IN (Done, Resolved, Closed) AND '
        f'resolutiondate >= "{start_date}" AND '
        f'resolutiondate <= "{end_date}"'
    )

    logger.info(f"Fetching SUP issues with JQL: {jql}")

    fields = ['key', 'summary', 'created', 'resolutiondate', 'status', 'assignee', 'reporter']

    issues = fetch_all_issues(
        session=session,
        base_url=base_url,
        jql=jql,
        fields=fields
    )

    logger.info(f"Fetched {len(issues)} SUP issues")
    return issues


def process_issues(issues: List[Dict]) -> List[Dict]:
    """
    Process raw Jira issues and calculate cycle times.

    Args:
        issues: List of Jira issue dictionaries from API

    Returns:
        List of processed issue dictionaries with cycle time data
    """
    logger.info(f"Processing {len(issues)} issues")
    processed_data = []

    for issue in issues:
        try:
            fields = issue.get('fields', {})

            # Extract dates
            created_str = fields.get('created')
            resolved_str = fields.get('resolutiondate')

            if not created_str or not resolved_str:
                logger.warning(f"Skipping {issue.get('key')}: missing dates")
                continue

            created = date_parser.parse(created_str)
            resolved = date_parser.parse(resolved_str)

            # Calculate cycle time
            cycle_time = calculate_cycle_time_days(created, resolved)

            # Get week label
            week = get_week_label(resolved)

            # Extract other fields
            assignee = fields.get('assignee', {})
            assignee_name = assignee.get('displayName', 'Unassigned') if assignee else 'Unassigned'

            reporter = fields.get('reporter', {})
            reporter_name = reporter.get('displayName', 'Unknown') if reporter else 'Unknown'

            processed_data.append({
                'key': issue.get('key'),
                'summary': fields.get('summary', ''),
                'created': created.strftime('%Y-%m-%d %H:%M'),
                'resolved': resolved.strftime('%Y-%m-%d %H:%M'),
                'cycle_time_days': round(cycle_time, 2),
                'resolved_week': week,
                'status': fields.get('status', {}).get('name', ''),
                'assignee': assignee_name,
                'reporter': reporter_name
            })

        except Exception as e:
            issue_key = issue.get('key', 'UNKNOWN')
            logger.error(f"Error processing issue {issue_key}: {e}")

    logger.info(f"Successfully processed {len(processed_data)} issues")
    return processed_data


def save_issues_csv(issues_data: List[Dict], output_path: str):
    """
    Save issues data to CSV file.

    Args:
        issues_data: List of processed issue dictionaries
        output_path: Path to save CSV file
    """
    logger.info(f"Saving issues CSV to {output_path}")

    df = pd.DataFrame(issues_data)

    # Reorder columns for better readability
    column_order = [
        'key', 'summary', 'created', 'resolved', 'cycle_time_days',
        'resolved_week', 'status', 'assignee', 'reporter'
    ]
    df = df[column_order]

    # Sort by resolved date
    df = df.sort_values('resolved', ascending=False)

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


def create_cycle_time_chart(
    weekly_stats: List[Tuple[str, float, int]],
    output_path: str
):
    """
    Create bar chart showing average cycle time per week.

    Args:
        weekly_stats: List of tuples (week_label, avg_cycle_time, count)
        output_path: Path to save chart PNG
    """
    logger.info(f"Creating cycle time chart at {output_path}")

    if not weekly_stats:
        logger.warning("No weekly stats to chart")
        return

    weeks, avg_times, counts = zip(*weekly_stats)

    fig, ax = plt.subplots(figsize=(12, 6))

    bars = ax.bar(weeks, avg_times, color='#2196F3', alpha=0.8)

    ax.set_xlabel('Week', fontsize=12, fontweight='bold')
    ax.set_ylabel('Average Cycle Time (days)', fontsize=12, fontweight='bold')
    ax.set_title('SUP Project: Average Cycle Time by Week', fontsize=14, fontweight='bold')
    ax.grid(axis='y', alpha=0.3, linestyle='--')

    # Rotate x-axis labels for better readability
    plt.xticks(rotation=45, ha='right')

    # Add count labels on bars
    for bar, count in zip(bars, counts):
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            height,
            f'{height:.1f}d\n(n={count})',
            ha='center',
            va='bottom',
            fontsize=9
        )

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

    logger.info(f"Chart saved successfully")


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
        description='Analyze SUP project cycle time over the last 8 weeks',
        formatter_class=argparse.RawDescriptionHelpFormatter
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


def main():
    """Main execution function."""
    args = parse_arguments()

    # Configure logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    logger.info(f"Starting SUP Cycle Time Analysis (last {args.weeks} weeks)")

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)
    logger.info(f"Output directory: {output_dir}")

    # Load configuration
    config = load_config(args.config)

    # Extract Jira configuration
    base_url = config.get('url', '').replace('/rest/api/3/search/jql', '')
    if not base_url:
        base_url = 'https://jarisinc.atlassian.net'

    username = config.get('email', '')
    api_token = config.get('api_token', '')

    if not username or not api_token:
        logger.error("Missing Jira credentials in config file")
        return

    # Create Jira session
    session = create_jira_session(
        base_url=base_url,
        username=username,
        api_token=api_token
    )

    # Get date range
    start_date, end_date = get_date_range(args.weeks)
    logger.info(f"Analyzing period: {start_date} to {end_date}")

    # Fetch issues
    issues = fetch_sup_issues(
        session=session,
        base_url=base_url,
        start_date=start_date,
        end_date=end_date
    )

    if not issues:
        logger.warning("No issues found for the given criteria")
        return

    # Process issues
    issues_data = process_issues(issues)

    if not issues_data:
        logger.warning("No valid issues to analyze")
        return

    # Group by week and calculate averages
    grouped = group_issues_by_week(issues_data)
    weekly_stats = calculate_weekly_averages(grouped)

    # Generate timestamp for output files
    timestamp = datetime.now().strftime("%Y_%m_%d")

    # Generate output file paths
    csv_path = output_dir / f"{timestamp}_sup_cycle_time.csv"
    chart_path = output_dir / f"{timestamp}_sup_cycle_time_chart.png"
    summary_path = output_dir / f"{timestamp}_sup_cycle_time_summary.txt"

    # Save outputs
    save_issues_csv(issues_data, str(csv_path))
    create_cycle_time_chart(weekly_stats, str(chart_path))
    summary_lines = build_cycle_time_summary(weekly_stats, start_date, end_date)
    save_summary_txt(summary_lines, str(summary_path))

    # Log summary statistics
    logger.info("\n" + "=" * 60)
    logger.info("SUMMARY STATISTICS")
    logger.info("=" * 60)
    logger.info(f"Total issues analyzed: {len(issues_data)}")

    all_cycle_times = [issue['cycle_time_days'] for issue in issues_data]
    if all_cycle_times:
        logger.info(f"Average cycle time: {sum(all_cycle_times) / len(all_cycle_times):.2f} days")
        logger.info(f"Median cycle time: {sorted(all_cycle_times)[len(all_cycle_times) // 2]:.2f} days")
        logger.info(f"Min cycle time: {min(all_cycle_times):.2f} days")
        logger.info(f"Max cycle time: {max(all_cycle_times):.2f} days")

    logger.info("\nWeekly Breakdown:")
    for week, avg_time, count in weekly_stats:
        logger.info(f"  {week}: {avg_time:.2f} days (n={count})")

    logger.info("=" * 60)
    logger.info("\nAnalysis complete! Generated files:")
    logger.info(f"  1. CSV    : {csv_path}")
    logger.info(f"  2. Chart  : {chart_path}")
    logger.info(f"  3. Summary: {summary_path}")


if __name__ == '__main__':
    main()
