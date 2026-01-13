#!/usr/bin/env python3
"""
OpsGenie Alerts Analysis
Generates a stacked bar chart showing alert counts by week and team for the last 4 weeks.
"""

import argparse
import csv
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-10s %(asctime)s %(filename)s %(lineno)d %(message)s",
)
logger = logging.getLogger(__name__)


def parse_csv(csv_path: str) -> list[dict]:
    """
    Parse the OpsGenie CSV file and return list of alert records.

    Args:
        csv_path: Path to the CSV file

    Returns:
        List of dictionaries containing alert data
    """
    alerts = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            alerts.append(row)

    logger.info(f"Parsed {len(alerts)} alerts from {csv_path}")
    return alerts


def filter_last_n_weeks(alerts: list[dict], n_weeks: int = 4) -> list[dict]:
    """
    Filter alerts to only include those from the last n weeks.

    Args:
        alerts: List of alert dictionaries
        n_weeks: Number of weeks to include

    Returns:
        Filtered list of alerts
    """
    cutoff_date = datetime.now() - timedelta(weeks=n_weeks)
    filtered_alerts = []

    for alert in alerts:
        # Parse the CreatedAt timestamp (milliseconds since epoch)
        created_at = datetime.fromtimestamp(int(alert['CreatedAt']) / 1000)
        if created_at >= cutoff_date:
            filtered_alerts.append({**alert, 'parsed_date': created_at})

    logger.info(f"Filtered to {len(filtered_alerts)} alerts from last {n_weeks} weeks")
    return filtered_alerts


def get_week_start(date: datetime) -> datetime:
    """
    Get the start of the week (Monday) for a given date.

    Args:
        date: The date to get the week start for

    Returns:
        Datetime representing the start of the week
    """
    return date - timedelta(days=date.weekday())


def aggregate_by_week_and_team(alerts: list[dict]) -> dict:
    """
    Aggregate alerts by week and team.

    Args:
        alerts: List of alert dictionaries with parsed_date

    Returns:
        Nested dictionary: {week_start: {team: count}}
    """
    aggregated = defaultdict(lambda: defaultdict(int))

    for alert in alerts:
        week_start = get_week_start(alert['parsed_date']).date()
        team = alert.get('Teams', '').strip()

        # Handle empty team names
        if not team:
            team = 'Unassigned'

        aggregated[week_start][team] += 1

    logger.info(f"Aggregated alerts into {len(aggregated)} weeks")
    return dict(aggregated)


def create_stacked_bar_chart(data: dict, output_path: str, n_weeks: int = 6) -> None:
    """
    Create a stacked bar chart showing alerts by week and team.

    Args:
        data: Nested dictionary of {week_start: {team: count}}
        output_path: Path to save the PNG output
        n_weeks: Number of weeks being analyzed (for title)
    """
    # Sort weeks chronologically
    weeks = sorted(data.keys())

    # Get all unique teams across all weeks
    all_teams = set()
    for week_data in data.values():
        all_teams.update(week_data.keys())
    all_teams = sorted(all_teams)

    logger.info(f"Found {len(all_teams)} teams: {', '.join(all_teams)}")

    # Prepare data for stacked bar chart
    team_data = {team: [] for team in all_teams}
    for week in weeks:
        for team in all_teams:
            team_data[team].append(data[week].get(team, 0))

    # Create the plot
    fig, ax = plt.subplots(figsize=(12, 7))

    # Week labels (format as "Week of MM/DD")
    week_labels = [week.strftime('%m/%d') for week in weeks]
    x_pos = range(len(weeks))

    # Create stacked bars
    bottom = [0] * len(weeks)
    colors = plt.cm.Set3(range(len(all_teams)))

    for idx, team in enumerate(all_teams):
        counts = team_data[team]
        ax.bar(x_pos, counts, bottom=bottom, label=team, color=colors[idx])
        bottom = [b + c for b, c in zip(bottom, counts)]

    # Calculate total alerts per week
    totals = [sum(data[week].values()) for week in weeks]

    # Customize the chart
    ax.set_xlabel('Week Starting', fontsize=12, fontweight='bold')
    ax.set_ylabel('Alert Count', fontsize=12, fontweight='bold')
    ax.set_title(f'OpsGenie Alerts - Last {n_weeks} Weeks by Team', fontsize=14, fontweight='bold', pad=20)
    ax.set_xticks(x_pos)
    ax.set_xticklabels(week_labels, rotation=0)

    # Add total count labels on top of each bar
    for i, (x, total) in enumerate(zip(x_pos, totals)):
        ax.text(x, total + 0.5, str(total), ha='center', va='bottom', fontweight='bold')

    # Add legend
    ax.legend(title='Team', bbox_to_anchor=(1.05, 1), loc='upper left')

    # Add grid for easier reading
    ax.grid(axis='y', alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)

    # Tight layout to prevent label cutoff
    plt.tight_layout()

    # Save the figure
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    logger.info(f"Chart saved to {output_path}")

    # Log summary statistics
    logger.info(f"Total alerts across all weeks: {sum(totals)}")
    for team in all_teams:
        total_team_alerts = sum(team_data[team])
        logger.info(f"  {team}: {total_team_alerts} alerts")


def main():
    """Main entry point for the script."""
    # Generate default output filename with today's date
    today = datetime.now().strftime('%Y_%m_%d')
    default_output = f'~/Desktop/debris/{today}_opsgenie_alerts_6_weeks.png'

    parser = argparse.ArgumentParser(
        description='Generate a stacked bar chart of OpsGenie alerts by week and team'
    )
    parser.add_argument(
        '--csv',
        type=str,
        default='~/Desktop/debris/2026_01_09_opsgenie.csv',
        help='Path to the OpsGenie CSV file (default: ~/Desktop/debris/2026_01_09_opsgenie.csv)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default=default_output,
        help=f'Output path for the PNG chart (default: {default_output})'
    )
    parser.add_argument(
        '--weeks',
        type=int,
        default=6,
        help='Number of weeks to analyze (default: 6)'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )

    args = parser.parse_args()

    # Configure logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Expand paths
    csv_path = Path(args.csv).expanduser()
    output_path = Path(args.output).expanduser()

    logger.info(f"Starting OpsGenie alerts analysis")
    logger.info(f"Input CSV: {csv_path}")
    logger.info(f"Output PNG: {output_path}")

    # Process the data
    alerts = parse_csv(str(csv_path))
    filtered_alerts = filter_last_n_weeks(alerts, args.weeks)
    aggregated_data = aggregate_by_week_and_team(filtered_alerts)

    # Generate the chart
    create_stacked_bar_chart(aggregated_data, str(output_path), args.weeks)

    logger.info("Analysis complete!")


if __name__ == '__main__':
    main()
