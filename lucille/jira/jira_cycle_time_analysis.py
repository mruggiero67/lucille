#!/usr/bin/env python3
"""
Jira Cycle Time Analysis

Analyzes cycle time for Jira issues within a project and date range.
Generates detailed reports and visualizations.
"""

import argparse
import logging
from datetime import datetime
from typing import List, Dict, Tuple, Optional
from collections import defaultdict
from pathlib import Path

import yaml
import pandas as pd
import matplotlib.pyplot as plt
import requests
from dateutil import parser as date_parser
import sys

try:
    from .utils import create_jira_session, fetch_all_issues
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from lucille.jira.utils import create_jira_session, fetch_all_issues

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Jira workflow states
STATES = [
    "Ready for Development",
    "In Progress",
    "Review",
    "Ready for Testing",
    "In Testing",
    "To Deploy",
    "Done",
]


# ============================================================================
# Pure Functions (No Side Effects)
# ============================================================================

def calculate_time_in_state(
    transitions: List[Dict],
    state: str,
    next_states: List[str]
) -> float:
    """
    Calculate time spent in a specific state in days.

    Args:
        transitions: List of transition dictionaries with 'to_state' and 'timestamp'
        state: The state to calculate time for
        next_states: List of states that follow the given state

    Returns:
        Time spent in state in days (float)
    """
    time_in_state = 0.0
    entry_time = None

    for transition in transitions:
        if transition['to_state'] == state:
            entry_time = transition['timestamp']
        elif entry_time and transition['to_state'] in next_states:
            exit_time = transition['timestamp']
            time_in_state += (exit_time - entry_time).total_seconds() / 86400  # Convert to days
            entry_time = None

    return time_in_state


def calculate_cycle_time(transitions: List[Dict], states: List[str]) -> Dict[str, float]:
    """
    Calculate time spent in each state for an issue.

    Args:
        transitions: List of transition dictionaries
        states: List of workflow states in order

    Returns:
        Dictionary mapping state names to time spent (in days)
    """
    cycle_time = {}

    for i, state in enumerate(states[:-1]):  # Exclude 'Done' as it has no exit
        next_states = states[i+1:]
        cycle_time[state] = calculate_time_in_state(transitions, state, next_states)

    # Calculate total time for 'Done' state if needed
    cycle_time[states[-1]] = 0.0  # 'Done' is terminal

    return cycle_time


def calculate_total_cycle_time(cycle_time: Dict[str, float]) -> float:
    """
    Calculate total cycle time across all states.

    Args:
        cycle_time: Dictionary of state -> time in days

    Returns:
        Total cycle time in days
    """
    return sum(cycle_time.values())


def calculate_deployment_wait_time(cycle_time: Dict[str, float]) -> float:
    """
    Calculate time spent waiting for deployment (To Deploy state).

    Args:
        cycle_time: Dictionary of state -> time in days

    Returns:
        Deployment wait time in days
    """
    return cycle_time.get("To Deploy", 0.0)


def calculate_summary_statistics(cycle_times: List[Dict[str, float]]) -> Dict[str, float]:
    """
    Calculate summary statistics for cycle times.

    Args:
        cycle_times: List of cycle time dictionaries for multiple issues

    Returns:
        Dictionary with summary statistics
    """
    if not cycle_times:
        return {
            'average_cycle_time': 0.0,
            'std_dev': 0.0,
            'median_cycle_time': 0.0,
            'min_cycle_time': 0.0,
            'max_cycle_time': 0.0,
            'average_deployment_wait': 0.0,
        }

    total_times = [calculate_total_cycle_time(ct) for ct in cycle_times]
    deployment_waits = [calculate_deployment_wait_time(ct) for ct in cycle_times]

    df = pd.DataFrame(total_times)

    return {
        'average_cycle_time': float(df.mean().iloc[0]),
        'std_dev': float(df.std().iloc[0]),
        'median_cycle_time': float(df.median().iloc[0]),
        'min_cycle_time': float(df.min().iloc[0]),
        'max_cycle_time': float(df.max().iloc[0]),
        'average_deployment_wait': sum(deployment_waits) / len(deployment_waits),
    }


def identify_bottlenecks(cycle_times: List[Dict[str, float]], states: List[str]) -> Dict[str, float]:
    """
    Identify bottleneck stages by calculating average time in each state.

    Args:
        cycle_times: List of cycle time dictionaries
        states: List of workflow states

    Returns:
        Dictionary of state -> average time in days, sorted by time (descending)
    """
    state_totals = defaultdict(float)
    state_counts = defaultdict(int)

    for cycle_time in cycle_times:
        for state in states:
            if state in cycle_time and cycle_time[state] > 0:
                state_totals[state] += cycle_time[state]
                state_counts[state] += 1

    averages = {
        state: state_totals[state] / state_counts[state] if state_counts[state] > 0 else 0.0
        for state in states
    }

    # Sort by time descending
    return dict(sorted(averages.items(), key=lambda x: x[1], reverse=True))


def categorize_cycle_time(total_time: float) -> str:
    """
    Categorize cycle time into buckets.

    Args:
        total_time: Total cycle time in days

    Returns:
        Category string (e.g., '0-2 days')
    """
    if total_time <= 2:
        return '0-2 days'
    elif total_time <= 5:
        return '3-5 days'
    elif total_time <= 10:
        return '6-10 days'
    elif total_time <= 20:
        return '11-20 days'
    else:
        return '20+ days'


def calculate_distribution(cycle_times: List[Dict[str, float]]) -> Dict[str, int]:
    """
    Calculate cycle time distribution across buckets.

    Args:
        cycle_times: List of cycle time dictionaries

    Returns:
        Dictionary mapping category to count
    """
    distribution = defaultdict(int)
    categories_order = ['0-2 days', '3-5 days', '6-10 days', '11-20 days', '20+ days']

    for cycle_time in cycle_times:
        total = calculate_total_cycle_time(cycle_time)
        category = categorize_cycle_time(total)
        distribution[category] += 1

    # Ensure all categories exist
    return {cat: distribution[cat] for cat in categories_order}


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
        logger.warning(f"Config file {config_path} not found, using defaults")
        return {}


def fetch_issues(
    session: requests.Session,
    base_url: str,
    project_key: str,
    start_date: str,
    end_date: str
) -> List[Dict]:
    """
    Fetch issues from Jira for the given project and date range.

    Args:
        session: Authenticated requests session
        base_url: Jira base URL
        project_key: Jira project key
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        List of Jira issue dictionaries
    """
    jql = (
        f'project = {project_key} AND '
        f'status = Done AND '
        f'resolutiondate >= "{start_date}" AND '
        f'resolutiondate <= "{end_date}"'
    )

    logger.info(f"Fetching issues with JQL: {jql}")

    # Use the utility function to fetch all issues with pagination
    issues = fetch_all_issues(
        session=session,
        base_url=base_url,
        jql=jql,
        fields=['key', 'summary', 'status', 'resolutiondate'],
        expand='changelog'
    )

    logger.info(f"Fetched {len(issues)} issues")
    return issues


def extract_transitions(issue: Dict) -> List[Dict]:
    """
    Extract state transitions from issue changelog.

    Args:
        issue: Jira issue dictionary from API response

    Returns:
        List of transition dictionaries
    """
    transitions = []

    changelog = issue.get('changelog', {})
    histories = changelog.get('histories', [])

    for history in histories:
        for item in history.get('items', []):
            if item.get('field') == 'status':
                transitions.append({
                    'to_state': item.get('toString', ''),
                    'from_state': item.get('fromString', ''),
                    'timestamp': date_parser.parse(history.get('created'))
                })

    # Sort by timestamp
    transitions.sort(key=lambda x: x['timestamp'])
    return transitions


def process_issues(issues: List[Dict], states: List[str]) -> Tuple[pd.DataFrame, List[Dict[str, float]]]:
    """
    Process Jira issues and calculate cycle times.

    Args:
        issues: List of Jira issue dictionaries
        states: List of workflow states

    Returns:
        Tuple of (detailed DataFrame, list of cycle time dictionaries)
    """
    logger.info(f"Processing {len(issues)} issues")

    detailed_data = []
    cycle_times = []

    for issue in issues:
        try:
            issue_key = issue.get('key', 'UNKNOWN')
            fields = issue.get('fields', {})
            summary = fields.get('summary', '')

            transitions = extract_transitions(issue)
            cycle_time = calculate_cycle_time(transitions, states)
            total_time = calculate_total_cycle_time(cycle_time)

            row = {
                'Issue Key': issue_key,
                'Summary': summary,
                'Total Cycle Time (days)': round(total_time, 2),
            }

            # Add individual state times
            for state in states:
                row[f'{state} (days)'] = round(cycle_time.get(state, 0.0), 2)

            row['Deployment Wait (days)'] = round(calculate_deployment_wait_time(cycle_time), 2)

            detailed_data.append(row)
            cycle_times.append(cycle_time)

        except Exception as e:
            issue_key = issue.get('key', 'UNKNOWN')
            logger.error(f"Error processing issue {issue_key}: {e}")

    df = pd.DataFrame(detailed_data)
    logger.info(f"Successfully processed {len(detailed_data)} issues")

    return df, cycle_times


def save_detailed_spreadsheet(df: pd.DataFrame, output_path: str):
    """
    Save detailed cycle time data to Excel spreadsheet.

    Args:
        df: DataFrame with detailed cycle time information
        output_path: Path to save Excel file
    """
    logger.info(f"Saving detailed spreadsheet to {output_path}")
    df.to_excel(output_path, index=False, engine='openpyxl')
    logger.info(f"Spreadsheet saved successfully")


def save_summary_csv(
    summary_stats: Dict[str, float],
    bottlenecks: Dict[str, float],
    output_path: str
):
    """
    Save summary statistics to CSV file.

    Args:
        summary_stats: Dictionary of summary statistics
        bottlenecks: Dictionary of bottleneck information
        output_path: Path to save CSV file
    """
    logger.info(f"Saving summary statistics to {output_path}")

    # Prepare summary data
    data = {
        'Metric': [],
        'Value': []
    }

    # Add summary statistics
    for key, value in summary_stats.items():
        data['Metric'].append(key.replace('_', ' ').title())
        data['Value'].append(round(value, 2))

    # Add bottlenecks
    data['Metric'].append('')  # Empty row separator
    data['Value'].append('')
    data['Metric'].append('Bottleneck Analysis')
    data['Value'].append('Average Days')

    for state, avg_time in bottlenecks.items():
        data['Metric'].append(state)
        data['Value'].append(round(avg_time, 2))

    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)
    logger.info(f"Summary CSV saved successfully")


def create_distribution_chart(distribution: Dict[str, int], output_path: str):
    """
    Create bar chart showing cycle time distribution.

    Args:
        distribution: Dictionary of category -> count
        output_path: Path to save chart image
    """
    logger.info(f"Creating distribution chart at {output_path}")

    fig, ax = plt.subplots(figsize=(10, 6))

    categories = list(distribution.keys())
    counts = list(distribution.values())

    ax.bar(categories, counts, color='#2196F3')
    ax.set_xlabel('Cycle Time Range', fontsize=12)
    ax.set_ylabel('Number of Issues', fontsize=12)
    ax.set_title('Cycle Time Distribution', fontsize=14, fontweight='bold')
    ax.grid(axis='y', alpha=0.3)

    # Add count labels on bars
    for i, count in enumerate(counts):
        ax.text(i, count + 0.5, str(count), ha='center', va='bottom')

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

    logger.info(f"Distribution chart saved successfully")


def create_breakdown_chart(bottlenecks: Dict[str, float], output_path: str):
    """
    Create bar chart showing cycle time breakdown by stage.

    Args:
        bottlenecks: Dictionary of state -> average time
        output_path: Path to save chart image
    """
    logger.info(f"Creating breakdown chart at {output_path}")

    fig, ax = plt.subplots(figsize=(12, 6))

    states = list(bottlenecks.keys())
    times = list(bottlenecks.values())

    colors = ['#f44336' if i == 0 else '#FF9800' if i == 1 else '#4CAF50'
              for i in range(len(states))]

    ax.barh(states, times, color=colors)
    ax.set_xlabel('Average Time (days)', fontsize=12)
    ax.set_ylabel('Workflow Stage', fontsize=12)
    ax.set_title('Cycle Time Breakdown by Stage', fontsize=14, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)

    # Add time labels on bars
    for i, time in enumerate(times):
        ax.text(time + 0.1, i, f'{time:.1f}d', va='center')

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

    logger.info(f"Breakdown chart saved successfully")


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
        description='Analyze Jira cycle time for a project',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        'project_key',
        help='Jira project key (e.g., PROJ)'
    )

    parser.add_argument(
        'start_date',
        help='Start date in YYYY-MM-DD format'
    )

    parser.add_argument(
        'end_date',
        help='End date in YYYY-MM-DD format'
    )

    parser.add_argument(
        '-c', '--config',
        default='jira_config.yaml',
        help='Path to configuration YAML file (default: jira_config.yaml)'
    )

    parser.add_argument(
        '-o', '--output-dir',
        default='output',
        help='Output directory for generated files (default: output)'
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

    logger.info("Starting Jira Cycle Time Analysis")

    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)
    logger.info(f"Output directory: {output_dir}")

    # Load configuration
    config = load_config(args.config)
    jira_config = config.get('jira', {})

    # Create Jira session
    session = create_jira_session(
        base_url=jira_config['base_url'],
        username=jira_config['username'],
        api_token=jira_config['api_token']
    )

    # Fetch issues
    issues = fetch_issues(
        session=session,
        base_url=jira_config['base_url'],
        project_key=args.project_key,
        start_date=args.start_date,
        end_date=args.end_date
    )

    if not issues:
        logger.warning("No issues found for the given criteria")
        return

    # Process issues
    detailed_df, cycle_times = process_issues(issues, STATES)

    # Calculate summary statistics
    summary_stats = calculate_summary_statistics(cycle_times)
    bottlenecks = identify_bottlenecks(cycle_times, STATES)
    distribution = calculate_distribution(cycle_times)

    # Generate artifacts
    detailed_path = output_dir / f"{args.project_key}_cycle_time_detailed.xlsx"
    summary_path = output_dir / f"{args.project_key}_cycle_time_summary.csv"
    distribution_chart_path = output_dir / f"{args.project_key}_cycle_time_distribution.png"
    breakdown_chart_path = output_dir / f"{args.project_key}_cycle_time_breakdown.png"

    save_detailed_spreadsheet(detailed_df, str(detailed_path))
    save_summary_csv(summary_stats, bottlenecks, str(summary_path))
    create_distribution_chart(distribution, str(distribution_chart_path))
    create_breakdown_chart(bottlenecks, str(breakdown_chart_path))

    logger.info("Analysis complete! Generated files:")
    logger.info(f"  1. Detailed spreadsheet: {detailed_path}")
    logger.info(f"  2. Summary statistics: {summary_path}")
    logger.info(f"  3. Distribution chart: {distribution_chart_path}")
    logger.info(f"  4. Breakdown chart: {breakdown_chart_path}")


if __name__ == '__main__':
    main()
