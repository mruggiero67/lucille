#!/usr/bin/env python3
"""
Weekly Deployment Trends Analyzer

Analyzes deployment data from CSV and generates:
1. Weekly deployment counts over time
2. Trend line showing overall deployment velocity
3. Summary statistics and insights
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import yaml

# Configure logging at module level
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path: Path) -> dict:
    """
    Load configuration from YAML file.

    Pure function with no side effects.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Dictionary containing configuration parameters
    """
    if config_path and config_path.exists():
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    return {}


def calculate_weekly_deployments(df: pd.DataFrame, date_column: str = 'date') -> pd.DataFrame:
    """
    Calculate deployments per week from daily deployment data.

    Pure function with no side effects.

    Args:
        df: DataFrame with deployment data
        date_column: Name of the column containing dates

    Returns:
        DataFrame with week_start_date and deployment_count columns
    """
    # Convert date column to datetime
    df = df.copy()
    df['date_parsed'] = pd.to_datetime(df[date_column])

    # Extract week start date (Monday of each week)
    df['week_start'] = df['date_parsed'] - pd.to_timedelta(df['date_parsed'].dt.dayofweek, unit='d')

    # Group by week and count deployments
    weekly_counts = df.groupby('week_start').size().reset_index(name='deployment_count')
    weekly_counts = weekly_counts.sort_values('week_start')

    return weekly_counts


def calculate_trend_line(weekly_data: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, float]:
    """
    Calculate linear trend line for weekly deployment data.

    Pure function with no side effects.

    Args:
        weekly_data: DataFrame with week_start and deployment_count columns

    Returns:
        Tuple of (x_values, y_values, slope) for the trend line
    """
    # Convert dates to numeric values (days since first week)
    x = (weekly_data['week_start'] - weekly_data['week_start'].min()).dt.days.values
    y = weekly_data['deployment_count'].values

    # Calculate linear regression
    coefficients = np.polyfit(x, y, 1)
    slope = coefficients[0]
    trend_line = np.poly1d(coefficients)

    return x, trend_line(x), slope


def calculate_statistics(weekly_data: pd.DataFrame) -> dict:
    """
    Calculate summary statistics for weekly deployment data.

    Pure function with no side effects.

    Args:
        weekly_data: DataFrame with week_start and deployment_count columns

    Returns:
        Dictionary containing various statistics
    """
    stats = {
        'total_weeks': len(weekly_data),
        'total_deployments': weekly_data['deployment_count'].sum(),
        'average_per_week': weekly_data['deployment_count'].mean(),
        'median_per_week': weekly_data['deployment_count'].median(),
        'max_week': weekly_data['deployment_count'].max(),
        'min_week': weekly_data['deployment_count'].min(),
        'std_dev': weekly_data['deployment_count'].std(),
        'first_week': weekly_data['week_start'].min(),
        'last_week': weekly_data['week_start'].max(),
    }

    return stats


def create_weekly_trend_graph(
    weekly_data: pd.DataFrame,
    output_path: Path,
    title: str = "Weekly Deployment Trends",
    figsize: Tuple[int, int] = (14, 8)
) -> None:
    """
    Create and save a graph showing weekly deployment trends.

    Side-effecting function that writes to file system.

    Args:
        weekly_data: DataFrame with week_start and deployment_count columns
        output_path: Path where the PNG file should be saved
        title: Title for the graph
        figsize: Figure size as (width, height)
    """
    logger.info(f"Creating weekly trend graph: {output_path}")

    # Create figure and axis
    fig, ax = plt.subplots(figsize=figsize)

    # Plot weekly deployment bars
    weeks = weekly_data['week_start']
    counts = weekly_data['deployment_count']

    bars = ax.bar(weeks, counts, width=6, color='steelblue', alpha=0.7, label='Weekly Deployments')

    # Calculate and plot trend line
    x_numeric, trend_y, slope = calculate_trend_line(weekly_data)

    # Convert x_numeric back to dates for plotting
    trend_dates = weekly_data['week_start'].min() + pd.to_timedelta(x_numeric, unit='d')
    ax.plot(trend_dates, trend_y, 'r--', linewidth=2, label=f'Trend (slope: {slope:.2f} deployments/week)')

    # Customize the plot
    ax.set_xlabel('Week Starting', fontsize=12, fontweight='bold')
    ax.set_ylabel('Number of Deployments', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold', pad=20)

    # Format x-axis
    ax.tick_params(axis='x', rotation=45)

    # Add grid for readability
    ax.grid(axis='y', alpha=0.3, linestyle='--')

    # Add legend
    ax.legend(loc='upper left', fontsize=10)

    # Add average line
    avg = counts.mean()
    ax.axhline(y=avg, color='green', linestyle=':', linewidth=2, alpha=0.5, label=f'Average: {avg:.1f}')

    # Adjust layout to prevent label cutoff
    plt.tight_layout()

    # Save the figure
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    logger.info(f"Graph saved successfully to {output_path}")

    # Close the figure to free memory
    plt.close(fig)


def create_summary_report(
    weekly_data: pd.DataFrame,
    statistics: dict,
    output_path: Path
) -> None:
    """
    Create a text summary report of weekly deployment trends.

    Side-effecting function that writes to file system.

    Args:
        weekly_data: DataFrame with week_start and deployment_count columns
        statistics: Dictionary of calculated statistics
        output_path: Path where the summary text file should be saved
    """
    logger.info(f"Creating summary report: {output_path}")
    with open(output_path, 'w') as f:
        f.write(f"Date Range: week starting {statistics['first_week'].strftime('%Y-%m-%d')} to week starting "
                f"{statistics['last_week'].strftime('%Y-%m-%d')}\n")
        f.write(f"Average Deployments per Week: {statistics['average_per_week']:.2f}\n")
    logger.info(f"Summary report saved to {output_path}")


def main():
    """
    Main entry point for the weekly deployment trends analyzer.
    """
    parser = argparse.ArgumentParser(
        description='Analyze weekly deployment trends from CSV data'
    )
    parser.add_argument(
        '--csv',
        type=Path,
        required=True,
        help='Path to the deployment analysis CSV file'
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=Path.home() / 'Desktop' / 'debris',
        help='Directory where output files will be saved (default: ~/Desktop/debris)'
    )
    parser.add_argument(
        '--config',
        type=Path,
        help='Path to YAML configuration file (optional)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging output'
    )

    args = parser.parse_args()

    # Adjust logging level if verbose
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")

    # Load configuration if provided
    config = load_config(args.config) if args.config else {}
    logger.debug(f"Loaded configuration: {config}")

    # Validate input file exists
    if not args.csv.exists():
        logger.error(f"CSV file not found: {args.csv}")
        return 1

    # Create output directory if it doesn't exist
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Load the data
    logger.info(f"Loading deployment data from {args.csv}")
    df = pd.read_csv(args.csv)
    logger.info(f"Loaded {len(df)} deployment records")

    # Calculate weekly deployments
    weekly_data = calculate_weekly_deployments(df, date_column='date')
    logger.info(f"Calculated weekly data for {len(weekly_data)} weeks")

    # Calculate statistics
    statistics = calculate_statistics(weekly_data)

    # Generate outputs
    timestamp = datetime.now().strftime("%Y_%m_%d")
    graph_path = args.output_dir / f'{timestamp}_weekly_deployment_trends.png'
    summary_path = args.output_dir / f'{timestamp}_weekly_deployment_summary.txt'

    create_weekly_trend_graph(weekly_data, graph_path)
    create_summary_report(weekly_data, statistics, summary_path)

    # Log summary
    logger.info("=" * 50)
    logger.info("ANALYSIS COMPLETE")
    logger.info("=" * 50)
    logger.info(f"Total Weeks Analyzed: {statistics['total_weeks']}")
    logger.info(f"Average Deployments/Week: {statistics['average_per_week']:.2f}")
    logger.info(f"Date Range: {statistics['first_week'].strftime('%Y-%m-%d')} to "
                f"{statistics['last_week'].strftime('%Y-%m-%d')}")
    logger.info(f"\nGraph saved to: {graph_path}")
    logger.info(f"Summary saved to: {summary_path}")

    return 0


if __name__ == '__main__':
    exit(main())
