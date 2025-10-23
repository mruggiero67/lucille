#!/usr/bin/env python3
"""
Claude Code Token Usage Analyzer

Analyzes token usage data from Claude Code CSV export and generates:
1. A bar graph showing total tokens used per day (saved as PNG)
2. A summary text file with the daily token usage values
"""

import argparse
import logging
from pathlib import Path
from typing import Dict

import matplotlib.pyplot as plt
import pandas as pd
import yaml


# Configure logging at module level
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path: Path) -> Dict:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        Dictionary containing configuration parameters
    """
    if config_path.exists():
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    return {}


def calculate_total_tokens(row: pd.Series) -> int:
    """
    Calculate total tokens for a single row.

    This is a pure function with no side effects.

    Args:
        row: A pandas Series representing one row of token usage data

    Returns:
        Total token count (input + output tokens)
    """
    input_tokens = (
        row.get('usage_input_tokens_no_cache', 0) +
        row.get('usage_input_tokens_cache_write_5m', 0) +
        row.get('usage_input_tokens_cache_write_1h', 0) +
        row.get('usage_input_tokens_cache_read', 0)
    )
    output_tokens = row.get('usage_output_tokens', 0)
    return input_tokens + output_tokens


def load_and_process_data(csv_path: Path) -> pd.DataFrame:
    """
    Load CSV data and calculate total tokens per day.

    Side-effecting function that reads from file system.

    Args:
        csv_path: Path to the CSV file containing token usage data

    Returns:
        DataFrame with usage_date_utc and total_tokens columns
    """
    logger.info(f"Loading data from {csv_path}")

    # Load the CSV file
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} rows of data")

    # Calculate total tokens for each row
    df['total_tokens'] = df.apply(calculate_total_tokens, axis=1)

    # Group by date and sum tokens
    daily_totals = df.groupby('usage_date_utc')['total_tokens'].sum().reset_index()
    daily_totals = daily_totals.sort_values('usage_date_utc')

    logger.info(f"Processed data for {len(daily_totals)} unique days")
    return daily_totals


def create_graph(daily_totals: pd.DataFrame, output_path: Path) -> None:
    """
    Create and save a bar graph of daily token usage.

    Side-effecting function that writes to file system.

    Args:
        daily_totals: DataFrame with usage_date_utc and total_tokens columns
        output_path: Path where the PNG file should be saved
    """
    logger.info(f"Creating graph and saving to {output_path}")

    # Create figure and axis
    fig, ax = plt.subplots(figsize=(14, 8))

    # Create bar chart
    dates = daily_totals['usage_date_utc']
    tokens = daily_totals['total_tokens']

    bars = ax.bar(range(len(dates)), tokens, color='steelblue', alpha=0.8)

    # Customize the plot
    ax.set_xlabel('Date', fontsize=12, fontweight='bold')
    ax.set_ylabel('Total Tokens', fontsize=12, fontweight='bold')
    ax.set_title('Claude Code Token Usage by Day', fontsize=14, fontweight='bold', pad=20)

    # Set x-axis labels
    ax.set_xticks(range(len(dates)))
    ax.set_xticklabels(dates, rotation=45, ha='right')

    # Add grid for readability
    ax.grid(axis='y', alpha=0.3, linestyle='--')

    # Format y-axis to show numbers in millions
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1e6:.1f}M'))

    # Add value labels on top of bars
    for i, (bar, value) in enumerate(zip(bars, tokens)):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{value/1e6:.2f}M',
                ha='center', va='bottom', fontsize=8)

    # Adjust layout to prevent label cutoff
    plt.tight_layout()

    # Save as PNG
    plt.savefig(output_path, format='png', dpi=150)
    logger.info(f"Graph saved successfully to {output_path}")

    # Close the figure to free memory
    plt.close(fig)


def create_summary_file(daily_totals: pd.DataFrame, output_path: Path) -> None:
    """
    Create a text summary file with daily token usage values.

    Side-effecting function that writes to file system.

    Args:
        daily_totals: DataFrame with usage_date_utc and total_tokens columns
        output_path: Path where the summary text file should be saved
    """
    logger.info(f"Creating summary file at {output_path}")

    with open(output_path, 'w') as f:
        f.write("Claude Code Token Usage Summary\n")
        f.write("=" * 60 + "\n\n")

        # Write daily breakdown
        f.write("Daily Token Usage:\n")
        f.write("-" * 60 + "\n")
        for _, row in daily_totals.iterrows():
            date = row['usage_date_utc']
            tokens = row['total_tokens']
            f.write(f"{date}: {tokens:,} tokens ({tokens/1e6:.2f}M)\n")

        # Write summary statistics
        f.write("\n" + "=" * 60 + "\n")
        f.write("Summary Statistics:\n")
        f.write("-" * 60 + "\n")
        total = daily_totals['total_tokens'].sum()
        mean = daily_totals['total_tokens'].mean()
        median = daily_totals['total_tokens'].median()
        max_val = daily_totals['total_tokens'].max()
        min_val = daily_totals['total_tokens'].min()

        f.write(f"Total tokens (all days): {total:,} ({total/1e6:.2f}M)\n")
        f.write(f"Average tokens per day: {mean:,.0f} ({mean/1e6:.2f}M)\n")
        f.write(f"Median tokens per day: {median:,.0f} ({median/1e6:.2f}M)\n")
        f.write(f"Maximum tokens (single day): {max_val:,} ({max_val/1e6:.2f}M)\n")
        f.write(f"Minimum tokens (single day): {min_val:,} ({min_val/1e6:.2f}M)\n")
        f.write(f"Number of days: {len(daily_totals)}\n")

    logger.info(f"Summary file created successfully at {output_path}")


def main():
    """
    Main entry point for the token usage analyzer.
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description='Analyze Claude Code token usage and generate visualizations'
    )
    parser.add_argument(
        '--csv',
        type=Path,
        default=Path.home() / 'Desktop' / 'debris' / '2025_10_23_claude_token_use.csv',
        help='Path to the CSV file containing token usage data (default: ~/Desktop/debris/2025_10_23_claude_token_use.csv)'
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
    config = {}
    if args.config:
        config = load_config(args.config)
        logger.debug(f"Loaded configuration: {config}")

    # Validate input file exists
    if not args.csv.exists():
        logger.error(f"CSV file not found: {args.csv}")
        return 1

    # Create output directory if it doesn't exist
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Process the data
    daily_totals = load_and_process_data(args.csv)

    # Generate outputs
    graph_path = args.output_dir / 'claude_token_usage_by_day.png'
    summary_path = args.output_dir / 'claude_token_usage_summary.txt'

    create_graph(daily_totals, graph_path)
    create_summary_file(daily_totals, summary_path)

    logger.info("Analysis complete!")
    logger.info(f"Graph saved to: {graph_path}")
    logger.info(f"Summary saved to: {summary_path}")

    return 0


if __name__ == '__main__':
    exit(main())
