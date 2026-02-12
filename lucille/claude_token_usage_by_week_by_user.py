#!/usr/bin/env python3
"""
Claude Token Usage Analyzer - Weekly By User

Analyzes token usage data from Claude Code CSV exports and generates:
1. A line graph showing weekly token usage per user (saved as PNG)
2. A master CSV file with weekly token usage by user
"""

import argparse
import logging
import re
from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import pandas as pd

# Configure logging at module level
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def extract_username_from_api_key(api_key: str) -> str:
    """
    Extract username from API key string.

    This is a pure function with no side effects.

    Args:
        api_key: API key string like 'claude_code_key_dyngosz_pcvf'

    Returns:
        Username extracted from the key, or the full key if pattern doesn't match

    Examples:
        >>> extract_username_from_api_key('claude_code_key_dyngosz_pcvf')
        'dyngosz'
        >>> extract_username_from_api_key('jaris-claude-key')
        'jaris'
    """
    # Pattern: claude_code_key_<username>_<suffix>
    match = re.search(r'claude_code_key_([^_]+)_[^_]+$', api_key)
    if match:
        return match.group(1)

    # Pattern: <name>-claude-key
    match = re.search(r'^([^-]+)-claude-key$', api_key)
    if match:
        return match.group(1)

    # If no pattern matches, return the full key as fallback
    return api_key


def calculate_total_tokens(row: pd.Series) -> int:
    """
    Calculate total tokens for a single row.

    This is a pure function with no side effects.

    Args:
        row: A pandas Series representing one row of token usage data

    Returns:
        Total token count (all input and output tokens)
    """
    input_tokens = (
        row.get("usage_input_tokens_no_cache", 0)
        + row.get("usage_input_tokens_cache_write_5m", 0)
        + row.get("usage_input_tokens_cache_write_1h", 0)
        + row.get("usage_input_tokens_cache_read", 0)
    )
    output_tokens = row.get("usage_output_tokens", 0)
    return input_tokens + output_tokens


def load_and_process_csv_files(csv_dir: Path) -> pd.DataFrame:
    """
    Load all CSV files from directory and process token usage data.

    Side-effecting function that reads from file system.

    Args:
        csv_dir: Path to directory containing CSV files

    Returns:
        DataFrame with columns: username, week_start, total_tokens
    """
    logger.info(f"Loading CSV files from {csv_dir}")

    # Find all CSV files in the directory
    csv_files = sorted(csv_dir.glob("*.csv"))

    if not csv_files:
        raise ValueError(f"No CSV files found in {csv_dir}")

    logger.info(f"Found {len(csv_files)} CSV files")

    # Load and concatenate all CSV files
    dfs = []
    for csv_file in csv_files:
        logger.info(f"Loading {csv_file.name}")
        df = pd.read_csv(csv_file)
        dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Loaded {len(combined_df)} total rows")

    # Extract username from api_key
    combined_df["username"] = combined_df["api_key"].apply(extract_username_from_api_key)

    # Calculate total tokens for each row
    combined_df["total_tokens"] = combined_df.apply(calculate_total_tokens, axis=1)

    # Convert usage_date_utc to datetime
    combined_df["usage_date"] = pd.to_datetime(combined_df["usage_date_utc"])

    # Calculate week start date (Monday) for each usage date
    combined_df["week_start"] = combined_df["usage_date"].dt.to_period("W").apply(lambda x: x.start_time)

    # Group by username and week_start, sum tokens
    weekly_by_user = (
        combined_df.groupby(["username", "week_start"])["total_tokens"]
        .sum()
        .reset_index()
    )

    # Sort by week_start and username
    weekly_by_user = weekly_by_user.sort_values(["week_start", "username"])

    logger.info(f"Processed data for {weekly_by_user['username'].nunique()} unique users")
    logger.info(f"Processed data for {weekly_by_user['week_start'].nunique()} unique weeks")

    return weekly_by_user


def create_weekly_usage_graph(weekly_by_user: pd.DataFrame, output_path: Path) -> None:
    """
    Create and save a line graph of weekly token usage by user.

    Side-effecting function that writes to file system.

    Args:
        weekly_by_user: DataFrame with username, week_start, and total_tokens columns
        output_path: Path where the PNG file should be saved
    """
    logger.info(f"Creating graph and saving to {output_path}")

    # Create figure and axis
    fig, ax = plt.subplots(figsize=(16, 10))

    # Get unique users
    users = sorted(weekly_by_user["username"].unique())

    # Create a color map
    colors = plt.cm.tab20(range(len(users)))

    # Plot a line for each user
    for idx, user in enumerate(users):
        user_data = weekly_by_user[weekly_by_user["username"] == user].sort_values("week_start")

        ax.plot(
            user_data["week_start"],
            user_data["total_tokens"],
            marker="o",
            linewidth=2,
            markersize=6,
            label=user,
            color=colors[idx],
            alpha=0.8
        )

    # Customize the plot
    ax.set_xlabel("Week Starting", fontsize=12, fontweight="bold")
    ax.set_ylabel("Total Tokens", fontsize=12, fontweight="bold")
    ax.set_title(
        "Claude Token Usage by Week and User", fontsize=14, fontweight="bold", pad=20
    )

    # Format y-axis to show numbers in millions
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f"{x/1e6:.1f}M"))

    # Add grid for readability
    ax.grid(axis="both", alpha=0.3, linestyle="--")

    # Add legend
    ax.legend(
        loc="upper left",
        bbox_to_anchor=(1.02, 1),
        borderaxespad=0,
        fontsize=9,
        framealpha=0.9
    )

    # Rotate x-axis labels for readability
    plt.xticks(rotation=45, ha="right")

    # Adjust layout to prevent label cutoff
    plt.tight_layout()

    # Save as PNG
    plt.savefig(output_path, format="png", dpi=150, bbox_inches="tight")
    logger.info(f"Graph saved successfully to {output_path}")

    # Close the figure to free memory
    plt.close(fig)


def create_master_csv(weekly_by_user: pd.DataFrame, output_path: Path) -> None:
    """
    Create a master CSV file with weekly token usage by user.

    Side-effecting function that writes to file system.

    Args:
        weekly_by_user: DataFrame with username, week_start, and total_tokens columns
        output_path: Path where the CSV file should be saved
    """
    logger.info(f"Creating master CSV at {output_path}")

    # Create a pivot table with weeks as rows and users as columns
    pivot_df = weekly_by_user.pivot(
        index="week_start",
        columns="username",
        values="total_tokens"
    ).fillna(0)

    # Convert to integers
    pivot_df = pivot_df.astype(int)

    # Reset index to make week_start a column
    pivot_df = pivot_df.reset_index()

    # Format week_start as string
    pivot_df["week_start"] = pivot_df["week_start"].dt.strftime("%Y-%m-%d")

    # Add a total column
    user_columns = [col for col in pivot_df.columns if col != "week_start"]
    pivot_df["total"] = pivot_df[user_columns].sum(axis=1)

    # Add summary row with totals for each user
    summary_row = {"week_start": "TOTAL"}
    for col in user_columns:
        summary_row[col] = pivot_df[col].sum()
    summary_row["total"] = pivot_df["total"].sum()

    # Append summary row
    summary_df = pd.DataFrame([summary_row])
    final_df = pd.concat([pivot_df, summary_df], ignore_index=True)

    # Save to CSV
    final_df.to_csv(output_path, index=False)

    logger.info(f"Master CSV created successfully at {output_path}")


def main():
    """
    Main entry point for the weekly token usage analyzer by user.
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Analyze Claude token usage by week and user"
    )
    parser.add_argument(
        "--csv-dir",
        type=Path,
        default=Path.home() / "Desktop" / "debris" / "claude_tokens",
        help="Directory containing CSV files with token usage data (default: ~/Desktop/debris/claude_tokens)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.home() / "Desktop" / "debris",
        help="Directory where output files will be saved (default: ~/Desktop/debris)",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose logging output"
    )

    args = parser.parse_args()

    # Adjust logging level if verbose
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")

    # Validate input directory exists
    if not args.csv_dir.exists():
        logger.error(f"CSV directory not found: {args.csv_dir}")
        return 1

    # Create output directory if it doesn't exist
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Process the data
    weekly_by_user = load_and_process_csv_files(args.csv_dir)

    # Generate outputs
    graph_path = args.output_dir / "claude_token_usage_by_week_by_user.png"
    csv_path = args.output_dir / "claude_token_usage_by_week_by_user.csv"

    create_weekly_usage_graph(weekly_by_user, graph_path)
    create_master_csv(weekly_by_user, csv_path)

    logger.info("Analysis complete!")
    logger.info(f"Graph saved to: {graph_path}")
    logger.info(f"Master CSV saved to: {csv_path}")

    return 0


if __name__ == "__main__":
    exit(main())
