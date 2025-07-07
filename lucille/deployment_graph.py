import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime
from typing import Callable, Optional
import yaml
import os
import argparse
import logging

logging.basicConfig(
    format="%(levelname)-10s %(asctime)s %(filename)s %(lineno)d %(message)s",
    level=logging.INFO,
)

def analyze_deployments_per_day(
    csv_file: str,
    date_parser: Callable[[str], datetime],
    date_column: str = "date",
    title: str = "Deployments per Day",
    figsize: tuple = (12, 6),
    config_file: str = "config.yaml",
) -> dict:
    """
    Analyze and visualize deployments per day from CSV data.

    Args:
        csv_file: Path to the CSV file
        date_parser: Function that takes a date string and returns a datetime object
        date_column: Name of the column containing dates
        title: Title for the chart
        figsize: Figure size as (width, height)
        config_file: Path to YAML config file containing output directory

    Returns:
        Dictionary with analysis results including average deployments per week
    """
    # Load configuration
    try:
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
        output_dir = config.get("output_directory", "./output")
    except FileNotFoundError:
        logging.error(
            f"Warning: Config file {config_file} not found. Using default output directory './output'"
        )
        output_dir = "./output"
    except Exception as e:
        logging.error(
            f"Warning: Error reading config file: {e}. Using default output directory './output'"
        )
        output_dir = "./output"

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    # Read the CSV file
    df = pd.read_csv(csv_file)

    # Parse dates using the provided parser
    df["parsed_date"] = df[date_column].apply(date_parser)

    # Extract just the date part (remove time if present)
    df["date_only"] = df["parsed_date"].dt.date

    # Count deployments per day
    deployments_per_day = (
        df.groupby("date_only").size().reset_index(name="deployment_count")
    )

    # Convert back to datetime for plotting
    deployments_per_day["date_only"] = pd.to_datetime(deployments_per_day["date_only"])

    # Sort by date
    deployments_per_day = deployments_per_day.sort_values("date_only")

    # Create the bar chart
    plt.figure(figsize=figsize)
    sns.barplot(data=deployments_per_day, x="date_only", y="deployment_count")
    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel("Number of Deployments")
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Save the chart as PNG
    # Generate filename based on title and current timestamp
    safe_title = "".join(
        c for c in title if c.isalnum() or c in (" ", "-", "_")
    ).rstrip()
    safe_title = safe_title.replace(" ", "_").lower()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{safe_title}_{timestamp}.png"
    filepath = os.path.join(output_dir, filename)

    plt.savefig(filepath, dpi=300, bbox_inches="tight")
    logging.info(f"Chart saved to: {filepath}")

    # Calculate statistics
    total_deployments = deployments_per_day["deployment_count"].sum()
    total_days = len(deployments_per_day)
    avg_deployments_per_day = total_deployments / total_days

    # Calculate average deployments per week
    # Assuming 7 days per week
    avg_deployments_per_week = avg_deployments_per_day * 7

    # Calculate date range for more accurate weekly average
    date_range = (
        deployments_per_day["date_only"].max() - deployments_per_day["date_only"].min()
    )
    total_weeks = date_range.days / 7
    avg_deployments_per_week_actual = (
        total_deployments / total_weeks if total_weeks > 0 else 0
    )

    # Print summary statistics
    logging.info(f"=== Deployment Analysis Summary ===")
    logging.info(f"Total deployments: {total_deployments}")
    logging.info(
        f"Date range: {deployments_per_day['date_only'].min().strftime('%Y-%m-%d')} to {deployments_per_day['date_only'].max().strftime('%Y-%m-%d')}"
    )
    logging.info(f"Total days with deployments: {total_days}")
    logging.info(f"Average deployments per day: {avg_deployments_per_day:.2f}")
    logging.info(f"Average deployments per week (simple): {avg_deployments_per_week:.2f}")
    logging.info(
        f"Average deployments per week (actual): {avg_deployments_per_week_actual:.2f}"
    )

    # plt.show()

    return {
        "total_deployments": total_deployments,
        "total_days": total_days,
        "avg_per_day": avg_deployments_per_day,
        "avg_per_week_simple": avg_deployments_per_week,
        "avg_per_week_actual": avg_deployments_per_week_actual,
        "deployments_per_day": deployments_per_day,
        "date_range_days": date_range.days,
        "chart_saved_to": filepath,
    }


# Example usage with different date parsers


def parse_iso_date(date_str: str) -> datetime:
    """Parser for ISO format dates like '2025-07-07'"""
    return datetime.strptime(date_str, "%Y-%m-%d")


def parse_us_date(date_str: str) -> datetime:
    """Parser for US format dates like '07/07/2025'"""
    return datetime.strptime(date_str, "%m/%d/%Y")


def parse_eu_date(date_str: str) -> datetime:
    """Parser for European format dates like '07/07/2025'"""
    return datetime.strptime(date_str, "%d/%m/%Y")


def parse_verbose_date(date_str: str) -> datetime:
    """Parser for verbose dates like 'July 7, 2025'"""
    return datetime.strptime(date_str, "%B %d, %Y")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create graph from deployments CSV")

    parser.add_argument("-c", "--config", type=str, help="path to config file")
    parser.add_argument("-f", "--csv_filepath", type=str, help="path to deployments CSV")

    args = parser.parse_args()
    config_file = args.config
    csv = args.csv_filepath

    try:
        results = analyze_deployments_per_day(
            csv_file=csv,
            date_parser=parse_iso_date,
            date_column="date",
            title="Daily Deployments Analysis",
            config_file=config_file
        )

        logging.info(
            f"\nKey insight: You're averaging {results['avg_per_week_actual']:.1f} deployments per week"
        )

    except Exception as e:
        logging.error(f"Error: {e}")
        logging.error("Make sure the CSV file exists and the date format matches your parser")

    # Example 2: If you had a different CSV with US date format
    # results = analyze_deployments_per_day(
    #     csv_file='other_events.csv',
    #     date_parser=parse_us_date,
    #     date_column='event_date',
    #     title='Daily Events Analysis',
    #     config_file='config.yaml'
    # )

    # Example 3: Custom date parser for your specific format
    # def custom_parser(date_str: str) -> datetime:
    #     # Handle your specific date format here
    #     return datetime.strptime(date_str, '%Y-%m-%d')
