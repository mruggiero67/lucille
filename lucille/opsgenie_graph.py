import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime
from typing import Callable, Optional
import yaml
import os
import logging
import argparse

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-10s %(asctime)s %(filename)s %(lineno)d %(message)s",
)

def analyze_alerts_per_day(
    csv_file: str,
    date_parser: Callable[[str], datetime],
    date_column: str = 'CreatedAtDate',
    title: str = 'Alerts per Day',
    figsize: tuple = (12, 6),
    config_file: str = 'config.yaml'
) -> dict:
    """
    Analyze and visualize alerts per day from CSV data.

    Args:
        csv_file: Path to the CSV file
        date_parser: Function that takes a date string and returns a datetime object
        date_column: Name of the column containing dates
        title: Title for the chart
        figsize: Figure size as (width, height)
        config_file: Path to YAML config file containing output directory

    Returns:
        Dictionary with analysis results including average alerts per week
    """
    logging.info(f"Starting analysis of {csv_file}")

    # Load configuration
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        output_dir = config.get('output_directory', './output')
        days_back = config.get('days_back', 180)
        logging.info(f"Loaded config from {config_file}, output directory: {output_dir}")
    except FileNotFoundError:
        logging.warning(f"Config file {config_file} not found. Using default output directory './output'")
        output_dir = './output'
        days_back = 180
    except Exception as e:
        logging.warning(f"Error reading config file: {e}. Using default output directory './output'")
        output_dir = './output'
        days_back = 180

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    logging.info(f"Output directory ensured: {output_dir}")

    # Read the CSV file
    try:
        df = pd.read_csv(csv_file)
        logging.info(f"Successfully loaded CSV with {len(df)} rows and {len(df.columns)} columns")
    except Exception as e:
        logging.error(f"Failed to read CSV file: {e}")
        raise

    # Parse dates using the provided parser with error handling
    def safe_date_parser(date_str):
        try:
            dt = date_parser(date_str)
            # Convert timezone-aware datetime to UTC, then make it naive
            if dt.tzinfo is not None:
                dt = dt.utctimetuple()
                dt = datetime(*dt[:6])  # Convert back to naive datetime
            return dt
        except Exception as e:
            logging.warning(f"Failed to parse date '{date_str}': {e}")
            return None

    try:
        df['parsed_date'] = df[date_column].apply(safe_date_parser)

        # Remove rows where date parsing failed
        initial_rows = len(df)
        df = df.dropna(subset=['parsed_date'])
        rows_after_parsing = len(df)

        if initial_rows > rows_after_parsing:
            logging.warning(f"Dropped {initial_rows - rows_after_parsing} rows due to date parsing failures")

        logging.info(f"Successfully parsed dates from column '{date_column}' for {rows_after_parsing} rows")

        # Convert to pandas datetime (should now be all naive datetimes)
        df['parsed_date'] = pd.to_datetime(df['parsed_date'])

    except Exception as e:
        logging.error(f"Failed to parse dates: {e}")
        raise

    # Extract just the date part (remove time if present)
    df['date_only'] = df['parsed_date'].dt.date

    # Filter by date range if specified
    if days_back is not None:
        cutoff_date = datetime.now().date() - pd.Timedelta(days=days_back)
        initial_rows = len(df)
        df = df[df['date_only'] >= cutoff_date]
        rows_after_filter = len(df)
        logging.info(f"Filtered to last {days_back} days (from {cutoff_date})")
        if initial_rows > rows_after_filter:
            logging.info(f"Filtered out {initial_rows - rows_after_filter} rows older than {days_back} days")
            logging.info(f"Analyzing {rows_after_filter} rows from {cutoff_date} onwards")

    # Count alerts per day
    alerts_per_day = df.groupby('date_only').size().reset_index(name='alert_count')
    logging.info(f"Grouped data into {len(alerts_per_day)} unique days")

    # Convert back to datetime for plotting
    alerts_per_day['date_only'] = pd.to_datetime(alerts_per_day['date_only'])

    # Sort by date
    alerts_per_day = alerts_per_day.sort_values('date_only')

    # Create the bar chart
    plt.figure(figsize=figsize)
    sns.barplot(data=alerts_per_day, x='date_only', y='alert_count', color='tomato')
    plt.title(title)
    plt.xlabel('Date')
    plt.ylabel('Number of Alerts')
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Save the chart as PNG
    # Generate filename based on title and current timestamp
    safe_title = "".join(c for c in title if c.isalnum() or c in (' ', '-', '_')).rstrip()
    safe_title = safe_title.replace(' ', '_').lower()
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{safe_title}_{timestamp}.png"
    filepath = os.path.join(output_dir, filename)

    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    logging.info(f"Chart saved to: {filepath}")

    # Calculate statistics
    total_alerts = alerts_per_day['alert_count'].sum()
    total_days = len(alerts_per_day)
    avg_alerts_per_day = total_alerts / total_days

    # Calculate average alerts per week
    # Assuming 7 days per week
    avg_alerts_per_week = avg_alerts_per_day * 7

    # Calculate date range for more accurate weekly average
    date_range = alerts_per_day['date_only'].max() - alerts_per_day['date_only'].min()
    total_weeks = date_range.days / 7
    avg_alerts_per_week_actual = total_alerts / total_weeks if total_weeks > 0 else 0

    # Log summary statistics
    logging.info("=== Alert Analysis Summary ===")
    logging.info(f"Total alerts: {total_alerts}")
    logging.info(f"Date range: {alerts_per_day['date_only'].min().strftime('%Y-%m-%d')} to {alerts_per_day['date_only'].max().strftime('%Y-%m-%d')}")
    logging.info(f"Total days with alerts: {total_days}")
    logging.info(f"Average alerts per day: {avg_alerts_per_day:.2f}")
    logging.info(f"Average alerts per week (simple): {avg_alerts_per_week:.2f}")
    logging.info(f"Average alerts per week (actual): {avg_alerts_per_week_actual:.2f}")

    # plt.show()

    return {
        'total_alerts': total_alerts,
        'total_days': total_days,
        'avg_per_day': avg_alerts_per_day,
        'avg_per_week_simple': avg_alerts_per_week,
        'avg_per_week_actual': avg_alerts_per_week_actual,
        'alerts_per_day': alerts_per_day,
        'date_range_days': date_range.days,
        'chart_saved_to': filepath
    }

# Example usage with different date parsers for OpsGenie data

def parse_opsgenie_date(date_str: str) -> datetime:
    """Parser for OpsGenie date format like '2025-07-07T10:30:45.123Z'"""
    # Handle NaN or None values
    if pd.isna(date_str) or date_str is None:
        raise ValueError("Date string is None or NaN")

    # Convert to string if it's not already
    date_str = str(date_str).strip()

    # Handle various OpsGenie date formats
    formats = [
        '%Y-%m-%dT%H:%M:%S.%fZ',  # ISO format with microseconds
        '%Y-%m-%dT%H:%M:%SZ',     # ISO format without microseconds
        '%Y-%m-%d %H:%M:%S',      # Simple datetime format
        '%Y-%m-%d',               # Date only
        '%m/%d/%Y',               # US format
        '%d/%m/%Y'                # EU format
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            # strptime returns naive datetime, which is what we want
            return dt
        except ValueError:
            continue

    # If none of the formats work, try pandas to_datetime as fallback
    try:
        dt = pd.to_datetime(date_str)
        # Convert to naive datetime if it's timezone-aware
        if dt.tz is not None:
            dt = dt.tz_convert('UTC').tz_localize(None)
        return dt.to_pydatetime()
    except:
        raise ValueError(f"Unable to parse date: {date_str}")

def parse_iso_date(date_str: str) -> datetime:
    """Parser for ISO format dates like '2025-07-07'"""
    return datetime.strptime(date_str, '%Y-%m-%d')

def parse_us_date(date_str: str) -> datetime:
    """Parser for US format dates like '07/07/2025'"""
    return datetime.strptime(date_str, '%m/%d/%Y')

def parse_eu_date(date_str: str) -> datetime:
    """Parser for European format dates like '07/07/2025'"""
    return datetime.strptime(date_str, '%d/%m/%Y')

def parse_verbose_date(date_str: str) -> datetime:
    """Parser for verbose dates like 'July 7, 2025'"""
    return datetime.strptime(date_str, '%B %d, %Y')

def analyze_alert_status_breakdown(csv_file: str) -> dict:
    """
    Additional analysis function to break down alerts by status.

    Args:
        csv_file: Path to the CSV file

    Returns:
        Dictionary with status breakdown statistics
    """
    logging.info(f"Starting alert status breakdown analysis of {csv_file}")

    df = pd.read_csv(csv_file)

    # Analyze alert status distribution
    status_counts = df['Status'].value_counts()
    logging.info("Alert Status Breakdown:")
    for status, count in status_counts.items():
        logging.info(f"  {status}: {count} alerts ({count/len(df)*100:.1f}%)")

    # Analyze acknowledgment rates
    ack_rate = df['Acknowledged'].mean() * 100
    logging.info(f"Acknowledgment rate: {ack_rate:.1f}%")

    # Analyze team distribution
    if 'Teams' in df.columns:
        team_counts = df['Teams'].value_counts().head(10)
        logging.info("Top 10 Teams by Alert Count:")
        for team, count in team_counts.items():
            logging.info(f"  {team}: {count} alerts")

    return {
        'status_counts': status_counts.to_dict(),
        'acknowledgment_rate': ack_rate,
        'total_alerts': len(df)
    }

# Usage examples:

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create graph from deployments CSV")

    parser.add_argument("-c", "--config", type=str, help="path to config file")
    parser.add_argument("-f", "--csv_filepath", type=str, help="path to Opsgenie CSV")

    args = parser.parse_args()
    config_file = args.config
    csv = args.csv_filepath
    try:
        results = analyze_alerts_per_day(
            csv_file=csv,
            date_parser=parse_opsgenie_date,
            date_column='CreatedAtDate',
            title='Daily OpsGenie Alerts Analysis',
            config_file=config_file
        )

        logging.info(f"Key insight: You're averaging {results['avg_per_week_actual']:.1f} alerts per week")

        # Additional analysis
        status_results = analyze_alert_status_breakdown(csv)

    except Exception as e:
        logging.error(f"Error during analysis: {e}")
        logging.error("Make sure the CSV file exists and the date format matches your parser")

    # Example 2: If you had a different CSV with different date format
    # results = analyze_alerts_per_day(
    #     csv_file='other_alerts.csv',
    #     date_parser=parse_us_date,
    #     date_column='alert_date',
    #     title='Daily Alert Analysis',
    #     config_file='config.yaml'
    # )

    # Example 3: Custom date parser for your specific format
    # def custom_parser(date_str: str) -> datetime:
    #     # Handle your specific date format here
    #     return datetime.strptime(date_str, '%Y-%m-%d')
