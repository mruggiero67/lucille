#!/usr/bin/env python3
"""
GitHub Security Alerts Fetcher

Fetches open security alerts from GitHub organization repositories and generates:
1. CSV of all open security alerts with metadata
2. CSV of critical severity alerts only
3. PNG graph showing alert distribution by severity level
"""

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import matplotlib.pyplot as plt
import pandas as pd
import requests
import yaml

# Configure logging at module level
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-10s %(asctime)s %(filename)s %(lineno)d %(message)s",
)
logger = logging.getLogger(__name__)


def load_config(config_path: Optional[Path]) -> dict:
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


def calculate_alert_age(created_at: str) -> int:
    """
    Calculate the age of an alert in days.

    Pure function with no side effects.

    Args:
        created_at: ISO 8601 timestamp string

    Returns:
        Age in days as integer
    """
    created_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
    now = datetime.now(timezone.utc)
    age = (now - created_date).days
    return age


def extract_alert_type(alert: dict) -> str:
    """
    Extract the alert type from alert data.

    Pure function with no side effects.

    Args:
        alert: Alert dictionary from GitHub API

    Returns:
        Alert type string (Dependabot, Code Scanning, Secret Scanning)
    """
    # Determine type based on which endpoint it came from or data structure
    if 'dependency' in alert or 'security_advisory' in alert:
        return 'Dependabot'
    elif 'rule' in alert:
        return 'Code Scanning'
    elif 'secret_type' in alert:
        return 'Secret Scanning'
    else:
        return 'Unknown'


def get_alert_link(alert: dict, repo_name: str, alert_type: str) -> str:
    """
    Generate a link to the alert in GitHub.

    Pure function with no side effects.

    Args:
        alert: Alert dictionary
        repo_name: Repository name (org/repo)
        alert_type: Type of alert

    Returns:
        URL string to the alert
    """
    alert_number = alert.get('number', alert.get('id', ''))

    if alert_type == 'Dependabot':
        return f"https://github.com/{repo_name}/security/dependabot/{alert_number}"
    elif alert_type == 'Code Scanning':
        return f"https://github.com/{repo_name}/security/code-scanning/{alert_number}"
    elif alert_type == 'Secret Scanning':
        return f"https://github.com/{repo_name}/security/secret-scanning/{alert_number}"
    else:
        return alert.get('html_url', f"https://github.com/{repo_name}/security")


def get_code_location(alert: dict, alert_type: str) -> str:
    """
    Extract code location information from alert.

    Pure function with no side effects.

    Args:
        alert: Alert dictionary
        alert_type: Type of alert

    Returns:
        String describing code location
    """
    if alert_type == 'Dependabot':
        manifest_path = alert.get('dependency', {}).get('manifest_path', 'N/A')
        package_name = alert.get('dependency', {}).get('package', {}).get('name', 'Unknown')
        return f"{manifest_path} ({package_name})"
    elif alert_type == 'Code Scanning':
        location = alert.get('most_recent_instance', {}).get('location', {})
        path = location.get('path', 'N/A')
        start_line = location.get('start_line', '')
        if start_line:
            return f"{path}:{start_line}"
        return path
    elif alert_type == 'Secret Scanning':
        locations = alert.get('locations', [])
        if locations:
            loc = locations[0].get('details', {})
            path = loc.get('path', 'N/A')
            start_line = loc.get('start_line', '')
            if start_line:
                return f"{path}:{start_line}"
            return path
        return 'N/A'
    return 'N/A'


def fetch_repositories(org: str, token: str) -> List[str]:
    """
    Fetch all repository names for an organization.
    Filters out archived repositories

    Side-effecting function that makes API calls

    Args:
        org: GitHub organization name
        token: GitHub personal access token

    Returns:
        List of repository names (org/repo format)
    """
    logger.info(f"Fetching repositories for organization: {org}")

    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28'
    }

    repos = []
    page = 1
    per_page = 100

    while True:
        url = f'https://api.github.com/orgs/{org}/repos'
        params = {'page': page, 'per_page': per_page, 'type': 'all'}

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        page_repos = response.json()
        if not page_repos:
            break

        # filter out archived repositories
        repos.extend([f"{org}/{repo['name']}" for repo in page_repos if not repo.get('archived', False)])
        logger.debug(f"Fetched page {page}: {len(page_repos)} repositories")

        page += 1

        # Check if we've reached the last page
        if len(page_repos) < per_page:
            break

    logger.info(f"Found {len(repos)} repositories")
    return repos


def fetch_dependabot_alerts(repo: str, token: str) -> List[dict]:
    """
    TODO allow dependabot API calls to be paginated
    via cursor-based pagination

    Fetch Dependabot alerts for a repository.

    Side-effecting function that makes API calls.

    Args:
        repo: Repository name (org/repo)
        token: GitHub personal access token

    Returns:
        List of alert dictionaries
    """
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28'
    }

    alerts = []
    page = 1

    while True:
        url = f'https://api.github.com/repos/{repo}/dependabot/alerts'
        params = {'state': 'open'}

        try:
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 404:
                # Repository doesn't have Dependabot enabled or doesn't exist
                return []

            response.raise_for_status()
            page_alerts = response.json()

            if not page_alerts:
                break

            alerts.extend(page_alerts)

            if len(page_alerts) < 100:
                break

            page += 1

        except requests.exceptions.RequestException as e:
            logger.warning(f"--------> Failed to fetch Dependabot alerts for {repo}: {e} {response.json()}")
            break

    return alerts


def fetch_code_scanning_alerts(repo: str, token: str) -> List[dict]:
    """
    Fetch code scanning alerts for a repository.

    Side-effecting function that makes API calls.

    Args:
        repo: Repository name (org/repo)
        token: GitHub personal access token

    Returns:
        List of alert dictionaries
    """
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28'
    }

    alerts = []
    page = 1

    while True:
        url = f'https://api.github.com/repos/{repo}/code-scanning/alerts'
        params = {'state': 'open', 'page': page, 'per_page': 100}

        try:
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 404:
                return []

            response.raise_for_status()
            page_alerts = response.json()

            if not page_alerts:
                break

            alerts.extend(page_alerts)

            if len(page_alerts) < 100:
                break

            page += 1

        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch code scanning alerts for {repo}: {e}")
            break

    return alerts


def fetch_secret_scanning_alerts(repo: str, token: str) -> List[dict]:
    """
    Fetch secret scanning alerts for a repository.

    Side-effecting function that makes API calls.

    Args:
        repo: Repository name (org/repo)
        token: GitHub personal access token

    Returns:
        List of alert dictionaries
    """
    headers = {
        'Authorization': f'token {token}',
        'Accept': 'application/vnd.github+json',
        'X-GitHub-Api-Version': '2022-11-28'
    }

    alerts = []
    page = 1

    while True:
        url = f'https://api.github.com/repos/{repo}/secret-scanning/alerts'
        params = {'state': 'open', 'page': page, 'per_page': 100}

        try:
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 404:
                return []

            response.raise_for_status()
            page_alerts = response.json()

            if not page_alerts:
                break

            alerts.extend(page_alerts)

            if len(page_alerts) < 100:
                break

            page += 1

        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch secret scanning alerts for {repo}: {e}")
            break

    return alerts


def process_alerts(repos: List[str], token: str) -> List[dict]:
    """
    Process all security alerts for a list of repositories.

    Side-effecting function that makes API calls.

    Args:
        repos: List of repository names
        token: GitHub personal access token

    Returns:
        List of processed alert dictionaries
    """
    all_alerts = []

    for repo in repos:
        logger.info(f"Processing repository: {repo}")

        # Fetch all alert types
        dependabot_alerts = fetch_dependabot_alerts(repo, token)
        code_scanning_alerts = fetch_code_scanning_alerts(repo, token)
        secret_scanning_alerts = fetch_secret_scanning_alerts(repo, token)

        # Process Dependabot alerts
        for alert in dependabot_alerts:
            alert_type = 'Dependabot'
            severity = alert.get('security_advisory', {}).get('severity', 'unknown')

            processed = {
                'repository': repo,
                'alert_type': alert_type,
                'alert_id': alert.get('number'),
                'severity': severity.upper(),
                'title': alert.get('security_advisory', {}).get('summary', 'N/A'),
                'created_at': alert.get('created_at'),
                'age_days': calculate_alert_age(alert.get('created_at')),
                'code_location': get_code_location(alert, alert_type),
                'alert_link': get_alert_link(alert, repo, alert_type),
                'state': alert.get('state', 'open'),
                'cve_id': alert.get('security_advisory', {}).get('cve_id', 'N/A')
            }
            all_alerts.append(processed)

        # Process code scanning alerts
        for alert in code_scanning_alerts:
            alert_type = 'Code Scanning'
            severity = alert.get('rule', {}).get('security_severity_level', 'unknown')

            processed = {
                'repository': repo,
                'alert_type': alert_type,
                'alert_id': alert.get('number'),
                'severity': severity.upper() if severity else 'UNKNOWN',
                'title': alert.get('rule', {}).get('description', 'N/A'),
                'created_at': alert.get('created_at'),
                'age_days': calculate_alert_age(alert.get('created_at')),
                'code_location': get_code_location(alert, alert_type),
                'alert_link': get_alert_link(alert, repo, alert_type),
                'state': alert.get('state', 'open'),
                'cve_id': 'N/A'
            }
            all_alerts.append(processed)

        # Process secret scanning alerts
        for alert in secret_scanning_alerts:
            alert_type = 'Secret Scanning'

            processed = {
                'repository': repo,
                'alert_type': alert_type,
                'alert_id': alert.get('number'),
                'severity': 'HIGH',  # Secret scanning alerts are typically high severity
                'title': alert.get('secret_type_display_name', 'Secret detected'),
                'created_at': alert.get('created_at'),
                'age_days': calculate_alert_age(alert.get('created_at')),
                'code_location': get_code_location(alert, alert_type),
                'alert_link': get_alert_link(alert, repo, alert_type),
                'state': alert.get('state', 'open'),
                'cve_id': 'N/A'
            }
            all_alerts.append(processed)

        logger.debug(f"{repo}: {len(dependabot_alerts)} Dependabot, "
                    f"{len(code_scanning_alerts)} Code Scanning, "
                    f"{len(secret_scanning_alerts)} Secret Scanning")

    return all_alerts


def save_alerts_to_csv(alerts: List[dict], output_path: Path) -> None:
    """
    Save alerts to a CSV file.

    Side-effecting function that writes to filesystem.

    Args:
        alerts: List of alert dictionaries
        output_path: Path where CSV should be saved
    """
    logger.info(f"Saving {len(alerts)} alerts to {output_path}")

    df = pd.DataFrame(alerts)

    # Sort by severity and age
    severity_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'MODERATE': 3, 'LOW': 4, 'UNKNOWN': 5}
    df['severity_rank'] = df['severity'].map(lambda x: severity_order.get(x, 6))
    df = df.sort_values(['severity_rank', 'age_days'], ascending=[True, False])
    df = df.drop('severity_rank', axis=1)

    df.to_csv(output_path, index=False)
    logger.info(f"CSV saved successfully")


def save_critical_alerts_to_csv(alerts: List[dict], output_path: Path) -> None:
    """
    Save critical alerts to a CSV file with specific columns.

    Side-effecting function that writes to filesystem.

    Args:
        alerts: List of critical alert dictionaries
        output_path: Path where CSV should be saved
    """
    logger.info(f"Saving {len(alerts)} critical alerts to {output_path}")

    df = pd.DataFrame(alerts)

    # Select and reorder columns
    columns = ['created_at', 'repository', 'severity', 'age_days', 'alert_link', 'title']
    df = df[columns]

    # Sort by age (oldest first)
    df = df.sort_values('age_days', ascending=False)

    df.to_csv(output_path, index=False)
    logger.info(f"Critical alerts CSV saved successfully")


def create_severity_graph(alerts: List[dict], output_path: Path) -> None:
    """
    Create a bar chart showing alert counts by severity level.

    Side-effecting function that writes to filesystem.

    Args:
        alerts: List of alert dictionaries
        output_path: Path where PNG should be saved
    """
    logger.info(f"Creating severity distribution graph")

    # Count alerts by severity
    severity_counts = {}
    for alert in alerts:
        severity = alert['severity']
        severity_counts[severity] = severity_counts.get(severity, 0) + 1

    # Define severity order and colors
    severity_order = ['CRITICAL', 'HIGH', 'MEDIUM', 'MODERATE', 'LOW', 'UNKNOWN']
    severity_colors = {
        'CRITICAL': '#d32f2f',
        'HIGH': '#f57c00',
        'MEDIUM': '#fbc02d',
        'MODERATE': '#689f38',
        'LOW': '#1976d2',
        'UNKNOWN': '#757575'
    }

    # Filter to only severities that exist in the data
    severities = [s for s in severity_order if severity_counts.get(s, 0) > 0]
    counts = [severity_counts.get(s, 0) for s in severities]
    colors = [severity_colors[s] for s in severities]

    # Create bar chart
    plt.figure(figsize=(10, 6))
    bars = plt.bar(severities, counts, color=colors, edgecolor='black', linewidth=0.7)

    # Add value labels on top of bars
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2., height,
                f'{int(height)}',
                ha='center', va='bottom', fontsize=10, fontweight='bold')

    plt.xlabel('Severity Level', fontsize=12, fontweight='bold')
    plt.ylabel('Number of Alerts', fontsize=12, fontweight='bold')
    plt.title('GitHub Security Alerts by Severity Level', fontsize=14, fontweight='bold')
    plt.grid(axis='y', alpha=0.3, linestyle='--')
    plt.tight_layout()

    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()

    logger.info(f"Severity graph saved to {output_path}")


def main():
    """
    Main entry point for GitHub security alerts fetcher.
    """
    parser = argparse.ArgumentParser(
        description='Fetch open security alerts from GitHub organization repositories'
    )
    parser.add_argument(
        '--org',
        type=str,
        help='GitHub organization name (can also be specified in config file)'
    )
    parser.add_argument(
        '--token',
        type=str,
        help='GitHub personal access token (can also be specified in config file or GITHUB_TOKEN env var)'
    )
    parser.add_argument(
        '--config',
        type=Path,
        required=True,
        help='Path to YAML configuration file with "org", "github_token", and "csv_directory" keys'
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

    # Load configuration
    config = load_config(args.config)
    if not config:
        logger.error(f"Failed to load configuration from {args.config}")
        return 1

    # Get org from config file (first) or CLI args (second)
    org = config.get('org') or args.org
    if not org:
        logger.error("GitHub organization is required. Provide via --org argument or 'org' key in config file")
        return 1

    # Get token from config file (first), CLI args (second), or environment (third)
    token = config.get('github_token') or args.token
    if not token:
        import os
        token = os.getenv('GITHUB_TOKEN')

    if not token:
        logger.error("GitHub token is required. Provide via --token argument, 'github_token' key in config file, or GITHUB_TOKEN environment variable")
        return 1

    # Get output directory from config file
    csv_directory = config.get('csv_directory')
    if not csv_directory:
        logger.error("Output directory is required. Provide via 'csv_directory' key in config file")
        return 1

    output_dir = Path(config.get('output_directory'))

    # Fetch repositories
    try:
        repos = fetch_repositories(org, token)
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch repositories: {e}")
        return 1

    if not repos:
        logger.warning("No repositories found")
        return 0

    # Process alerts
    alerts = process_alerts(repos, token)

    if not alerts:
        logger.warning("No open security alerts found")
        return 0

    # Generate output files
    timestamp = datetime.now().strftime("%Y_%m_%d")
    all_alerts_path = output_dir / f'{timestamp}_github_security_alerts_all.csv'
    critical_alerts_path = output_dir / f'{timestamp}_github_security_alerts_critical.csv'
    severity_graph_path = output_dir / f'{timestamp}_github_security_alerts_severity.png'

    # Save all alerts
    save_alerts_to_csv(alerts, all_alerts_path)

    # Filter and save critical alerts
    critical_alerts = [a for a in alerts if a['severity'] == 'CRITICAL']
    if critical_alerts:
        save_critical_alerts_to_csv(critical_alerts, critical_alerts_path)
        logger.info(f"Saved {len(critical_alerts)} critical alerts")
    else:
        logger.info("No critical alerts found")

    # Create severity distribution graph
    create_severity_graph(alerts, severity_graph_path)

    # Log summary
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total Alerts: {len(alerts)}")

    # Count by type
    by_type = {}
    for alert in alerts:
        alert_type = alert['alert_type']
        by_type[alert_type] = by_type.get(alert_type, 0) + 1

    logger.info("\nBy Type:")
    for alert_type, count in sorted(by_type.items()):
        logger.info(f"  {alert_type}: {count}")

    # Count by severity
    by_severity = {}
    for alert in alerts:
        severity = alert['severity']
        by_severity[severity] = by_severity.get(severity, 0) + 1

    logger.info("\nBy Severity:")
    for severity in ['CRITICAL', 'HIGH', 'MEDIUM', 'MODERATE', 'LOW', 'UNKNOWN']:
        count = by_severity.get(severity, 0)
        if count > 0:
            logger.info(f"  {severity}: {count}")

    logger.info(f"\nOutput files:")
    logger.info(f"  All alerts: {all_alerts_path}")
    if critical_alerts:
        logger.info(f"  Critical alerts: {critical_alerts_path}")
    logger.info(f"  Severity graph: {severity_graph_path}")

    return 0


if __name__ == '__main__':
    exit(main())
