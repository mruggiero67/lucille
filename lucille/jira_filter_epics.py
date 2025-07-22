#!/usr/bin/env python3
"""
Jira Epic Key Extractor

This script calls a series of Jira filters via API, extracts epic keys,
deduplicates them, and saves the results to a CSV file.

Usage:
    python jira_epic_extractor.py config.yaml
"""

import argparse
import csv
import logging
import sys
from pathlib import Path
from typing import List, Set

import requests
import yaml
from requests.auth import HTTPBasicAuth


def setup_logging(level: str = "INFO") -> None:
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_config(config_path: str) -> dict:
    """Load configuration from YAML file."""
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        logging.info(f"Configuration loaded from {config_path}")
        return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {config_path}")
        sys.exit(1)
    except yaml.YAMLError as e:
        logging.error(f"Error parsing YAML configuration: {e}")
        sys.exit(1)


def validate_config(config: dict) -> None:
    """Validate required configuration parameters."""
    required_keys = [
        "jira",
        "filter_ids",
        "epic_keys_file",
    ]

    for key in required_keys:
        if key not in config:
            logging.error(f"Missing required configuration key: {key}")
            sys.exit(1)

    if not isinstance(config["filter_ids"], list):
        logging.error("filter_ids must be a list")
        sys.exit(1)

    if not config["filter_ids"]:
        logging.error("filter_ids list cannot be empty")
        sys.exit(1)

    logging.info("Configuration validation passed")


def create_jira_session(
    base_url: str, username: str, api_token: str
) -> requests.Session:
    """Create and configure a requests session for Jira API calls."""
    session = requests.Session()
    session.auth = HTTPBasicAuth(username, api_token)
    session.headers.update(
        {"Accept": "application/json", "Content-Type": "application/json"}
    )

    # Test the connection
    test_url = f"{base_url.rstrip('/')}/rest/api/2/myself"
    try:
        response = session.get(test_url)
        response.raise_for_status()
        user_info = response.json()
        logging.info(
            f"Successfully authenticated as: {user_info.get('displayName', username)}"
        )
        return session
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to authenticate with Jira: {e}")
        sys.exit(1)


def get_filter_issues(
    session: requests.Session, base_url: str, filter_id: int, max_results: int = 1000
) -> List[dict]:
    """Retrieve all issues from a Jira filter."""
    url = f"{base_url.rstrip('/')}/rest/api/2/search"
    all_issues = []
    start_at = 0

    while True:
        params = {
            "jql": f"filter = {filter_id}",
            "fields": "key,summary,issuetype,status",
            "maxResults": max_results,
            "startAt": start_at,
        }

        try:
            response = session.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            issues = data.get("issues", [])
            all_issues.extend(issues)

            logging.info(
                f"Filter {filter_id}: Retrieved {len(issues)} issues "
                f"(total so far: {len(all_issues)})"
            )

            if len(issues) < max_results:
                break

            start_at += max_results

        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to retrieve issues from filter {filter_id}: {e}")
            return []

    logging.info(f"Filter {filter_id}: Total issues retrieved: {len(all_issues)}")
    return all_issues


def extract_epic_keys(issues: List[dict]) -> Set[str]:
    """Extract epic keys from issues list."""
    epic_keys = set()

    for issue in issues:
        # Check if the issue itself is an Epic
        issue_type = (
            issue.get("fields", {}).get("issuetype", {}).get("name", "").lower()
        )
        if issue_type == "epic":
            epic_keys.add(issue["key"])

        # Check if the issue has an Epic Link field (common custom field names)
        fields = issue.get("fields", {})

        # Common Epic Link field names (adjust based on your Jira configuration)
        epic_link_fields = [
            "customfield_10014",  # Common default Epic Link field
            "customfield_10008",  # Another common Epic Link field
            "parent",  # For issues under epics in some configurations
        ]

        for field_name in epic_link_fields:
            if field_name in fields and fields[field_name]:
                if isinstance(fields[field_name], dict) and "key" in fields[field_name]:
                    epic_keys.add(fields[field_name]["key"])
                elif isinstance(fields[field_name], str):
                    epic_keys.add(fields[field_name])

    return epic_keys


def save_epic_keys_to_csv(epic_keys: Set[str], epic_keys_file: str) -> None:
    """Save epic keys to CSV file."""
    try:
        # Sort keys for consistent output
        sorted_keys = sorted(epic_keys)

        with open(epic_keys_file, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["epic_key"])  # Header

            for key in sorted_keys:
                writer.writerow([key])

        logging.info(
            f"Successfully saved {len(epic_keys)} unique epic keys to {epic_keys_file}"
        )

    except IOError as e:
        logging.error(f"Failed to write to output file {epic_keys_file}: {e}")
        sys.exit(1)


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description="Extract epic keys from Jira filters")
    parser.add_argument("config", help="Path to YAML configuration file")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level (default: INFO)",
    )

    args = parser.parse_args()

    setup_logging(args.log_level)
    logging.info("Starting Jira epic key extraction")

    # Load and validate configuration
    config = load_config(args.config)
    validate_config(config)

    # Create Jira session
    session = create_jira_session(
        config["jira"]["base_url"],
        config["jira"]["username"],
        config["jira"]["api_token"]
    )

    all_epic_keys = set()

    # Process each filter
    for filter_id in config["filter_ids"]:
        logging.info(f"Processing filter ID: {filter_id}")

        issues = get_filter_issues(
            session, config["jira"]["base_url"], filter_id
        )
        if not issues:
            logging.warning(f"No issues found for filter {filter_id}")
            continue

        epic_keys = extract_epic_keys(issues)
        logging.info(f"Filter {filter_id}: Found {len(epic_keys)} unique epic keys")

        all_epic_keys.update(epic_keys)

    if not all_epic_keys:
        logging.warning("No epic keys found across all filters")
        return

    logging.info(
        f"Total unique epic keys found across all filters: {len(all_epic_keys)}"
    )

    # Save to CSV
    save_epic_keys_to_csv(all_epic_keys, config["epic_keys_file"])

    logging.info("Epic key extraction completed successfully")


if __name__ == "__main__":
    main()
