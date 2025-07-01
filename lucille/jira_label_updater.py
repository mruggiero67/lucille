#!/usr/bin/env python3
"""
Jira Epic Label Bulk Updater
Reads CSV file with epic keys and labels, then bulk applies labels to Jira epics.
Configuration driven by YAML file.
"""

import requests
import csv
import yaml
import base64
from datetime import datetime
import os
import sys
from typing import List, Dict, Any, Set
from pathlib import Path
import time


class JiraLabelUpdater:
    def __init__(self, config: Dict[str, Any]):
        """Initialize with Jira configuration."""
        self.base_url = config["jira"]["base_url"].rstrip("/")
        self.username = config["jira"]["username"]
        self.api_token = config["jira"]["api_token"]

        # Auth header
        auth_string = f"{self.username}:{self.api_token}"
        auth_bytes = auth_string.encode("ascii")
        auth_b64 = base64.b64encode(auth_bytes).decode("ascii")

        self.headers = {
            "Authorization": f"Basic {auth_b64}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        # Configuration
        self.csv_file = config["csv_file"]
        self.dry_run = config.get("dry_run", True)
        self.rate_limit_delay = config.get(
            "rate_limit_delay", 1.0
        )  # seconds between API calls
        self.preserve_existing = config.get("preserve_existing_labels", True)
        self.output_directory = config.get("output_directory", "./label_update_logs")

    def test_connection(self) -> bool:
        """Test connection to Jira API."""
        try:
            url = f"{self.base_url}/rest/api/3/myself"
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            user_info = response.json()
            print(
                f"Connected to Jira as: {user_info.get('displayName', self.username)}"
            )
            return True
        except requests.exceptions.RequestException as e:
            print(f"Failed to connect to Jira: {e}")
            return False

    def read_csv_file(self) -> List[Dict[str, Any]]:
        """Read CSV file and parse epic label data."""
        try:
            with open(self.csv_file, "r", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                rows = list(reader)

            print(f"Read {len(rows)} rows from {self.csv_file}")

            # Parse label data
            parsed_data = []
            for row in rows:
                epic_key = row.get("epic_key", "").strip()
                if not epic_key:
                    continue

                # Collect all label columns
                labels = []
                for key, value in row.items():
                    if key.startswith("label_") and value and value.strip():
                        labels.append(value.strip())

                if labels:
                    parsed_data.append(
                        {"epic_key": epic_key, "labels_to_add": labels, "raw_row": row}
                    )

            print(f"Parsed {len(parsed_data)} epics with labels to update")
            return parsed_data

        except FileNotFoundError:
            print(f"Error: CSV file '{self.csv_file}' not found")
            sys.exit(1)
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            sys.exit(1)

    def get_epic_current_labels(self, epic_key: str) -> Set[str]:
        """Get current labels for an epic."""
        try:
            url = f"{self.base_url}/rest/api/3/issue/{epic_key}"
            params = {"fields": "labels"}

            response = requests.get(
                url, headers=self.headers, params=params, timeout=10
            )
            response.raise_for_status()

            issue_data = response.json()
            current_labels = set(issue_data["fields"].get("labels", []))

            return current_labels

        except requests.exceptions.RequestException as e:
            print(f"  Error fetching current labels for {epic_key}: {e}")
            return set()

    def update_epic_labels(self, epic_key: str, new_labels: List[str]) -> bool:
        """Update labels for a specific epic."""
        try:
            # Get current labels if preserving existing
            if self.preserve_existing:
                current_labels = self.get_epic_current_labels(epic_key)
            else:
                current_labels = set()

            # Combine current and new labels
            all_labels = current_labels.union(set(new_labels))
            labels_list = sorted(list(all_labels))  # Sort for consistency

            # Update the epic
            url = f"{self.base_url}/rest/api/3/issue/{epic_key}"
            update_data = {"fields": {"labels": labels_list}}

            if self.dry_run:
                print(f"  DRY RUN - Would update {epic_key} with labels: {labels_list}")
                return True
            else:
                response = requests.put(
                    url, headers=self.headers, json=update_data, timeout=10
                )
                response.raise_for_status()
                print(f"  ✓ Updated {epic_key} with labels: {new_labels}")
                return True

        except requests.exceptions.RequestException as e:
            print(f"  ✗ Error updating {epic_key}: {e}")
            return False

    def process_updates(self, epic_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Process all label updates."""
        print(f"\nProcessing {len(epic_data)} epic label updates...")
        if self.dry_run:
            print("*** DRY RUN MODE - No actual changes will be made ***")

        results = {
            "total_epics": len(epic_data),
            "successful_updates": 0,
            "failed_updates": 0,
            "skipped_updates": 0,
            "details": [],
        }

        for i, epic_info in enumerate(epic_data, 1):
            epic_key = epic_info["epic_key"]
            labels_to_add = epic_info["labels_to_add"]

            print(f"\n[{i}/{len(epic_data)}] Processing {epic_key}")
            print(f"  Labels to add: {labels_to_add}")

            # Skip if no labels to add
            if not labels_to_add:
                print(f"  ⚠ Skipping {epic_key} - no labels specified")
                results["skipped_updates"] += 1
                results["details"].append(
                    {
                        "epic_key": epic_key,
                        "status": "skipped",
                        "reason": "no_labels",
                        "labels": [],
                    }
                )
                continue

            # Attempt update
            success = self.update_epic_labels(epic_key, labels_to_add)

            if success:
                results["successful_updates"] += 1
                results["details"].append(
                    {"epic_key": epic_key, "status": "success", "labels": labels_to_add}
                )
            else:
                results["failed_updates"] += 1
                results["details"].append(
                    {"epic_key": epic_key, "status": "failed", "labels": labels_to_add}
                )

            # Rate limiting
            if i < len(epic_data):  # Don't sleep after the last item
                time.sleep(self.rate_limit_delay)

        return results

    def save_results_log(self, results: Dict[str, Any]):
        """Save update results to log file."""
        Path(self.output_directory).mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"label_update_log_{timestamp}.csv"
        filepath = Path(self.output_directory) / filename

        fieldnames = ["epic_key", "status", "labels", "reason"]

        with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for detail in results["details"]:
                writer.writerow(
                    {
                        "epic_key": detail["epic_key"],
                        "status": detail["status"],
                        "labels": ", ".join(detail["labels"]),
                        "reason": detail.get("reason", ""),
                    }
                )

        print(f"\nResults log saved to: {filepath}")
        return str(filepath)

    def print_summary(self, results: Dict[str, Any]):
        """Print summary of update results."""
        print("\n" + "=" * 60)
        print("LABEL UPDATE SUMMARY")
        print("=" * 60)
        print(f"Total Epics Processed: {results['total_epics']}")
        print(f"Successful Updates: {results['successful_updates']}")
        print(f"Failed Updates: {results['failed_updates']}")
        print(f"Skipped Updates: {results['skipped_updates']}")

        if results["failed_updates"] > 0:
            print(f"\nFailed Updates:")
            for detail in results["details"]:
                if detail["status"] == "failed":
                    print(f"  ✗ {detail['epic_key']}")

        if results["successful_updates"] > 0:
            print(f"\nSuccessful Updates (showing first 10):")
            successful = [d for d in results["details"] if d["status"] == "success"]
            for detail in successful[:10]:
                print(f"  ✓ {detail['epic_key']}: {', '.join(detail['labels'])}")
            if len(successful) > 10:
                print(f"  ... and {len(successful) - 10} more")

        success_rate = (
            (results["successful_updates"] / results["total_epics"] * 100)
            if results["total_epics"] > 0
            else 0
        )
        print(f"\nSuccess Rate: {success_rate:.1f}%")


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_path}' not found.")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing YAML configuration: {e}")
        sys.exit(1)


def validate_config(config: Dict[str, Any]) -> bool:
    """Validate configuration."""
    required_keys = ["jira", "csv_file"]
    for key in required_keys:
        if key not in config:
            print(f"Error: Missing required configuration key '{key}'")
            return False

    # Check Jira config
    jira_config = config["jira"]
    required_jira_keys = ["base_url", "username", "api_token"]
    for key in required_jira_keys:
        if key not in jira_config:
            print(f"Error: Missing required Jira configuration key '{key}'")
            return False

    # Check CSV file exists
    if not os.path.exists(config["csv_file"]):
        print(f"Error: CSV file '{config['csv_file']}' not found")
        return False

    return True


def create_sample_config(config_path: str):
    """Create sample configuration file."""
    sample_config = {
        "jira": {
            "base_url": "https://your-domain.atlassian.net",
            "username": "your-email@company.com",
            "api_token": "your_jira_api_token",
        },
        "csv_file": "epic_labels.csv",  # Path to your CSV file
        "dry_run": True,  # Set to False to actually make changes
        "preserve_existing_labels": True,  # Keep existing labels when adding new ones
        "rate_limit_delay": 1.0,  # Seconds to wait between API calls
        "output_directory": "./label_update_logs",
    }

    with open(config_path, "w") as file:
        yaml.dump(sample_config, file, default_flow_style=False, sort_keys=False)

    print(f"Sample configuration created at '{config_path}'")
    print("Please edit this file with your Jira credentials and CSV file path.")


def create_sample_csv():
    """Create a sample CSV file for reference."""
    sample_data = [
        {
            "epic_key": "PROJ-123",
            "label_1": "initiative:mobile-redesign",
            "label_2": "phase:development",
            "label_3": "team:frontend",
            "label_4": "priority:p1",
            "label_5": "",
            "label_6": "",
        },
        {
            "epic_key": "PROJ-456",
            "label_1": "initiative:payment-system",
            "label_2": "phase:testing",
            "label_3": "team:backend",
            "label_4": "priority:p1",
            "label_5": "dependency:third-party-vendor",
            "label_6": "",
        },
        {
            "epic_key": "FRONTEND-789",
            "label_1": "initiative:mobile-redesign",
            "label_2": "phase:launch-prep",
            "label_3": "team:frontend",
            "label_4": "priority:p2",
            "label_5": "target:2025-q2",
            "label_6": "impact:user-experience",
        },
    ]

    filename = "epic_labels_sample.csv"
    fieldnames = [
        "epic_key",
        "label_1",
        "label_2",
        "label_3",
        "label_4",
        "label_5",
        "label_6",
    ]

    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sample_data)

    print(f"Sample CSV created at '{filename}'")
    print("Use this as a template for your epic labeling data.")


def main():
    """Main function."""
    config_path = "/Users/michael@jaris.io/bin/jira_epic_config.yaml"

    if not os.path.exists(config_path):
        print(f"Configuration file not found. Creating sample files...")
        create_sample_config(config_path)
        create_sample_csv()
        print("\nPlease edit the configuration file and CSV, then run again.")
        return

    config = load_config(config_path)
    if not validate_config(config):
        sys.exit(1)

    # Initialize updater
    updater = JiraLabelUpdater(config)

    # Test connection
    if not updater.test_connection():
        print("Failed to connect to Jira. Please check your configuration.")
        sys.exit(1)

    # Read CSV data
    epic_data = updater.read_csv_file()
    if not epic_data:
        print("No epic data found in CSV file.")
        sys.exit(1)

    # Confirm before proceeding (unless dry run)
    if not config.get("dry_run", True):
        print(f"\n⚠️  WARNING: This will update {len(epic_data)} epics in Jira!")
        confirm = input("Are you sure you want to proceed? (yes/no): ").lower()
        if confirm != "yes":
            print("Operation cancelled.")
            return

    # Process updates
    results = updater.process_updates(epic_data)

    # Save results and print summary
    updater.save_results_log(results)
    updater.print_summary(results)

    if config.get("dry_run", True):
        print(f"\n💡 Tip: Set 'dry_run: False' in config to actually apply changes")


if __name__ == "__main__":
    main()
