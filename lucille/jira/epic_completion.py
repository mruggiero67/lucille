#!/usr/bin/env python3
"""
Jira Epic Completion Analyzer
Analyzes epic completion rates by examining all child stories and their statuses.
Configuration driven by YAML file.
"""

import requests
import csv
import yaml
import base64
from datetime import datetime
import os
import sys
import argparse
from typing import List, Dict, Any
from pathlib import Path
import json
import logging

logging.basicConfig(
        format="%(levelname)-10s %(asctime)s %(filename)s %(lineno)d %(message)s",
        level=logging.DEBUG,
)


class JiraEpicAnalyzer:
    def __init__(self, config: Dict[str, Any], epic_keys: List[str]):
        """
        Initialize the analyzer with Jira configuration.

        Args:
            config: Configuration dictionary from YAML
        """
        self.base_url = config["jira"]["base_url"].rstrip("/")
        self.username = config["jira"]["username"]
        self.api_token = config["jira"]["api_token"]

        # Create auth header
        auth_string = f"{self.username}:{self.api_token}"
        auth_bytes = auth_string.encode("ascii")
        auth_b64 = base64.b64encode(auth_bytes).decode("ascii")
        self.headers = { "Authorization": f"Basic {auth_b64}", "Accept": "application/json",
            "Content-Type": "application/json",
        }

        # Status configuration
        self.done_statuses = set(
            status.upper() for status in config.get("done_statuses", ["Done", "Closed", "Resolved"])
        )
        self.output_directory = config["output_directory"]

        # Epic list
        self.epic_keys = epic_keys

    def test_connection(self) -> bool:
        """
        Test connection to Jira API.

        Returns:
            True if connection successful, False otherwise
        """
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

    def get_epic_details(self, epic_key: str) -> Dict[str, Any]:
        """
        Get basic details about an epic.

        Args:
            epic_key: Epic key (e.g., 'PROJ-123')

        Returns:
            Epic details dictionary
        """
        try:
            url = f"{self.base_url}/rest/api/3/issue/{epic_key}"
            params = {
                "fields": "summary,status,assignee,created,updated,labels,priority"
            }

            response = requests.get(
                url, headers=self.headers, params=params, timeout=10
            )
            response.raise_for_status()

            issue = response.json()
            fields = issue["fields"]

            return {
                "key": epic_key,
                "summary": fields.get("summary", ""),
                "status": fields.get("status", {}).get("name", "Unknown"),
                "assignee": (
                    fields.get("assignee", {}).get("displayName", "Unassigned")
                    if fields.get("assignee")
                    else "Unassigned"
                ),
                "created": (
                    fields.get("created", "")[:10] if fields.get("created") else ""
                ),
                "updated": (
                    fields.get("updated", "")[:10] if fields.get("updated") else ""
                ),
                "priority": (
                    fields.get("priority", {}).get("name", "Unknown")
                    if fields.get("priority")
                    else "Unknown"
                ),
                "labels": ", ".join(fields.get("labels", [])),
            }

        except requests.exceptions.RequestException as e:
            print(f"Error fetching epic {epic_key}: {e}")
            return {
                "key": epic_key,
                "summary": "Error fetching epic",
                "status": "Unknown",
                "assignee": "Unknown",
                "created": "",
                "updated": "",
                "priority": "Unknown",
                "labels": "",
            }

    def get_epic_children(self, epic_key: str) -> List[Dict[str, Any]]:
        """
        Get all child issues (stories, tasks, bugs) for an epic.

        Args:
            epic_key: Epic key (e.g., 'PROJ-123')

        Returns:
            List of child issue dictionaries
        """
        children = []
        start_at = 0
        max_results = 100

        while True:
            try:
                # JQL to find all issues that are children of the epic
                jql = f'"Epic Link" = {epic_key} OR parent = {epic_key}'

                url = f"{self.base_url}/rest/api/3/search/jql"
                params = {
                    "jql": jql,
                    "fields": "summary,status,issuetype,assignee,created,updated,priority,resolution,resolutiondate",
                    "startAt": start_at,
                    "maxResults": max_results,
                }

                response = requests.get(
                    url, headers=self.headers, params=params, timeout=10
                )
                response.raise_for_status()

                data = response.json()
                issues = data.get("issues", [])

                if not issues:
                    break

                for issue in issues:
                    fields = issue["fields"]

                    child_info = {
                        "key": issue["key"],
                        "summary": fields.get("summary", ""),
                        "status": fields.get("status", {}).get("name", "Unknown"),
                        "issue_type": fields.get("issuetype", {}).get(
                            "name", "Unknown"
                        ),
                        "assignee": (
                            fields.get("assignee", {}).get("displayName", "Unassigned")
                            if fields.get("assignee")
                            else "Unassigned"
                        ),
                        "created": (
                            fields.get("created", "")[:10]
                            if fields.get("created")
                            else ""
                        ),
                        "updated": (
                            fields.get("updated", "")[:10]
                            if fields.get("updated")
                            else ""
                        ),
                        "priority": (
                            fields.get("priority", {}).get("name", "Unknown")
                            if fields.get("priority")
                            else "Unknown"
                        ),
                        "resolution": (
                            fields.get("resolution", {}).get("name", "")
                            if fields.get("resolution")
                            else ""
                        ),
                        "resolution_date": (
                            fields.get("resolutiondate", "")[:10]
                            if fields.get("resolutiondate")
                            else ""
                        ),
                    }

                    children.append(child_info)

                # Check if we need to fetch more
                if len(issues) < max_results:
                    break

                start_at += max_results

            except requests.exceptions.RequestException as e:
                print(f"Error fetching children for epic {epic_key}: {e}")
                break

        return children

    def analyze_epic_completion(self, epic_key: str) -> Dict[str, Any]:
        """
        Analyze completion status of an epic based on its children.

        Args:
            epic_key: Epic key to analyze

        Returns:
            Analysis dictionary with completion metrics
        """
        print(f"Analyzing epic: {epic_key}")

        # Get epic details
        epic_details = self.get_epic_details(epic_key)

        # Get all children
        children = self.get_epic_children(epic_key)

        if not children:
            print(f"  No children found for epic {epic_key}")
            return {
                **epic_details,
                "total_children": 0,
                "done_children": 0,
                "not_done_children": 0,
                "completion_percentage": 0,
                "child_statuses": {},
                "child_types": {},
            }

        print(f"  Found {len(children)} children")

        # Analyze status distribution
        status_counts = {}
        type_counts = {}
        done_count = 0

        for child in children:
            status = child["status"]
            issue_type = child["issue_type"]

            # Count statuses
            status_counts[status] = status_counts.get(status, 0) + 1

            # Count issue types
            type_counts[issue_type] = type_counts.get(issue_type, 0) + 1

            # Check if done
            if status.upper() in self.done_statuses:
                done_count += 1

        total_children = len(children)
        not_done_count = total_children - done_count
        completion_percentage = (
            (done_count / total_children * 100) if total_children > 0 else 0
        )

        return {
            **epic_details,
            "total_children": total_children,
            "done_children": done_count,
            "not_done_children": not_done_count,
            "completion_percentage": round(completion_percentage, 1),
            "child_statuses": status_counts,
            "child_types": type_counts,
            "children_details": children,
        }

    def analyze_all_epics(self) -> List[Dict[str, Any]]:
        """
        Analyze all epics from the configuration.

        Returns:
            List of epic analysis results
        """
        results = []

        print(f"Analyzing {len(self.epic_keys)} epics...")

        for epic_key in self.epic_keys:
            analysis = self.analyze_epic_completion(epic_key)
            results.append(analysis)

        # Sort by completion percentage (lowest first to highlight blockers)
        results.sort(key=lambda x: x["completion_percentage"])

        return results

    def save_epic_summary_csv(
        self, analyses: List[Dict[str, Any]], filename: str = None
    ):
        """
        Save epic summary to CSV file.

        Args:
            analyses: List of epic analysis dictionaries
            filename: Optional custom filename
        """
        if not analyses:
            print("No epic analyses to save")
            return

        # Create output directory if it doesn't exist
        Path(self.output_directory).mkdir(parents=True, exist_ok=True)

        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"epic_completion_summary_{timestamp}.csv"

        filepath = Path(self.output_directory) / filename

        # Epic summary fieldnames
        fieldnames = [
            "epic_key",
            "summary",
            "epic_status",
            "assignee",
            "created",
            "updated",
            "priority",
            "labels",
            "total_children",
            "done_children",
            "not_done_children",
            "completion_percentage",
            "child_statuses",
            "child_types",
        ]

        with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for analysis in analyses:
                # Flatten the data for CSV
                row = {
                    "epic_key": analysis["key"],
                    "summary": analysis["summary"],
                    "epic_status": analysis["status"],
                    "assignee": analysis["assignee"],
                    "created": analysis["created"],
                    "updated": analysis["updated"],
                    "priority": analysis["priority"],
                    "labels": analysis["labels"],
                    "total_children": analysis["total_children"],
                    "done_children": analysis["done_children"],
                    "not_done_children": analysis["not_done_children"],
                    "completion_percentage": analysis["completion_percentage"],
                    "child_statuses": json.dumps(analysis["child_statuses"]),
                    "child_types": json.dumps(analysis["child_types"]),
                }
                writer.writerow(row)

        print(f"Epic summary saved to {filepath}")
        return str(filepath)

    def save_detailed_csv(self, analyses: List[Dict[str, Any]], filename: str = None):
        """
        Save detailed child story information to CSV.

        Args:
            analyses: List of epic analysis dictionaries
            filename: Optional custom filename
        """
        if not analyses:
            print("No epic analyses to save")
            return

        # Create output directory if it doesn't exist
        Path(self.output_directory).mkdir(parents=True, exist_ok=True)

        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"epic_children_detailed_{timestamp}.csv"

        filepath = Path(self.output_directory) / filename

        # Detailed fieldnames
        fieldnames = [
            "epic_key",
            "epic_summary",
            "epic_completion_percentage",
            "child_key",
            "child_summary",
            "child_status",
            "child_type",
            "child_assignee",
            "child_created",
            "child_updated",
            "child_priority",
            "child_resolution",
            "child_resolution_date",
            "is_done",
        ]

        with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for analysis in analyses:
                epic_key = analysis["key"]
                epic_summary = analysis["summary"]
                epic_completion = analysis["completion_percentage"]

                for child in analysis.get("children_details", []):
                    is_done = child["status"].upper() in self.done_statuses

                    row = {
                        "epic_key": epic_key,
                        "epic_summary": epic_summary,
                        "epic_completion_percentage": epic_completion,
                        "child_key": child["key"],
                        "child_summary": child["summary"],
                        "child_status": child["status"],
                        "child_type": child["issue_type"],
                        "child_assignee": child["assignee"],
                        "child_created": child["created"],
                        "child_updated": child["updated"],
                        "child_priority": child["priority"],
                        "child_resolution": child["resolution"],
                        "child_resolution_date": child["resolution_date"],
                        "is_done": is_done,
                    }
                    writer.writerow(row)

        print(f"Detailed children data saved to {filepath}")
        return str(filepath)

    def print_summary(self, analyses: List[Dict[str, Any]]):
        """
        Print summary of epic completion analysis.

        Args:
            analyses: List of epic analysis results
        """
        if not analyses:
            print("No epic analyses to summarize")
            return

        print("\n" + "=" * 80)
        print("EPIC COMPLETION ANALYSIS SUMMARY")
        print("=" * 80)

        total_epics = len(analyses)
        completed_epics = len(
            [a for a in analyses if a["completion_percentage"] == 100]
        )
        blocked_epics = len([a for a in analyses if a["completion_percentage"] == 0])
        avg_completion = sum(a["completion_percentage"] for a in analyses) / total_epics

        print(f"Total Epics Analyzed: {total_epics}")
        print(
            f"Fully Completed Epics: {completed_epics} ({completed_epics/total_epics*100:.1f}%)"
        )
        print(
            f"Not Started Epics: {blocked_epics} ({blocked_epics/total_epics*100:.1f}%)"
        )
        print(f"Average Completion: {avg_completion:.1f}%")

        print(f"\nEpic Completion Breakdown:")
        print("-" * 80)
        print(
            f"{'Epic Key':<15} {'Summary':<40} {'Progress':<10} {'Children':<10} {'Done':<6}"
        )
        print("-" * 80)

        for analysis in analyses:
            epic_key = analysis["key"]
            summary = (
                analysis["summary"][:37] + "..."
                if len(analysis["summary"]) > 40
                else analysis["summary"]
            )
            progress = f"{analysis['completion_percentage']}%"
            children = f"{analysis['done_children']}/{analysis['total_children']}"
            done = "✓" if analysis["completion_percentage"] == 100 else ""

            print(
                f"{epic_key:<15} {summary:<40} {progress:<10} {children:<10} {done:<6}"
            )

        # Highlight attention-needed epics
        needs_attention = [a for a in analyses if 0 < a["completion_percentage"] < 100]
        if needs_attention:
            print(f"\nEpics Needing Attention ({len(needs_attention)} epics):")
            for analysis in needs_attention[:5]:  # Top 5
                print(
                    f"  {analysis['key']}: {analysis['completion_percentage']:.1f}% complete ({analysis['not_done_children']} remaining)"
                )


def load_config(config_path: str) -> Dict[str, Any]:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to the YAML configuration file

    Returns:
        Configuration dictionary
    """
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
    """
    Validate the configuration dictionary.

    Args:
        config: Configuration dictionary

    Returns:
        True if valid, False otherwise
    """
    # Check required top-level keys
    required_keys = ["jira", "epics", "output_directory"]
    for key in required_keys:
        if key not in config:
            print(f"Error: Missing required configuration key '{key}'")
            return False

    # Check Jira configuration
    jira_config = config["jira"]
    required_jira_keys = ["base_url", "username", "api_token"]
    for key in required_jira_keys:
        if key not in jira_config:
            print(f"Error: Missing required Jira configuration key '{key}'")
            return False

    # Check epics list
    if not isinstance(config["epics"], list) or not config["epics"]:
        print("Error: 'epics' must be a non-empty list")
        return False

    return True


def create_sample_config(config_path: str):
    """
    Create a sample configuration file.

    Args:
        config_path: Path where to create the sample config
    """
    sample_config = {
        "jira": {
            "base_url": "https://your-domain.atlassian.net",
            "username": "your-email@company.com",
            "api_token": "your_jira_api_token",
        },
        "epics": ["PROJ-123", "PROJ-456", "PROJ-789"],
        "done_statuses": ["Done", "Closed", "Resolved", "Complete"],
        "output_directory": "./jira_analysis_output",
    }

    with open(config_path, "w") as file:
        yaml.dump(sample_config, file, default_flow_style=False, sort_keys=False)

    print(f"Sample configuration created at '{config_path}'")
    print("Please edit this file with your actual Jira credentials and epic keys.")
    print(
        "To create a Jira API token: https://id.atlassian.com/manage-profile/security/api-tokens"
    )


def read_epic_keys_from_file(file_path: str) -> List[str]:
    epic_keys = []

    with open(file_path, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            epic_key = row.get("epic_key", "").strip()
            if epic_key:
                epic_keys.append(epic_key)

    return epic_keys


def main():
    """
    Main function to run the epic analysis.
    """
    parser = argparse.ArgumentParser(
        description="Analyze a set of epics and their % completion"
    )
    parser.add_argument("config", type=str, help="path to config file")
    args = parser.parse_args()
    config_path = args.config

    # Check if config file exists, create sample if not
    if not os.path.exists(config_path):
        print(f"Configuration file '{config_path}' not found.")
        create_sample_config(config_path)
        print("\nPlease edit the configuration file and run the script again.")
        sys.exit(0)

    # Load and validate configuration
    config = load_config(config_path)
    if not validate_config(config):
        sys.exit(1)

    # Initialize analyzer
    epics_file = config.get("epic_keys_file")
    logging.info(f"Reading epic keys from file: {epics_file}")
    epic_keys = read_epic_keys_from_file(epics_file)
    analyzer = JiraEpicAnalyzer(config, epic_keys)

    # Test connection
    if not analyzer.test_connection():
        print("Failed to connect to Jira. Please check your configuration.")
        sys.exit(1)

    # Analyze epics
    print(f"\nAnalyzing {len(config['epics'])} epics...")
    analyses = analyzer.analyze_all_epics()

    # Save results
    summary_file = analyzer.save_epic_summary_csv(analyses)
    detailed_file = analyzer.save_detailed_csv(analyses)

    # Print summary
    analyzer.print_summary(analyses)

    print(f"\nFiles saved:")
    print(f"  Summary: {summary_file}")
    print(f"  Detailed: {detailed_file}")
    print("\nReady to import into Google Sheets for team review!")


if __name__ == "__main__":
    main()
