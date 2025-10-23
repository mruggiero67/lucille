#!/usr/bin/env python3
"""
Jira Sprint Cycle Time Analysis Script

Extracts cycle time data for stories in a sprint, tracking transitions
from "Ready for Development" to "Done".
"""

import argparse
import csv
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from dateutil import parser as date_parser

import requests
import yaml

logging.basicConfig(
    format="%(levelname)-10s %(asctime)s %(filename)s %(lineno)d %(message)s",
    level=logging.DEBUG,
)

logger = logging.getLogger(__name__)


class JiraClient:
    """Client for interacting with Jira API."""

    def __init__(self, base_url: str, email: str, api_token: str):
        self.base_url = base_url.rstrip("/")
        self.auth = (email, api_token)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({"Accept": "application/json"})

    def get_sprint_issues(self, sprint_id: int) -> List[Dict]:
        """Fetch all issues in a sprint with pagination."""
        url = f"{self.base_url}/rest/agile/1.0/sprint/{sprint_id}/issue"
        all_issues = []
        start_at = 0
        max_results = 100

        while True:
            params = {
                "startAt": start_at,
                "maxResults": max_results,
                "fields": "summary,status,issuetype,created,resolutiondate,assignee",

            }

            logger.debug(f"Fetching issues from sprint {sprint_id}, startAt={start_at}")
            response = self.session.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            issues = data.get("issues", [])
            all_issues.extend(issues)

            total = data.get("total", 0)
            logger.info(f"Retrieved {len(all_issues)} of {total} issues")

            if start_at + max_results >= total:
                break

            start_at += max_results

        return all_issues

    def get_issue_changelog(self, issue_key: str) -> List[Dict]:
        """Fetch complete changelog for an issue with pagination."""
        url = f"{self.base_url}/rest/api/3/issue/{issue_key}/changelog"
        all_histories = []
        start_at = 0
        max_results = 100

        while True:
            params = {"startAt": start_at, "maxResults": max_results}

            logger.debug(f"Fetching changelog for {issue_key}, startAt={start_at}")
            response = self.session.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            histories = data.get("values", [])
            all_histories.extend(histories)

            total = data.get("total", 0)

            if start_at + max_results >= total:
                break

            start_at += max_results

        logger.debug(f"Retrieved {len(all_histories)} changelog entries for {issue_key}")
        return all_histories


class CycleTimeCalculator:
    """Calculate cycle time from status transitions."""

    STATES = [
        "Ready for Development",
        "In Progress",
        "Review",
        "Ready for Testing",
        "In Testing",
        "To Deploy",
        "Done",
    ]

    def __init__(self):
        self.start_state = "In Progress"
        self.end_state = "Done"

    def extract_status_transitions(self, changelog: List[Dict]) -> List[Dict]:
        """Extract status change events from changelog."""
        transitions = []

        for history in changelog:
            created = history.get("created")
            for item in history.get("items", []):
                if item.get("field") == "status":
                    transitions.append(
                        {
                            "timestamp": created,
                            "from_status": item.get("fromString"),
                            "to_status": item.get("toString"),
                        }
                    )

        # Sort by timestamp
        transitions.sort(key=lambda x: x["timestamp"])
        return transitions


    def calculate_cycle_time(
            self, transitions: List[Dict]
        ) -> Optional[Dict[str, any]]:
            """
            Calculate cycle time from start_state to end_state.

            Returns dict with start_time, end_time, and cycle_time_hours,
            or None if cycle not complete.
            """
            start_time = None
            end_time = None

            for transition in transitions:
                # Find first transition TO start_state
                if transition["to_status"] == self.start_state and start_time is None:
                    start_time = transition["timestamp"]
                    logger.debug(f"Found start: {start_time}")

                # Find first transition TO end_state after start
                if (
                    transition["to_status"] == self.end_state
                    and start_time is not None
                    and end_time is None
                ):
                    end_time = transition["timestamp"]
                    logger.debug(f"Found end: {end_time}")
                    break

            if start_time and end_time:
                start_dt = date_parser.parse(start_time)
                end_dt = date_parser.parse(end_time)
                cycle_time_hours = (end_dt - start_dt).total_seconds() / 3600

                return {
                    "start_time": start_time,
                    "end_time": end_time,
                    "cycle_time_hours": round(cycle_time_hours, 2),
                    "cycle_time_days": round(cycle_time_hours / 24, 2),
                }

            return None


def load_config(config_path: str) -> Dict:
    """Load configuration from YAML file."""
    logger.info(f"Loading configuration from {config_path}")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

#    required_fields = ["board_id", "sprint_id", "csv_directory", "jira_url", "email", "api_token"]
#    missing = [field for field in required_fields if field not in config]
#
#    if missing:
#        raise ValueError(f"Missing required config fields: {', '.join(missing)}")

    return config


def write_summary_csv(data: List[Dict], output_path: Path):
    """Write summary CSV with cycle time per story."""
    logger.info(f"Writing summary CSV to {output_path}")

    fieldnames = [
        "issue_key",
        "summary",
        "issue_type",
        "assignee",
        "status",
        "start_time",
        "end_time",
        "cycle_time_hours",
        "cycle_time_days",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    logger.info(f"Wrote {len(data)} rows to summary CSV")


def write_verbose_csv(data: List[Dict], output_path: Path):
    """Write verbose CSV with all transitions."""
    logger.info(f"Writing verbose CSV to {output_path}")

    if not data:
        logger.warning("No data to write to verbose CSV")
        return

    fieldnames = [
        "issue_key",
        "summary",
        "issue_type",
        "current_status",
        "transition_timestamp",
        "from_status",
        "to_status",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    logger.info(f"Wrote {len(data)} rows to verbose CSV")


def main():
    parser = argparse.ArgumentParser(
        description="Analyze Jira sprint cycle times"
    )
    parser.add_argument(
        "config", help="Path to YAML configuration file"
    )
    args = parser.parse_args()

    try:
        # Load configuration
        config = load_config(args.config)
        csv_dir = Path(config["sprint_report"]["csv_directory"])
        csv_dir.mkdir(parents=True, exist_ok=True)

        # Initialize clients
        client = JiraClient(
            base_url=config["jira"]["base_url"],
            email=config["jira"]["username"],
            api_token=config["jira"]["api_token"],
        )
        calculator = CycleTimeCalculator()

        # Fetch sprint issues
        sprint_id = config["sprint_report"]["sprint_id"]
        logger.info(f"Fetching issues for sprint {sprint_id}")
        issues = client.get_sprint_issues(sprint_id)
        logger.info(f"Found {len(issues)} issues in sprint")

        summary_data = []
        verbose_data = []

        # Process each issue
        for issue in issues:
            issue_key = issue["key"]
            fields = issue["fields"]
            summary = fields.get("summary", "")
            issue_type = fields.get("issuetype", {}).get("name", "")
            current_status = fields.get("status", {}).get("name", "")
            assignee = fields.get("assignee", {}).get("displayName", "") if fields.get("assignee") else "Unassigned"


            logger.info(f"Processing {issue_key}: {summary}")

            # Get changelog
            changelog = client.get_issue_changelog(issue_key)
            transitions = calculator.extract_status_transitions(changelog)

            # Add all transitions to verbose data
            for transition in transitions:
                verbose_data.append(
                    {
                        "issue_key": issue_key,
                        "summary": summary,
                        "issue_type": issue_type,
                        "current_status": current_status,
                        "transition_timestamp": transition["timestamp"],
                        "from_status": transition["from_status"],
                        "to_status": transition["to_status"],
                    }
                )

            # Calculate cycle time
            cycle_time = calculator.calculate_cycle_time(transitions)

            if cycle_time:
                logger.info(
                    f"{issue_key} cycle time: {cycle_time['cycle_time_hours']} hours "
                    f"({cycle_time['cycle_time_days']} days)"
                )
                summary_data.append(
                    {
                        "issue_key": issue_key,
                        "summary": summary,
                        "issue_type": issue_type,
                        "assignee": assignee,
                        "status": current_status,
                        "start_time": cycle_time["start_time"],
                        "end_time": cycle_time["end_time"],
                        "cycle_time_hours": cycle_time["cycle_time_hours"],
                        "cycle_time_days": cycle_time["cycle_time_days"],
                    }
                )
            else:
                logger.warning(
                    f"{issue_key} did not complete cycle "
                    f"(Ready for Development -> Done)"
                )

        # Write output files
        timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")
        summary_path = csv_dir / f"{timestamp}_sprint_{sprint_id}_summary.csv"
        verbose_path = csv_dir / f"{timestamp}_sprint_{sprint_id}_verbose.csv"

        write_summary_csv(summary_data, summary_path)
        write_verbose_csv(verbose_data, verbose_path)

        logger.info("Analysis complete")
        logger.info(f"Summary: {len(summary_data)} issues with complete cycle time")
        logger.info(f"Total issues processed: {len(issues)}")

    except Exception as e:
        logger.error(f"Error during execution: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
