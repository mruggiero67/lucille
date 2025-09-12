#!/usr/bin/env python3
"""
Jira Sprint Data Extractor

extracts stories and epics from active Jira sprints and exports them to CSV files.
Configuration driven by YAML file containing Jira credentials et al
"""

import csv
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional
import requests
import yaml
from requests.auth import HTTPBasicAuth
import argparse


def setup_logging(log_level: str = "INFO") -> None:
    """Set up logging configuration."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_config(config_path: str) -> Dict:
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


class JiraClient:
    """Jira API client for extracting sprint and issue data."""

    def __init__(self, board_ids: List[int], base_url: str, username: str, api_token: str):
        self.board_ids = board_ids
        self.base_url = base_url.rstrip("/")
        self.auth = HTTPBasicAuth(username, api_token)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update(
            {"Accept": "application/json", "Content-Type": "application/json"}
        )

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make a GET request to the Jira API."""
        url = f"{self.base_url}/rest/agile/1.0/{endpoint}"
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed for {endpoint}: {e}")
            raise

    def get_active_sprints(self) -> List[Dict]:
        """Get all active sprints across all boards."""
        logging.info("Fetching active sprints...")
        active_sprints = []

        # Get all boards
        boards_response = self._make_request("board", params={"type": "scrum"})
        boards = boards_response.get("values", [])

        for board in boards:
            if self.board_ids and board["id"] not in self.board_ids:
                continue
            board_id = board["id"]
            board_name = board["name"]
            logging.debug(f"Checking board: {board_name} (ID: {board_id})")

            try:
                # Get active sprints for this board
                sprints_response = self._make_request(
                    f"board/{board_id}/sprint", params={"state": "active"}
                )
                sprints = sprints_response.get("values", [])

                for sprint in sprints:
                    sprint["board_name"] = board_name
                    sprint["board_id"] = board_id
                    active_sprints.append(sprint)
                    logging.info(
                        f"Found active sprint: {sprint['name']} in board {board_name}"
                    )

            except requests.exceptions.RequestException:
                logging.warning(f"Could not fetch sprints for board {board_name}")
                continue

        logging.info(f"Found {len(active_sprints)} active sprints total")
        return active_sprints

    def get_sprint_issues(self, sprint_id: int) -> List[Dict]:
        """Get all issues in a specific sprint."""
        logging.debug(f"Fetching issues for sprint ID: {sprint_id}")
        issues = []
        start_at = 0
        max_results = 50

        while True:
            response = self._make_request(
                f"sprint/{sprint_id}/issue",
                params={
                    "startAt": start_at,
                    "maxResults": max_results,
                    "fields": "key,summary,status,issuetype,project,parent,assignee,created,updated,priority,components",
                },
            )

            batch_issues = response.get("issues", [])
            issues.extend(batch_issues)

            if len(batch_issues) < max_results:
                break
            start_at += max_results

        logging.debug(f"Retrieved {len(issues)} issues from sprint {sprint_id}")
        return issues

    def get_epic_details(self, epic_key: str) -> Optional[Dict]:
        """Get detailed information about an epic."""
        try:
            response = self._make_request(
                f"issue/{epic_key}",
                params={
                    "fields": "key,summary,status,project,assignee,created,updated,priority,components"
                },
            )
            return response
        except requests.exceptions.RequestException:
            logging.warning(f"Could not fetch epic details for {epic_key}")
            return None


def extract_story_data(issue: Dict, sprint_info: Dict) -> Dict:
    """Extract relevant data from a story issue."""
    fields = issue.get("fields", {})

    return {
        "project_name": fields.get("project", {}).get("name", ""),
        "project_key": fields.get("project", {}).get("key", ""),
        "sprint_name": sprint_info.get("name", ""),
        "sprint_id": sprint_info.get("id", ""),
        "board_name": sprint_info.get("board_name", ""),
        "board_id": sprint_info.get("board_id", ""),
        "story_key": issue.get("key", ""),
        "story_summary": fields.get("summary", ""),
        "story_type": fields.get("issuetype", {}).get("name", ""),
        "story_status": fields.get("status", {}).get("name", ""),
        "story_priority": (
            fields.get("priority", {}).get("name", "") if fields.get("priority") else ""
        ),
        "assignee": (
            fields.get("assignee", {}).get("displayName", "")
            if fields.get("assignee")
            else "Unassigned"
        ),
        "parent_epic_key": (
            fields.get("parent", {}).get("key", "") if fields.get("parent") else ""
        ),
        "components": ", ".join(
            [comp["name"] for comp in fields.get("components", [])]
        ),
        "created": fields.get("created", ""),
        "updated": fields.get("updated", ""),
    }


def extract_epic_data(epic_issue: Dict) -> Dict:
    """Extract relevant data from an epic issue."""
    fields = epic_issue.get("fields", {})

    return {
        "project_name": fields.get("project", {}).get("name", ""),
        "project_key": fields.get("project", {}).get("key", ""),
        "epic_key": epic_issue.get("key", ""),
        "epic_summary": fields.get("summary", ""),
        "epic_status": fields.get("status", {}).get("name", ""),
        "epic_priority": (
            fields.get("priority", {}).get("name", "") if fields.get("priority") else ""
        ),
        "assignee": (
            fields.get("assignee", {}).get("displayName", "")
            if fields.get("assignee")
            else "Unassigned"
        ),
        "components": ", ".join(
            [comp["name"] for comp in fields.get("components", [])]
        ),
        "created": fields.get("created", ""),
        "updated": fields.get("updated", ""),
    }


def write_csv(data: List[Dict], filepath: str, fieldnames: List[str]) -> None:
    """Write data to CSV file."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

    logging.info(f"Written {len(data)} rows to {filepath}")


def main(config_path: str) -> None:
    """Main execution function."""
    # Set up logging
    setup_logging(log_level="DEBUG")

    # Load configuration
    config = load_config(config_path)

    # Initialize Jira client
    jira_config = config["jira"]
    board_ids = config["board_ids"]
    epic_keys_file = config.get("epic_keys_file")
    jira_client = JiraClient(
        board_ids=board_ids,
        base_url=jira_config["base_url"],
        username=jira_config["username"],
        api_token=jira_config["api_token"],
    )

    # Get active sprints
    active_sprints = jira_client.get_active_sprints()

    if not active_sprints:
        logging.warning("No active sprints found")
        return

    # Collect all stories and track unique epics
    all_stories = []
    epic_keys = set()

    for sprint in active_sprints:
        if not sprint.get("board_id") in board_ids:
            logging.warning(f"Sprint {sprint.get('name')} doesn't match desired boards, skipping")
            continue
        sprint_id = sprint["id"]
        sprint_name = sprint["name"]

        logging.info(f"Processing sprint: {sprint_name} (id: {sprint_id})")

        # Get issues in this sprint
        issues = jira_client.get_sprint_issues(sprint_id)

        for issue in issues:
            # Extract story data
            story_data = extract_story_data(issue, sprint)
            all_stories.append(story_data)

            # Track parent epic if it exists
            parent_epic = story_data["parent_epic_key"]
            if parent_epic:
                epic_keys.add(parent_epic)

    logging.info(
        f"Collected {len(all_stories)} stories from {len(active_sprints)} active sprints"
    )
    logging.info(f"Found {len(epic_keys)} unique parent epics")

    # Fetch epic details
    all_epics = []
    for epic_key in epic_keys:
        logging.debug(f"Fetching epic details for {epic_key}")
        epic_details = jira_client.get_epic_details(epic_key)
        if epic_details:
            epic_data = extract_epic_data(epic_details)
            all_epics.append(epic_data)

    # Generate timestamp for file naming
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = config["output_directory"]

    # Define CSV fieldnames
    story_fieldnames = [
        "project_name",
        "project_key",
        "sprint_name",
        "sprint_id",
        "board_name",
        "board_id",
        "story_key",
        "story_summary",
        "story_type",
        "story_status",
        "story_priority",
        "assignee",
        "parent_epic_key",
        "components",
        "created",
        "updated",
    ]

    epic_fieldnames = [
        "project_name",
        "project_key",
        "epic_key",
        "epic_summary",
        "epic_status",
        "epic_priority",
        "assignee",
        "components",
        "created",
        "updated",
    ]

    # Write CSV files
    stories_filepath = os.path.join(output_dir, f"jira_stories_{timestamp}.csv")
    epics_filepath = os.path.join(output_dir, f"jira_epics_{timestamp}.csv")

    write_csv(all_stories, stories_filepath, story_fieldnames)
    write_csv(all_epics, epics_filepath, epic_fieldnames)

    # and save the epics to a separate CSV file, used by the completion script
    save_epic_keys_to_csv(all_epics, epic_keys_file)

    logging.info("Data extraction completed successfully")
    logging.info(f"Stories CSV: {stories_filepath}")
    logging.info(f"Epics CSV: {epics_filepath}")


def save_epic_keys_to_csv(epics: List[Dict], epic_keys_file: str) -> None:
    """Save epic keys to CSV file."""
    try:
        # Sort keys for consistent output
        keys = [epic["epic_key"] for epic in epics if "epic_key" in epic]
        sorted_keys = sorted(set(keys))

        with open(epic_keys_file, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["epic_key"])  # Header

            for key in sorted_keys:
                writer.writerow([key])

        logging.info(
            f"Saved {len(sorted_keys)} unique epic keys to {epic_keys_file}"
        )

    except IOError as e:
        logging.error(f"Failed to write to output file {epic_keys_file}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape current active sprints and extract stories, epics"
    )
    parser.add_argument("config", type=str, help="path to config file")
    args = parser.parse_args()
    config_path = args.config
    main(config_path)
