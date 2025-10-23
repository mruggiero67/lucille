"""
Jira Sprint Report Generator
Generates detailed CSV reports for sprint velocity analysis
"""

import csv
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional
import requests
import yaml
from requests.auth import HTTPBasicAuth

# Handle both direct script execution and module import
try:
    from .utils import fetch_all_issues
except ImportError:
    # Add parent directory to path for direct script execution
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from lucille.jira.utils import fetch_all_issues


class JiraSprintReporter:
    def __init__(self, config_path: str = "config.yaml"):
        """Initialize the Jira Sprint Reporter with configuration."""
        self.config = self._load_config(config_path)
        self.session = self._setup_session()
        self._setup_logging()

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, "r") as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file {config_path} not found")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML configuration: {e}")

    def _setup_session(self) -> requests.Session:
        """Setup authenticated requests session."""
        session = requests.Session()
        jira_config = self.config["jira"]
        session.auth = HTTPBasicAuth(jira_config["username"], jira_config["api_token"])
        session.headers.update(
            {"Accept": "application/json", "Content-Type": "application/json"}
        )
        return session

    def _setup_logging(self):
        """Configure logging."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("jira_sprint_report.log"),
                logging.StreamHandler(sys.stdout),
            ],
        )
        self.logger = logging.getLogger(__name__)

    def _make_jira_request(
        self, endpoint: str, params: Optional[Dict] = None, api_type: str = "api"
    ) -> Dict:
        """Make authenticated request to Jira API."""
        base_url = self.config["jira"]["base_url"].rstrip("/")

        if api_type == "agile":
            url = f"{base_url}/rest/agile/1.0/{endpoint}"
        else:
            url = f"{base_url}/rest/api/3/{endpoint}"

        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Jira API request failed: {e}")
            raise

    def get_sprint_info(self, board_id: str, sprint_id: str) -> Dict:
        """Get sprint information."""
        self.logger.info(f"Fetching sprint {sprint_id} info from board {board_id}")

        # Get sprint details using Agile API
        sprint_data = self._make_jira_request(f"sprint/{sprint_id}", api_type="agile")

        # Get board info using Agile API
        board_data = self._make_jira_request(f"board/{board_id}", api_type="agile")

        return {"sprint": sprint_data, "board": board_data}

    def get_sprint_issues(self, board_id: str, sprint_id: str) -> List[Dict]:
        """Get all issues in the sprint with detailed information."""
        self.logger.info(f"Fetching issues for sprint {sprint_id}")

        # JQL to get all issues that were in the sprint at any point
        jql = f"Sprint = {sprint_id}"
        fields = [
            "key",
            "summary",
            "status",
            "assignee",
            "reporter",
            "created",
            "updated",
            "resolutiondate",
            "issuetype",
            "priority",
            "labels",
            "components",
            "fixVersions",
            "customfield_10016",  # Story Points
            "customfield_10020",  # Sprint field
            "parent",
            "subtasks",
            "description",
            "resolution",
        ]
        params = {
            "jql": jql,
            "maxResults": 1000,
            "fields": fields,
            "expand": ["changelog"],
        }

        all_issues = fetch_all_issues(self.session,
                                      self.config["jira"]["base_url"],
                                      jql,
                                      fields,
                                      expand=["changelog"])
#        start_at = 0
#
#        while True:
#            params["startAt"] = start_at
#            response = self._make_jira_request("search", params)
#
#            issues = response.get("issues", [])
#            all_issues.extend(issues)
#
#            if len(issues) < params["maxResults"]:
#                break
#            start_at += params["maxResults"]
#
#        self.logger.info(f"Retrieved {len(all_issues)} issues from sprint")
        return all_issues

    def _parse_sprint_field(self, sprint_field_value) -> List[Dict]:
        """Parse the sprint custom field to extract sprint information."""
        if not sprint_field_value:
            return []

        sprints = []
        for sprint_str in sprint_field_value:
            # Parse sprint string format: com.atlassian.greenhopper.service.sprint.Sprint@[hash][id=X,rapidViewId=Y,state=ACTIVE,name=Sprint Name,startDate=...,endDate=...]
            if "id=" in sprint_str:
                sprint_info = {}
                parts = sprint_str.split("[")[1].split("]")[0].split(",")
                for part in parts:
                    if "=" in part:
                        key, value = part.split("=", 1)
                        sprint_info[key] = value
                sprints.append(sprint_info)

        return sprints

    def _get_issue_sprint_history(self, issue: Dict, target_sprint_id: str) -> Dict:
        """Analyze issue's sprint history to determine when it was added/removed."""
        changelog = issue.get("changelog", {}).get("histories", [])
        sprint_events = []

        for history in changelog:
            for item in history.get("items", []):
                if item.get("field") == "Sprint":
                    sprint_events.append(
                        {
                            "date": history["created"],
                            "author": history["author"]["displayName"],
                            "from_sprints": (
                                self._parse_sprint_field([item.get("fromString", "")])
                                if item.get("fromString")
                                else []
                            ),
                            "to_sprints": (
                                self._parse_sprint_field([item.get("toString", "")])
                                if item.get("toString")
                                else []
                            ),
                        }
                    )

        # Determine if issue was added after sprint start
        sprint_field = issue["fields"].get("customfield_10020", [])
        current_sprints = self._parse_sprint_field(sprint_field)

        is_in_target_sprint = any(
            s.get("id") == target_sprint_id for s in current_sprints
        )
        added_after_start = False

        # Check if issue was added to sprint after creation
        for event in sprint_events:
            if any(s.get("id") == target_sprint_id for s in event["to_sprints"]):
                # Issue was added to the target sprint at some point
                added_after_start = (
                    True  # Simplified - you might want more sophisticated logic
                )
                break

        return {
            "is_in_sprint": is_in_target_sprint,
            "added_after_start": added_after_start,
            "sprint_events": sprint_events,
        }

    def generate_detailed_csv(
        self, issues: List[Dict], sprint_info: Dict, output_dir: str, sprint_id: str
    ):
        """Generate detailed CSV with all issue information."""
        output_path = Path(output_dir) / f"sprint_{sprint_id}_detailed_issues.csv"

        self.logger.info(f"Generating detailed CSV: {output_path}")

        with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "key",
                "summary",
                "issue_type",
                "status",
                "resolution",
                "assignee",
                "reporter",
                "priority",
                "story_points",
                "created",
                "updated",
                "resolved",
                "labels",
                "components",
                "in_sprint",
                "added_after_start",
                "completed",
                "epic_link",
            ]

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for issue in issues:
                fields = issue["fields"]
                sprint_history = self._get_issue_sprint_history(issue, sprint_id)

                # Determine if issue is completed
                status_category = (
                    fields.get("status", {}).get("statusCategory", {}).get("key", "")
                )
                completed = status_category == "done"

                row = {
                    "key": issue["key"],
                    "summary": fields.get("summary", ""),
                    "issue_type": fields.get("issuetype", {}).get("name", ""),
                    "status": fields.get("status", {}).get("name", ""),
                    "resolution": (
                        fields.get("resolution", {}).get("name", "")
                        if fields.get("resolution")
                        else ""
                    ),
                    "assignee": (
                        fields.get("assignee", {}).get("displayName", "")
                        if fields.get("assignee")
                        else "Unassigned"
                    ),
                    "reporter": fields.get("reporter", {}).get("displayName", ""),
                    "priority": fields.get("priority", {}).get("name", ""),
                    "story_points": fields.get("customfield_10016", ""),
                    "created": fields.get("created", ""),
                    "updated": fields.get("updated", ""),
                    "resolved": fields.get("resolutiondate", ""),
                    "labels": ",".join(fields.get("labels", [])),
                    "components": ",".join(
                        [c["name"] for c in fields.get("components", [])]
                    ),
                    "in_sprint": sprint_history["is_in_sprint"],
                    "added_after_start": sprint_history["added_after_start"],
                    "completed": completed,
                    "epic_link": (
                        fields.get("parent", {}).get("key", "")
                        if fields.get("parent")
                        else ""
                    ),
                }

                writer.writerow(row)

    def generate_summary_csv(
        self, issues: List[Dict], sprint_info: Dict, output_dir: str, sprint_id: str
    ):
        """Generate summary CSV with key metrics."""
        output_path = Path(output_dir) / f"sprint_{sprint_id}_summary.csv"

        self.logger.info(f"Generating summary CSV: {output_path}")

        # Calculate metrics
        total_issues = len(issues)
        completed_issues = 0
        not_completed_issues = 0
        added_after_start = 0
        total_story_points = 0
        completed_story_points = 0

        issue_types = {}
        assignees = {}

        for issue in issues:
            fields = issue["fields"]
            sprint_history = self._get_issue_sprint_history(issue, sprint_id)

            # Status analysis
            status_category = (
                fields.get("status", {}).get("statusCategory", {}).get("key", "")
            )
            is_completed = status_category == "done"

            if is_completed:
                completed_issues += 1
            else:
                not_completed_issues += 1

            if sprint_history["added_after_start"]:
                added_after_start += 1

            # Story points
            story_points = fields.get("customfield_10016", 0) or 0
            total_story_points += story_points
            if is_completed:
                completed_story_points += story_points

            # Issue types
            issue_type = fields.get("issuetype", {}).get("name", "Unknown")
            issue_types[issue_type] = issue_types.get(issue_type, 0) + 1

            # Assignees
            assignee = (
                fields.get("assignee", {}).get("displayName", "Unassigned")
                if fields.get("assignee")
                else "Unassigned"
            )
            assignees[assignee] = assignees.get(assignee, 0) + 1

        # Create summary data
        sprint_data = sprint_info["sprint"]
        summary_data = [
            ["Metric", "Value"],
            ["Sprint Name", sprint_data.get("name", "")],
            ["Sprint State", sprint_data.get("state", "")],
            ["Start Date", sprint_data.get("startDate", "")],
            ["End Date", sprint_data.get("endDate", "")],
            ["Total Issues", total_issues],
            ["Issues Completed", completed_issues],
            ["Issues Not Completed", not_completed_issues],
            ["Issues Added After Start", added_after_start],
            [
                "Completion Rate (%)",
                (
                    f"{(completed_issues/total_issues*100):.1f}"
                    if total_issues > 0
                    else "0"
                ),
            ],
            ["Total Story Points", total_story_points],
            ["Completed Story Points", completed_story_points],
            [
                "Story Points Completion Rate (%)",
                (
                    f"{(completed_story_points/total_story_points*100):.1f}"
                    if total_story_points > 0
                    else "0"
                ),
            ],
            ["", ""],  # Empty row
            ["Issue Types Breakdown", ""],
        ]

        for issue_type, count in sorted(issue_types.items()):
            summary_data.append([f"  {issue_type}", count])

        summary_data.extend([["", ""], ["Assignee Breakdown", ""]])  # Empty row

        for assignee, count in sorted(assignees.items()):
            summary_data.append([f"  {assignee}", count])

        with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(summary_data)

    def run_report(self):
        """Execute the complete sprint report generation."""
        config = self.config
        board_id = config["sprint_report"]["board_id"]
        sprint_id = config["sprint_report"]["sprint_id"]
        output_dir = config["sprint_report"]["csv_directory"]

        self.logger.info(
            f"Starting sprint report for board {board_id}, sprint {sprint_id}"
        )

        # Ensure output directory exists
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        try:
            # Get sprint information
            sprint_info = self.get_sprint_info(board_id, sprint_id)

            # Get sprint issues
            issues = self.get_sprint_issues(board_id, sprint_id)

            # Generate reports
            self.generate_detailed_csv(issues, sprint_info, output_dir, sprint_id)
            self.generate_summary_csv(issues, sprint_info, output_dir, sprint_id)

            self.logger.info(f"Sprint report generation completed successfully")
            self.logger.info(f"Reports saved to: {output_dir}")

        except Exception as e:
            self.logger.error(f"Error generating sprint report: {e}")
            raise


def main():
    """Main entry point."""
    try:
        config_path = "/Users/michael@jaris.io/bin/jira_epic_config.yaml"
        reporter = JiraSprintReporter(config_path)
        reporter.run_report()
    except Exception as e:
        logging.error(f"Failed to generate sprint report: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
