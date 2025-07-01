"""
Jira Kanban Board Scraper with Initiative Rollups
Scrapes multiple Jira Kanban boards, parses labels for initiatives, and creates project rollups.
Configuration driven by YAML file.
"""

import requests
import csv
import yaml
import base64
from datetime import datetime, timezone
import os
import sys
from typing import List, Dict, Any, Optional, Set
from pathlib import Path
import json
import time


class JiraKanbanScraper:
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
        self.boards = config.get("boards", [])
        self.output_directory = config["output_directory"]
        self.include_subtasks = config.get("include_subtasks", False)
        self.status_categories = config.get(
            "status_categories",
            {
                "To Do": ["To Do", "Backlog", "New", "Open"],
                "In Progress": [
                    "In Progress",
                    "In Development",
                    "Code Review",
                    "Testing",
                ],
                "Done": ["Done", "Closed", "Resolved", "Complete"],
            },
        )

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

    def get_board_info(self, board_id: int) -> Dict[str, Any]:
        """Get basic information about a board."""
        try:
            url = f"{self.base_url}/rest/agile/1.0/board/{board_id}"
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()

            board_data = response.json()
            return {
                "id": board_data.get("id"),
                "name": board_data.get("name", "Unknown"),
                "type": board_data.get("type", "Unknown"),
                "project_key": board_data.get("location", {}).get(
                    "projectKey", "Unknown"
                ),
                "project_name": board_data.get("location", {}).get(
                    "projectName", "Unknown"
                ),
            }
        except requests.exceptions.RequestException as e:
            print(f"Error fetching board info for board {board_id}: {e}")
            return {
                "id": board_id,
                "name": f"Board {board_id}",
                "type": "Unknown",
                "project_key": "Unknown",
                "project_name": "Unknown",
            }

    def get_board_columns(self, board_id: int) -> List[Dict[str, Any]]:
        """Get column configuration for a board."""
        try:
            url = f"{self.base_url}/rest/agile/1.0/board/{board_id}/configuration"
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()

            config_data = response.json()
            columns = []

            for column in config_data.get("columnConfig", {}).get("columns", []):
                column_info = {
                    "name": column.get("name", "Unknown"),
                    "statuses": [
                        status.get("name") for status in column.get("statuses", [])
                    ],
                }
                columns.append(column_info)

            return columns
        except requests.exceptions.RequestException as e:
            print(f"Warning: Could not fetch column config for board {board_id}: {e}")
            return []

    def get_board_issues(
        self, board_id: int, board_info: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Get all issues from a specific board."""
        print(
            f"Fetching issues from board: {board_info['name']} ({board_info['project_key']})"
        )

        issues = []
        start_at = 0
        max_results = 100

        while True:
            try:
                url = f"{self.base_url}/rest/agile/1.0/board/{board_id}/issue"
                params = {
                    "startAt": start_at,
                    "maxResults": max_results,
                    "fields": "summary,status,issuetype,assignee,reporter,priority,created,updated,resolutiondate,labels,components,fixVersions,customfield_10016,parent",  # customfield_10016 is often story points
                    "expand": "changelog",
                }

                response = requests.get(
                    url, headers=self.headers, params=params, timeout=30
                )
                response.raise_for_status()

                data = response.json()
                batch_issues = data.get("issues", [])

                if not batch_issues:
                    break

                # Add board context to each issue
                for issue in batch_issues:
                    issue["board_info"] = board_info

                issues.extend(batch_issues)

                if len(batch_issues) < max_results:
                    break

                start_at += max_results
                print(f"  Fetched {len(issues)} issues so far...")

            except requests.exceptions.RequestException as e:
                print(f"  Error fetching issues for board {board_id}: {e}")
                break

        print(f"  Total issues fetched: {len(issues)}")
        return issues

    def get_epic_children(self, epic_key: str) -> List[Dict[str, Any]]:
        """Get all children issues for an epic."""
        try:
            # Use JQL to find all issues with this epic as parent
            jql = f'"Epic Link" = {epic_key} OR parent = {epic_key}'
            url = f"{self.base_url}/rest/api/3/search"
            params = {
                "jql": jql,
                "fields": "key,summary,status,issuetype",
                "maxResults": 1000,
            }
            
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            return data.get("issues", [])
            
        except requests.exceptions.RequestException as e:
            print(f"Warning: Could not fetch children for epic {epic_key}: {e}")
            return []

    def categorize_status(self, status_name: str) -> str:
        """Categorize a status into To Do, In Progress, or Done."""
        for category, statuses in self.status_categories.items():
            if status_name in statuses:
                return category
        
        # Default categorization based on common patterns
        status_lower = status_name.lower()
        if any(word in status_lower for word in ["done", "closed", "resolved", "complete"]):
            return "Done"
        elif any(word in status_lower for word in ["progress", "review", "development", "testing"]):
            return "In Progress"
        else:
            return "To Do"

    def parse_labels(self, labels: List[str]) -> Dict[str, str]:
        """Parse Jira labels into structured categories."""
        parsed_labels = {
            "initiative": "",
            "team": "",
            "phase": "",
            "priority": "",
            "target": "",
            "dependency": "",
            "impact": "",
            "other_labels": [],
        }

        for label in labels:
            if ":" in label:
                category, value = label.split(":", 1)
                if category in parsed_labels:
                    # For categories that can have multiple values, combine them
                    if parsed_labels[category]:
                        parsed_labels[category] += f", {value}"
                    else:
                        parsed_labels[category] = value
                else:
                    parsed_labels["other_labels"].append(label)
            else:
                parsed_labels["other_labels"].append(label)

        # Convert other_labels list to comma-separated string
        parsed_labels["other_labels"] = ", ".join(parsed_labels["other_labels"])

        return parsed_labels

    def _parse_jira_datetime(self, date_string: str) -> Optional[datetime]:
        """Parse Jira datetime string handling various formats."""
        if not date_string:
            return None

        try:
            # Handle different Jira datetime formats
            if date_string.endswith("Z"):
                # Format: 2025-06-18T12:50:16.624Z
                return datetime.fromisoformat(date_string.replace("Z", "+00:00"))
            elif "+" in date_string or date_string.count("-") > 2:
                # Format: 2025-06-18T12:50:16.624-0700 or 2025-06-18T12:50:16.624+0000
                # Remove milliseconds if present and normalize timezone
                if "." in date_string:
                    # Split at the dot to remove milliseconds
                    date_part, tz_part = date_string.split(".")
                    # Find where timezone starts (+ or - after the time part)
                    tz_start = (
                        -5
                        if date_string[-5:].replace("+", "").replace("-", "").isdigit()
                        else -4
                    )
                    tz = tz_part[tz_start:]
                    # Reconstruct without milliseconds
                    date_string = date_part + tz

                # Normalize timezone format (add colon if missing)
                if date_string[-5:].replace("+", "").replace("-", "").isdigit():
                    # Format: -0700 -> -07:00
                    tz = date_string[-5:]
                    date_string = date_string[:-5] + tz[:-2] + ":" + tz[-2:]

                return datetime.fromisoformat(date_string)
            else:
                # Try standard ISO format
                return datetime.fromisoformat(date_string)

        except Exception as e:
            print(f"Warning: Could not parse datetime '{date_string}': {e}")
            return None

    def calculate_days_in_status(self, issue: Dict[str, Any]) -> Optional[float]:
        """Calculate how many days the issue has been in current status."""
        try:
            changelog = issue.get("changelog", {}).get("histories", [])
            current_status = issue["fields"]["status"]["name"]
            current_time = datetime.now()

            # Find the most recent status change to current status
            last_status_change = None
            for history in reversed(changelog):  # Start from most recent
                for item in history.get("items", []):
                    if (
                        item.get("field") == "status"
                        and item.get("toString") == current_status
                    ):
                        last_status_change = self._parse_jira_datetime(
                            history["created"]
                        )
                        break
                if last_status_change:
                    break

            if not last_status_change:
                # If no status change found, use creation date
                last_status_change = self._parse_jira_datetime(
                    issue["fields"]["created"]
                )

            if not last_status_change:
                return None

            # Make sure current_time is timezone-aware to match last_status_change
            if last_status_change.tzinfo is not None and current_time.tzinfo is None:
                # Make current_time timezone-aware (assume local timezone)
                from datetime import timezone
                current_time = current_time.replace(tzinfo=timezone.utc)
            elif last_status_change.tzinfo is None and current_time.tzinfo is not None:
                # Make last_status_change timezone-naive to match current_time
                last_status_change = last_status_change.replace(tzinfo=None)

            days_in_status = (current_time - last_status_change).total_seconds() / (
                24 * 3600
            )
            return round(days_in_status, 1)

        except Exception as e:
            print(
                f"Warning: Could not calculate days in status for {issue['key']}: {e}"
            )
            return None

    def parse_issue(self, issue: Dict[str, Any]) -> Dict[str, Any]:
        """Parse issue data into standardized format."""
        fields = issue["fields"]
        board_info = issue["board_info"]

        # Parse labels into structured format
        labels = fields.get("labels", [])
        parsed_labels = self.parse_labels(labels)

        # Basic issue info
        parsed_issue = {
            "board_id": board_info["id"],
            "board_name": board_info["name"],
            "project_key": board_info["project_key"],
            "project_name": board_info["project_name"],
            "issue_key": issue["key"],
            "summary": fields.get("summary", ""),
            "issue_type": fields.get("issuetype", {}).get("name", "Unknown"),
            "status": fields.get("status", {}).get("name", "Unknown"),
            "status_category": self.categorize_status(
                fields.get("status", {}).get("name", "Unknown")
            ),
            "assignee": (
                fields.get("assignee", {}).get("displayName", "Unassigned")
                if fields.get("assignee")
                else "Unassigned"
            ),
            "reporter": (
                fields.get("reporter", {}).get("displayName", "Unknown")
                if fields.get("reporter")
                else "Unknown"
            ),
            "priority": (
                fields.get("priority", {}).get("name", "Unknown")
                if fields.get("priority")
                else "Unknown"
            ),
            "story_points": fields.get(
                "customfield_10016"
            ),  # Adjust field name as needed
            "labels": ", ".join(fields.get("labels", [])),
            "components": ", ".join(
                [comp["name"] for comp in fields.get("components", [])]
            ),
            "fix_versions": ", ".join(
                [version["name"] for version in fields.get("fixVersions", [])]
            ),
            "created": fields.get("created", "")[:10] if fields.get("created") else "",
            "updated": fields.get("updated", "")[:10] if fields.get("updated") else "",
            "resolution_date": (
                fields.get("resolutiondate", "")[:10]
                if fields.get("resolutiondate")
                else ""
            ),
            "parent_key": (
                fields.get("parent", {}).get("key", "") if fields.get("parent") else ""
            ),
            "days_in_current_status": self.calculate_days_in_status(issue),
            # Structured label data
            "initiative": parsed_labels["initiative"],
            "team_label": parsed_labels["team"],
            "phase": parsed_labels["phase"],
            "priority_label": parsed_labels["priority"],
            "target": parsed_labels["target"],
            "dependency": parsed_labels["dependency"],
            "impact": parsed_labels["impact"],
            "other_labels": parsed_labels["other_labels"],
        }

        return parsed_issue

    def calculate_epic_completion(self, epic_issue: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate completion percentage for an epic based on its children."""
        epic_key = epic_issue["issue_key"]

        # Get all children for this epic
        children = self.get_epic_children(epic_key)

        if not children:
            return {
                "total_children": 0,
                "done_children": 0,
                "completion_percentage": 0,
                "child_status_breakdown": {},
            }

        # Analyze children status
        done_count = 0
        status_breakdown = {}

        done_statuses = self.status_categories.get(
            "Done", ["Done", "Closed", "Resolved"]
        )

        for child in children:
            child_status = child["fields"]["status"]["name"]
            status_breakdown[child_status] = status_breakdown.get(child_status, 0) + 1

            if child_status in done_statuses:
                done_count += 1

        total_children = len(children)
        completion_percentage = (
            (done_count / total_children * 100) if total_children > 0 else 0
        )

        return {
            "total_children": total_children,
            "done_children": done_count,
            "completion_percentage": round(completion_percentage, 1),
            "child_status_breakdown": status_breakdown,
        }

    def create_initiative_rollups(self, issues: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Create initiative-based project rollups with completion percentages."""
        print("Creating initiative-based project rollups...")

        # Separate epics from other issues
        epics = [
            issue
            for issue in issues
            if issue["issue_type"] == "Epic" and issue["initiative"]
        ]
        other_issues = [
            issue
            for issue in issues
            if issue["issue_type"] != "Epic" or not issue["initiative"]
        ]

        print(f"Found {len(epics)} labeled epics for rollup analysis")

        # Calculate completion for each epic
        epics_with_completion = []
        for epic in epics:
            completion_data = self.calculate_epic_completion(epic)
            epic_with_completion = {**epic, **completion_data}
            epics_with_completion.append(epic_with_completion)

        # Group epics by initiative
        initiatives = {}
        for epic in epics_with_completion:
            initiative = epic["initiative"]
            if initiative not in initiatives:
                initiatives[initiative] = {
                    "epics": [],
                    "teams": set(),
                    "phases": set(),
                    "priorities": set(),
                    "total_epics": 0,
                    "total_children": 0,
                    "total_done_children": 0,
                    "completion_percentage": 0,
                    "epics_completed": 0,
                    "oldest_epic_days": 0,
                    "dependencies": set(),
                }

            initiative_data = initiatives[initiative]
            initiative_data["epics"].append(epic)
            initiative_data["total_epics"] += 1
            initiative_data["total_children"] += epic["total_children"]
            initiative_data["total_done_children"] += epic["done_children"]

            # Collect metadata
            if epic["team_label"]:
                initiative_data["teams"].add(epic["team_label"])
            if epic["phase"]:
                initiative_data["phases"].add(epic["phase"])
            if epic["priority_label"]:
                initiative_data["priorities"].add(epic["priority_label"])
            if epic["dependency"]:
                initiative_data["dependencies"].add(epic["dependency"])

            # Track completed epics (100% done)
            if epic["completion_percentage"] == 100:
                initiative_data["epics_completed"] += 1

            # Track oldest epic
            if (
                epic["days_in_current_status"]
                and epic["days_in_current_status"] > initiative_data["oldest_epic_days"]
            ):
                initiative_data["oldest_epic_days"] = epic["days_in_current_status"]

        # Calculate overall completion percentages
        for initiative, data in initiatives.items():
            if data["total_children"] > 0:
                data["completion_percentage"] = round(
                    (data["total_done_children"] / data["total_children"]) * 100, 1
                )

            # Convert sets to comma-separated strings
            data["teams"] = ", ".join(sorted(data["teams"]))
            data["phases"] = ", ".join(sorted(data["phases"]))
            data["priorities"] = ", ".join(sorted(data["priorities"]))
            data["dependencies"] = ", ".join(sorted(data["dependencies"]))

        return {
            "initiatives": initiatives,
            "labeled_epics": epics_with_completion,
            "other_issues": other_issues,
            "total_initiatives": len(initiatives),
        }

    def scrape_all_boards(self) -> List[Dict[str, Any]]:
        """Scrape all configured boards and return consolidated issue list."""
        all_issues = []

        print(f"Scraping {len(self.boards)} Kanban boards...")

        for board_config in self.boards:
            board_id = board_config["board_id"]
            expected_project = board_config.get("project_key", "Unknown")

            print(
                f"\n--- Processing Board {board_id} (Expected: {expected_project}) ---"
            )

            # Get board information
            board_info = self.get_board_info(board_id)

            # Validate project key if specified
            if (
                expected_project != "Unknown"
                and board_info["project_key"] != expected_project
            ):
                print(
                    f"Warning: Board {board_id} project key '{board_info['project_key']}' doesn't match expected '{expected_project}'"
                )

            # Get board issues
            board_issues = self.get_board_issues(board_id, board_info)

            # Parse issues
            for issue in board_issues:
                # Skip subtasks if not requested
                if not self.include_subtasks and issue["fields"].get(
                    "issuetype", {}
                ).get("subtask", False):
                    continue

                parsed_issue = self.parse_issue(issue)
                all_issues.append(parsed_issue)

        print(f"\nTotal issues scraped across all boards: {len(all_issues)}")
        return all_issues

    def save_to_csv(self, issues: List[Dict[str, Any]], filename: str = None):
        """Save consolidated board data to CSV."""
        if not issues:
            print("No issues to save")
            return

        # Create output directory
        Path(self.output_directory).mkdir(parents=True, exist_ok=True)

        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"kanban_boards_snapshot_{timestamp}.csv"

        filepath = Path(self.output_directory) / filename

        # Enhanced CSV fieldnames with label data
        fieldnames = [
            "board_id",
            "board_name",
            "project_key",
            "project_name",
            "issue_key",
            "summary",
            "issue_type",
            "status",
            "status_category",
            "assignee",
            "reporter",
            "priority",
            "story_points",
            "labels",
            "components",
            "fix_versions",
            "created",
            "updated",
            "resolution_date",
            "parent_key",
            "days_in_current_status",
            # Structured label columns
            "initiative",
            "team_label",
            "phase",
            "priority_label",
            "target",
            "dependency",
            "impact",
            "other_labels",
        ]

        with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(issues)

        print(f"Board data saved to {filepath}")
        return str(filepath)

    def save_initiative_rollups_csv(
        self, rollup_data: Dict[str, Any], filename: str = None
    ):
        """Save initiative rollups to CSV."""
        if not rollup_data["initiatives"]:
            print("No initiatives found to save")
            return

        # Create output directory
        Path(self.output_directory).mkdir(parents=True, exist_ok=True)

        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"initiative_rollups_{timestamp}.csv"

        filepath = Path(self.output_directory) / filename

        # Initiative rollup fieldnames
        fieldnames = [
            "initiative",
            "total_epics",
            "epics_completed",
            "total_children",
            "total_done_children",
            "completion_percentage",
            "teams",
            "phases",
            "priorities",
            "dependencies",
            "oldest_epic_days",
        ]

        with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for initiative, data in rollup_data["initiatives"].items():
                writer.writerow(
                    {
                        "initiative": initiative,
                        "total_epics": data["total_epics"],
                        "epics_completed": data["epics_completed"],
                        "total_children": data["total_children"],
                        "total_done_children": data["total_done_children"],
                        "completion_percentage": data["completion_percentage"],
                        "teams": data["teams"],
                        "phases": data["phases"],
                        "priorities": data["priorities"],
                        "dependencies": data["dependencies"],
                        "oldest_epic_days": data["oldest_epic_days"],
                    }
                )

        print(f"Initiative rollups saved to {filepath}")
        return str(filepath)

    def save_epic_details_csv(self, rollup_data: Dict[str, Any], filename: str = None):
        """Save detailed epic completion data to CSV."""
        if not rollup_data["labeled_epics"]:
            print("No labeled epics found to save")
            return

        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"epic_completion_details_{timestamp}.csv"

        filepath = Path(self.output_directory) / filename

        # Epic details fieldnames
        fieldnames = [
            "initiative",
            "epic_key",
            "summary",
            "phase",
            "team_label",
            "priority_label",
            "total_children",
            "done_children",
            "completion_percentage",
            "status",
            "assignee",
            "days_in_current_status",
            "dependency",
            "target",
        ]

        with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for epic in rollup_data["labeled_epics"]:
                writer.writerow(
                    {
                        "initiative": epic["initiative"],
                        "epic_key": epic["issue_key"],
                        "summary": epic["summary"],
                        "phase": epic["phase"],
                        "team_label": epic["team_label"],
                        "priority_label": epic["priority_label"],
                        "total_children": epic["total_children"],
                        "done_children": epic["done_children"],
                        "completion_percentage": epic["completion_percentage"],
                        "status": epic["status"],
                        "assignee": epic["assignee"],
                        "days_in_current_status": epic["days_in_current_status"],
                        "dependency": epic["dependency"],
                        "target": epic["target"],
                    }
                )

        print(f"Epic completion details saved to {filepath}")
        return str(filepath)

    def print_summary(self, issues: List[Dict[str, Any]], rollup_data: Dict[str, Any]):
        """Print summary of scraped board data."""
        if not issues:
            print("No issues found")
            return

        print("\n" + "=" * 80)
        print("KANBAN BOARDS SUMMARY")
        print("=" * 80)

        # Overall stats
        total_issues = len(issues)
        boards_scraped = len(set(issue["board_id"] for issue in issues))
        projects_covered = len(set(issue["project_key"] for issue in issues))

        print(f"Total Issues: {total_issues}")
        print(f"Boards Scraped: {boards_scraped}")
        print(f"Projects Covered: {projects_covered}")

        # Status category breakdown
        status_breakdown = {}
        for issue in issues:
            category = issue["status_category"]
            status_breakdown[category] = status_breakdown.get(category, 0) + 1

        print(f"\nStatus Category Breakdown:")
        for category, count in sorted(status_breakdown.items()):
            percentage = (count / total_issues) * 100
            print(f"  {category}: {count} issues ({percentage:.1f}%)")

        # Board breakdown
        board_breakdown = {}
        for issue in issues:
            board_key = f"{issue['board_name']} ({issue['project_key']})"
            if board_key not in board_breakdown:
                board_breakdown[board_key] = {"total": 0, "in_progress": 0}
            board_breakdown[board_key]["total"] += 1
            if issue["status_category"] == "In Progress":
                board_breakdown[board_key]["in_progress"] += 1

        print(f"\nPer-Board Breakdown:")
        print(
            f"{'Board (Project)':<40} {'Total':<8} {'In Progress':<12} {'% Active':<10}"
        )
        print("-" * 70)
        for board, stats in sorted(board_breakdown.items()):
            total = stats["total"]
            in_progress = stats["in_progress"]
            pct_active = (in_progress / total * 100) if total > 0 else 0
            print(f"{board:<40} {total:<8} {in_progress:<12} {pct_active:.1f}%")

        # Initiative Summary
        if rollup_data["initiatives"]:
            print(f"\nInitiative Summary:")
            print(f"Total Initiatives: {rollup_data['total_initiatives']}")
            print(f"Labeled Epics: {len(rollup_data['labeled_epics'])}")
            
            print(f"\n{'Initiative':<30} {'Epics':<8} {'Complete %':<12} {'Teams':<20}")
            print("-" * 70)
            for initiative, data in sorted(rollup_data["initiatives"].items()):
                print(f"{initiative[:29]:<30} {data['total_epics']:<8} {data['completion_percentage']:<12.1f} {data['teams'][:19]:<20}")

        # Issues needing attention (stale)
        stale_issues = [
            issue
            for issue in issues
            if issue["status_category"] == "In Progress"
            and issue["days_in_current_status"]
            and issue["days_in_current_status"] > 7
        ]

        if stale_issues:
            print(f"\nStale Issues (In Progress > 7 days): {len(stale_issues)}")
            print("Top 5 oldest:")
            stale_sorted = sorted(
                stale_issues, key=lambda x: x["days_in_current_status"], reverse=True
            )
            for issue in stale_sorted[:5]:
                days = issue["days_in_current_status"]
                key = issue["issue_key"]
                status = issue["status"]
                assignee = issue["assignee"]
                print(
                    f"  {key}: {days:.0f} days in '{status}' (assigned to {assignee})"
                )


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
    required_keys = ["jira", "boards", "output_directory"]
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

    # Check boards config
    if not isinstance(config["boards"], list) or not config["boards"]:
        print("Error: 'boards' must be a non-empty list")
        return False

    for i, board in enumerate(config["boards"]):
        if not isinstance(board, dict) or "board_id" not in board:
            print(f"Error: Board {i+1} must have 'board_id'")
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
        "boards": [
            {
                "board_id": 49,
                "project_key": "FED",  # Expected project (optional, for validation)
                "description": "Frontend Team Board",
            },
            {
                "board_id": 52,
                "project_key": "BACKEND",
                "description": "Backend Team Board",
            },
            {
                "board_id": 45,
                "project_key": "MOBILE",
                "description": "Mobile Team Board",
            },
        ],
        "include_subtasks": False,  # Whether to include subtasks in the output
        "status_categories": {
            "To Do": ["To Do", "Backlog", "New", "Open", "Ready for Development"],
            "In Progress": [
                "In Progress",
                "In Development",
                "Code Review",
                "Testing",
                "QA",
            ],
            "Done": ["Done", "Closed", "Resolved", "Complete", "Released"],
        },
        "output_directory": "./kanban_snapshots",
    }

    with open(config_path, "w") as file:
        yaml.dump(sample_config, file, default_flow_style=False, sort_keys=False)

    print(f"Sample configuration created at '{config_path}'")
    print("Please edit this file with your actual Jira credentials and board IDs.")
    print("To find board IDs, check the URL when viewing a board in Jira.")


def main():
    """Main function."""
    config_path = "/Users/michael@jaris.io/bin/kanban_scraper_config.yaml"

    if not os.path.exists(config_path):
        print(f"Configuration file not found. Creating sample...")
        create_sample_config(config_path)
        return

    config = load_config(config_path)
    if not validate_config(config):
        sys.exit(1)

    # Initialize scraper
    scraper = JiraKanbanScraper(config)

    # Test connection
    if not scraper.test_connection():
        print("Failed to connect to Jira. Please check your configuration.")
        sys.exit(1)

    # Scrape all boards
    print(f"\nStarting board scraping...")
    issues = scraper.scrape_all_boards()

    # Create initiative rollups
    rollup_data = scraper.create_initiative_rollups(issues)

    # Save results
    output_file = scraper.save_to_csv(issues)
    initiative_file = scraper.save_initiative_rollups_csv(rollup_data)
    epic_details_file = scraper.save_epic_details_csv(rollup_data)

    # Print summary
    scraper.print_summary(issues, rollup_data)

    print(f"\nFiles generated:")
    print(f"  All Issues: {output_file}")
    print(f"  Initiative Rollups: {initiative_file}")
    print(f"  Epic Details: {epic_details_file}")
    print("Ready to import into Google Sheets or your analytics tool!")


if __name__ == "__main__":
    main()
