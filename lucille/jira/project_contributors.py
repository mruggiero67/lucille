#!/usr/bin/env python3
"""
Jira Work Distribution Analyzer

analyzes work distribution in a Jira project by fetching completed stories
since January 1st, 2025, generating detailed and summary CSV reports.
"""

import requests
import csv
import yaml
import os
import argparse
from datetime import datetime
from typing import Dict, List, Any
import sys
from collections import defaultdict


class JiraAnalyzer:
    def __init__(self, jira_url: str, username: str, api_token: str):
        """
        Initialize the Jira analyzer.

        Args:
            jira_url: Base URL of your Jira instance (e.g., 'https://yourcompany.atlassian.net')
            username: Your Jira username/email
            api_token: Your Jira API token
        """
        self.jira_url = jira_url.rstrip("/")
        self.auth = (username, api_token)
        self.session = requests.Session()
        self.session.auth = self.auth

    def fetch_completed_stories(self, project_key: str = "SSJ") -> List[Dict[str, Any]]:
        """
        Fetch all completed stories from the specified project since Jan 1, 2025.

        Args:
            project_key: The Jira project key (default: "SSJ")

        Returns:
            List of story dictionaries
        """
        # JQL query for completed stories since Jan 1, 2025
        jql = f"""
        project = "{project_key}"
        AND type = Story
        AND status = Done
        AND resolved >= "2025-01-01"
        ORDER BY resolved DESC
        """

        stories = []
        start_at = 0
        max_results = 50  # Jira's typical page size

        while True:
            url = f"{self.jira_url}/rest/api/3/search/jql"
            params = {
                "jql": jql,
                "startAt": start_at,
                "maxResults": max_results,
                "fields": "key,summary,assignee,reporter,status,created,updated,resolved,priority,components,fixVersions,customfield_10016",  # customfield_10016 is typically story points
            }

            try:
                response = self.session.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                issues = data.get("issues", [])
                if not issues:
                    break

                stories.extend(issues)

                # Check if we've fetched all results
                if len(issues) < max_results:
                    break

                start_at += max_results

            except requests.exceptions.RequestException as e:
                print(f"Error fetching data from Jira: {e}")
                sys.exit(1)

        print(f"Fetched {len(stories)} completed stories from project {project_key}")
        return stories

    def extract_story_data(self, story: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract relevant data from a Jira story object.

        Args:
            story: Raw Jira story object

        Returns:
            Dictionary with extracted story data
        """
        fields = story.get("fields", {})

        # Extract assignee info
        assignee = fields.get("assignee")
        assignee_name = (
            assignee.get("displayName", "Unassigned") if assignee else "Unassigned"
        )
        assignee_email = assignee.get("emailAddress", "") if assignee else ""

        # Extract reporter info
        reporter = fields.get("reporter")
        reporter_name = (
            reporter.get("displayName", "Unknown") if reporter else "Unknown"
        )

        # Extract story points (may be in different custom fields depending on your setup)
        story_points = fields.get("customfield_10016")  # Common story points field
        if story_points is None:
            # Try other common story points field IDs
            for field_id in [
                "customfield_10002",
                "customfield_10004",
                "customfield_10008",
            ]:
                story_points = fields.get(field_id)
                if story_points is not None:
                    break

        # Extract components
        components = fields.get("components", [])
        component_names = [comp.get("name", "") for comp in components]

        # Extract fix versions
        fix_versions = fields.get("fixVersions", [])
        version_names = [ver.get("name", "") for ver in fix_versions]

        return {
            "key": story.get("key", ""),
            "summary": fields.get("summary", ""),
            "assignee": assignee_name,
            "assignee_email": assignee_email,
            "reporter": reporter_name,
            "status": fields.get("status", {}).get("name", ""),
            "priority": fields.get("priority", {}).get("name", ""),
            "created": self._format_date(fields.get("created")),
            "updated": self._format_date(fields.get("updated")),
            "resolved": self._format_date(fields.get("resolved")),
            "story_points": story_points if story_points else 0,
            "components": ", ".join(component_names),
            "fix_versions": ", ".join(version_names),
        }

    def _format_date(self, date_str: str) -> str:
        """Format Jira date string to readable format."""
        if not date_str:
            return ""
        try:
            # Jira dates are in ISO format
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return date_str

    def generate_detailed_csv(
        self,
        stories_data: List[Dict[str, Any]],
        filename: str = "jira_stories_detailed.csv",
    ):
        """
        Generate detailed CSV with all story information.

        Args:
            stories_data: List of processed story data
            filename: Output filename for detailed CSV
        """
        if not stories_data:
            print("No stories to write to CSV")
            return

        fieldnames = [
            "key",
            "summary",
            "assignee",
            "assignee_email",
            "reporter",
            "status",
            "priority",
            "created",
            "updated",
            "resolved",
            "story_points",
            "components",
            "fix_versions",
        ]

        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(stories_data)

        print(f"Detailed stories written to {filename}")

    def generate_summary_csv(
        self,
        stories_data: List[Dict[str, Any]],
        filename: str = "jira_work_distribution_summary.csv",
    ):
        """
        Generate summary CSV with work distribution by assignee.

        Args:
            stories_data: List of processed story data
            filename: Output filename for summary CSV
        """
        if not stories_data:
            print("No stories to summarize")
            return

        # Count stories and story points by assignee
        assignee_stats = defaultdict(
            lambda: {"story_count": 0, "total_story_points": 0, "stories": []}
        )

        for story in stories_data:
            assignee = story["assignee"]
            story_points = story["story_points"] or 0

            assignee_stats[assignee]["story_count"] += 1
            assignee_stats[assignee]["total_story_points"] += story_points
            assignee_stats[assignee]["stories"].append(story["key"])

        # Calculate percentages
        total_stories = len(stories_data)
        total_story_points = sum(story["story_points"] or 0 for story in stories_data)

        summary_data = []
        for assignee, stats in assignee_stats.items():
            story_percentage = (
                (stats["story_count"] / total_stories * 100) if total_stories > 0 else 0
            )
            points_percentage = (
                (stats["total_story_points"] / total_story_points * 100)
                if total_story_points > 0
                else 0
            )
            avg_points_per_story = (
                stats["total_story_points"] / stats["story_count"]
                if stats["story_count"] > 0
                else 0
            )

            summary_data.append(
                {
                    "assignee": assignee,
                    "story_count": stats["story_count"],
                    "story_percentage": round(story_percentage, 2),
                    "total_story_points": stats["total_story_points"],
                    "points_percentage": round(points_percentage, 2),
                    "avg_points_per_story": round(avg_points_per_story, 2),
                    "sample_stories": ", ".join(
                        stats["stories"][:5]
                    ),  # First 5 stories as sample
                }
            )

        # Sort by story count descending
        summary_data.sort(key=lambda x: x["story_count"], reverse=True)

        fieldnames = [
            "assignee",
            "story_count",
            "story_percentage",
            "total_story_points",
            "points_percentage",
            "avg_points_per_story",
            "sample_stories",
        ]

        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(summary_data)

        print(f"Work distribution summary written to {filename}")

        # Print summary to console
        print("\n=== WORK DISTRIBUTION SUMMARY ===")
        print(f"Total Stories Analyzed: {total_stories}")
        print(f"Total Story Points: {total_story_points}")
        print("\nTop Contributors:")
        for i, data in enumerate(summary_data[:10], 1):
            print(
                f"{i:2d}. {data['assignee']:25} | {data['story_count']:3d} stories ({data['story_percentage']:5.1f}%) | {data['total_story_points']:4.0f} points ({data['points_percentage']:5.1f}%)"
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
    required_keys = ["jira", "project"]
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

    # Check project config
    project_config = config["project"]
    if "key" not in project_config:
        print("Error: Missing required project configuration key 'key'")
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
        "project": {
            "key": "PROJECT",
            "analysis_start_date": "2025-01-01",
        },
        "output": {
            "detailed_csv": "jira_work_distribution_detailed.csv",
            "summary_csv": "jira_work_distribution_summary.csv",
        },
    }

    with open(config_path, "w") as file:
        yaml.dump(sample_config, file, default_flow_style=False, sort_keys=False)

    print(f"Sample configuration created at '{config_path}'")
    print("Please edit this file with your actual Jira credentials and project details.")


def main():
    """Main function to run the Jira work distribution analysis."""
    parser = argparse.ArgumentParser(description="Analyze work distribution in a Jira project")
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

    # Extract configuration values
    jira_config = config["jira"]
    project_config = config["project"]
    output_config = config.get("output", {})

    jira_url = jira_config["base_url"]
    username = jira_config["username"]
    api_token = jira_config["api_token"]
    project_key = project_config["key"]
    analysis_start_date = project_config.get("analysis_start_date", "2025-01-01")

    # Initialize analyzer
    analyzer = JiraAnalyzer(jira_url, username, api_token)

    try:
        # Fetch completed stories
        print(f"Fetching completed stories from project {project_key}")
        raw_stories = analyzer.fetch_completed_stories(project_key)

        # Process story data
        print("Processing story data...")
        processed_stories = [
            analyzer.extract_story_data(story) for story in raw_stories
        ]

        # Generate CSV files
        print("Generating CSV files...")
        detailed_filename = output_config.get("detailed_csv", "jira_work_distribution_detailed.csv")
        summary_filename = output_config.get("summary_csv", "jira_work_distribution_summary.csv")

        analyzer.generate_detailed_csv(processed_stories, detailed_filename)
        analyzer.generate_summary_csv(processed_stories, summary_filename)

        print("\nAnalysis complete! Check the CSV files for results.")

    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
