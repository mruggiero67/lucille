"""
Jira Lead Time Baseline Calculator
Calculates development lead times from Jira data to establish baseline metrics.
"""

import requests
import csv
import yaml
import base64
import argparse
from datetime import datetime, timedelta
import statistics
import os
import sys
from typing import List, Dict, Any, Optional
from pathlib import Path
import logging
from pprint import pformat

# Handle both direct script execution and module import
try:
    from .utils import fetch_all_issues
except ImportError:
    # Add parent directory to path for direct script execution
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from lucille.jira.utils import fetch_all_issues


logging.basicConfig(
    format="%(levelname)-10s %(asctime)s %(filename)s %(lineno)d %(message)s",
    level=logging.DEBUG,
)


class JiraLeadTimeAnalyzer:
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
        self.epic_keys = config.get("epic_keys", [])
        self.days_back = config.get("days_back", 60)
        self.done_statuses = [
            status.upper()
            for status in config.get("done_statuses", ["Done", "Closed", "Resolved"])
        ]
        self.dev_statuses = [
            status.upper()
            for status in config.get(
                "development_statuses", ["In Development", "In Progress", "Development"]
            )
        ]
        self.output_directory = config["output_directory"]

    def get_completed_stories(self) -> List[Dict[str, Any]]:
        """Get completed stories from specific epics."""
        if not self.epic_keys:
            logging.info("No epic keys specified in configuration")
            return []

        logging.info(f"Fetching completed stories from {len(self.epic_keys)} epics:")
        for epic_key in self.epic_keys:
            logging.info(f"  - {epic_key}")

        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.days_back)

        all_stories = []

        for epic_key in self.epic_keys:
            logging.info(f"\nProcessing epic: {epic_key}")

            # JQL to find completed stories that are children of this epic
            done_status_list = "', '".join(self.done_statuses)
            jql = f'("Epic Link" = {epic_key} OR parent = {epic_key}) AND status in (\'{done_status_list}\') AND resolved >= "{start_date.strftime("%Y-%m-%d")}"'

            logging.info(f"  JQL: {jql}")

            # Create a session for the API calls
            session = requests.Session()
            session.headers.update(self.headers)

            fields = ["summary",
                      "status",
                      "issuetype",
                      "created",
                      "updated",
                      "resolutiondate",
                      "assignee",
                      "priority",
                      "customfield_10016"]

            try:
                # Use the shared utils function for pagination
                issues = fetch_all_issues(
                    session=session,
                    base_url=self.base_url,
                    jql=jql,
                    fields=fields,
                    expand="changelog",
                    max_results=None  # No limit for epic stories
                )

                # Add epic information to each story
                for issue in issues:
                    issue["epic_key"] = epic_key

                epic_stories = issues

            except requests.exceptions.RequestException as e:
                logging.info(f"  Error fetching stories for epic {epic_key}: {e}")
                epic_stories = []

            logging.info(f"  Found {len(epic_stories)} completed stories in epic {epic_key}")
            all_stories.extend(epic_stories)

        logging.info(f"\nTotal stories fetched across all epics: {len(all_stories)}")
        return all_stories

    def parse_story_timeline(self, story: Dict[str, Any]) -> Dict[str, Any]:
        """Parse story timeline from changelog to calculate lead times."""
        key = story["key"]
        fields = story["fields"]
        changelog = story.get("changelog", {}).get("histories", [])

        # Basic info
        story_info = {
            "key": key,
            "epic_key": story.get("epic_key", "Unknown"),
            "summary": fields.get("summary", ""),
            "issue_type": fields.get("issuetype", {}).get("name", "Unknown"),
            "assignee": (
                fields.get("assignee", {}).get("displayName", "Unassigned")
                if fields.get("assignee")
                else "Unassigned"
            ),
            "priority": (
                fields.get("priority", {}).get("name", "Unknown")
                if fields.get("priority")
                else "Unknown"
            ),
            "story_points": fields.get(
                "customfield_10016"
            ),  # Adjust field name as needed
            "created": self._parse_datetime(fields.get("created")),
            "resolved": self._parse_datetime(fields.get("resolutiondate")),
            "final_status": fields.get("status", {}).get("name", "Unknown"),
        }

        # Parse timeline from changelog
        timeline = self._extract_status_timeline(changelog)

        # Calculate key timestamps
        timestamps = {
            "created_date": story_info["created"],
            "first_dev_start": self._find_first_dev_start(timeline),
            "last_dev_start": self._find_last_dev_start(timeline),
            "resolved_date": story_info["resolved"],
        }

        # Calculate lead times (in business days)
        lead_times = self._calculate_lead_times(timestamps)
        logging.info(f"lead_times: {pformat(lead_times)}")

        return {**story_info, **timestamps, **lead_times, "timeline": timeline}

    def _parse_datetime(self, date_string: str) -> Optional[datetime]:
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
            logging.info(f"Warning: Could not parse datetime '{date_string}': {e}")
            return None

    def _extract_status_timeline(self,
                                 changelog: List[Dict]) -> List[Dict[str, Any]]:
        """Extract status changes from changelog."""
        timeline = []

        for history in changelog:
            created = self._parse_datetime(history.get("created"))
            author = history.get("author", {}).get("displayName", "Unknown")

            for item in history.get("items", []):
                if item.get("field") == "status":
                    timeline.append(
                        {
                            "date": created,
                            "author": author,
                            "from_status": item.get("fromString"),
                            "to_status": item.get("toString"),
                        }
                    )

        # Sort by date
        timeline.sort(key=lambda x: x["date"] if x["date"] else datetime.min)
        return timeline

    def _find_first_dev_start(self,
                              timeline: List[Dict]) -> Optional[datetime]:
        """Find when story first entered development status."""
        for event in timeline:
            if event["to_status"] and event["to_status"].upper() in self.dev_statuses:
                return event["date"]
        return None

    def _find_last_dev_start(self, timeline: List[Dict]) -> Optional[datetime]:
        """Find when story last entered development status (for rework scenarios)."""
        last_dev_start = None
        for event in timeline:
            if event["to_status"] and event["to_status"].upper() in self.dev_statuses:
                last_dev_start = event["date"]
        return last_dev_start

    def _calculate_lead_times(
        self, timestamps: Dict[str, Optional[datetime]]
    ) -> Dict[str, Optional[float]]:
        """Calculate various lead time metrics in business days."""

        def business_days_between(
            start: Optional[datetime], end: Optional[datetime]
        ) -> Optional[float]:
            if not start or not end:
                return None

            # Simple approximation: total days * 5/7 (assuming 5-day work week)
            total_days = (end - start).total_seconds() / (24 * 3600)
            return round(total_days * (5 / 7), 1)

        return {
            "total_lead_time": business_days_between(
                timestamps["created_date"], timestamps["resolved_date"]
            ),
            "dev_lead_time": business_days_between(
                timestamps["first_dev_start"], timestamps["resolved_date"]
            ),
            "time_to_dev": business_days_between(
                timestamps["created_date"], timestamps["first_dev_start"]
            ),
            "pure_dev_time": business_days_between(
                timestamps["last_dev_start"], timestamps["resolved_date"]
            ),
        }

    def analyze_lead_times(self, stories: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze lead time distribution and calculate key metrics."""
        logging.info("Analyzing lead time patterns...")

        parsed_stories = []
        for story in stories:
            parsed = self.parse_story_timeline(story)
            parsed_stories.append(parsed)

        # Extract lead time values (excluding None values)
        metrics = {}
        epic_breakdown = {}

        # Group stories by epic for breakdown analysis
        for story in parsed_stories:
            epic_key = story["epic_key"]
            if epic_key not in epic_breakdown:
                epic_breakdown[epic_key] = []
            epic_breakdown[epic_key].append(story)

        for metric_name in [
            "total_lead_time",
            "dev_lead_time",
            "time_to_dev",
            "pure_dev_time",
        ]:
            values = [
                s[metric_name] for s in parsed_stories if s[metric_name] is not None
            ]

            if values:
                metrics[metric_name] = {
                    "count": len(values),
                    "median": round(statistics.median(values), 1),
                    "mean": round(statistics.mean(values), 1),
                    "percentile_95": round(self._percentile(values, 95), 1),
                    "percentile_85": round(self._percentile(values, 85), 1),
                    "min": round(min(values), 1),
                    "max": round(max(values), 1),
                }
            else:
                metrics[metric_name] = {
                    "count": 0,
                    "median": None,
                    "mean": None,
                    "percentile_95": None,
                    "percentile_85": None,
                    "min": None,
                    "max": None,
                }

        # Calculate per-epic metrics
        epic_metrics = {}
        for epic_key, epic_stories in epic_breakdown.items():
            epic_dev_times = [
                s["dev_lead_time"]
                for s in epic_stories
                if s["dev_lead_time"] is not None
            ]
            epic_metrics[epic_key] = {
                "story_count": len(epic_stories),
                "completed_with_dev_time": len(epic_dev_times),
                "median_dev_time": (
                    round(statistics.median(epic_dev_times), 1)
                    if epic_dev_times
                    else None
                ),
                "avg_dev_time": (
                    round(statistics.mean(epic_dev_times), 1)
                    if epic_dev_times
                    else None
                ),
            }

        return {
            "stories": parsed_stories,
            "metrics": metrics,
            "epic_breakdown": epic_breakdown,
            "epic_metrics": epic_metrics,
            "analysis_period": f"Last {self.days_back} days",
            "total_stories": len(parsed_stories),
            "epics_analyzed": len(epic_breakdown),
        }

    def _percentile(self, values: List[float], percentile: int) -> float:
        """Calculate percentile of a list of values."""
        return statistics.quantiles(values, n=100)[percentile - 1]

    def save_detailed_csv(self, analysis: Dict[str, Any], filename: str = None):
        """Save detailed story analysis to CSV."""
        Path(self.output_directory).mkdir(parents=True, exist_ok=True)

        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"lead_time_baseline_{timestamp}.csv"

        filepath = Path(self.output_directory) / filename

        fieldnames = [
            "epic_key",
            "key",
            "summary",
            "issue_type",
            "assignee",
            "priority",
            "story_points",
            "created_date",
            "resolved_date",
            "first_dev_start",
            "last_dev_start",
            "total_lead_time",
            "dev_lead_time",
            "time_to_dev",
            "pure_dev_time",
            "final_status",
            "created",
            "resolved",
        ]

        with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for story in analysis["stories"]:
                # Convert datetime objects to strings for CSV
                row = story.copy()
                for date_field in [
                    "created_date",
                    "resolved_date",
                    "first_dev_start",
                    "last_dev_start",
                ]:
                    if row[date_field]:
                        row[date_field] = row[date_field].strftime("%Y-%m-%d %H:%M")
                    else:
                        row[date_field] = ""

                # Remove timeline for CSV (too complex)
                row.pop("timeline", None)
                writer.writerow(row)

        logging.info(f"Detailed analysis saved to {filepath}")
        return str(filepath)

    def save_summary_csv(self, analysis: Dict[str, Any], filename: str = None):
        """Save summary metrics to CSV."""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"lead_time_summary_{timestamp}.csv"

        filepath = Path(self.output_directory) / filename

        with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(
                [
                    "Metric",
                    "Count",
                    "Median_Days",
                    "Mean_Days",
                    "95th_Percentile",
                    "85th_Percentile",
                    "Min_Days",
                    "Max_Days",
                ]
            )

            for metric_name, values in analysis["metrics"].items():
                writer.writerow(
                    [
                        metric_name.replace("_", " ").title(),
                        values["count"],
                        values["median"],
                        values["mean"],
                        values["percentile_95"],
                        values["percentile_85"],
                        values["min"],
                        values["max"],
                    ]
                )

        logging.info(f"Summary metrics saved to {filepath}")
        return str(filepath)

    def print_analysis(self, analysis: Dict[str, Any]):
        """Print lead time analysis summary."""
        print("\n" + "=" * 60)
        print("LEAD TIME BASELINE ANALYSIS")
        print("=" * 60)
        print(f"Analysis Period: {analysis['analysis_period']}")
        print(f"Total Stories Analyzed: {analysis['total_stories']}")
        print(f"Epics Analyzed: {analysis['epics_analyzed']}")

        # Epic breakdown
        print(f"\nSTORIES PER EPIC:")
        print("-" * 40)
        for epic_key, metrics in analysis["epic_metrics"].items():
            print(f"{epic_key}: {metrics['story_count']} stories")
            if metrics["median_dev_time"]:
                print(f"  └─ Median dev time: {metrics['median_dev_time']} days")

        print(f"\nOVERALL METRICS (Business Days):")
        print("-" * 60)
        print(f"{'Metric':<20} {'Median':<8} {'95th %':<8} {'Mean':<8} {'Count':<8}")
        print("-" * 60)

        for metric_name, values in analysis["metrics"].items():
            if values["count"] > 0:
                display_name = metric_name.replace("_", " ").title()
                print(
                    f"{display_name:<20} {values['median']:<8} {values['percentile_95']:<8} {values['mean']:<8} {values['count']:<8}"
                )

        # Key insights
        dev_metrics = analysis["metrics"].get("dev_lead_time", {})
        total_metrics = analysis["metrics"].get("total_lead_time", {})

        if dev_metrics.get("median") and total_metrics.get("median"):
            dev_portion = (dev_metrics["median"] / total_metrics["median"]) * 100
            print(f"\nKEY INSIGHTS:")
            print(f"• Development represents {dev_portion:.0f}% of total lead time")
            print(f"• Median development time: {dev_metrics['median']} days")
            print(
                f"• 95% of stories complete development within {dev_metrics['percentile_95']} days"
            )

            if dev_metrics["percentile_95"] > dev_metrics["median"] * 2:
                print(
                    f"• High variability detected (95th percentile is {dev_metrics['percentile_95']/dev_metrics['median']:.1f}x median)"
                )


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        logging.info(f"Error: Configuration file '{config_path}' not found.")
        sys.exit(1)
    except yaml.YAMLError as e:
        logging.info(f"Error parsing YAML configuration: {e}")
        sys.exit(1)


def create_sample_config(config_path: str):
    """Create sample configuration file."""
    sample_config = {
        "jira": {
            "base_url": "https://your-domain.atlassian.net",
            "username": "your-email@company.com",
            "api_token": "your_jira_api_token",
        },
        "epic_keys": [
            "PROJ-123",  # Epic from project PROJ
            "FRONTEND-456",  # Epic from project FRONTEND
            "BACKEND-789",  # Epic from project BACKEND
            "MOBILE-101",  # Epic from project MOBILE
        ],
        "days_back": 60,  # How many days back to analyze
        "done_statuses": ["Done", "Closed", "Resolved", "Complete"],
        "development_statuses": [
            "In Development",
            "In Progress",
            "Development",
            "Code Review",
        ],
        "output_directory": "./lead_time_analysis",
    }

    with open(config_path, "w") as file:
        yaml.dump(sample_config, file, default_flow_style=False, sort_keys=False)

    logging.info(f"Sample configuration created at '{config_path}'")
    logging.info("Edit this file with your Jira details and run again.")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Jira Lead Time Baseline Calculator")
    parser.add_argument(
        "--config",
        type=str,
        default="lead_time_config.yaml",
        help="Path to the YAML configuration file.",
    )
    args = parser.parse_args()
    config_path = args.config

    if not os.path.exists(config_path):
        logging.info(f"Configuration file not found. Creating sample...")
        create_sample_config(config_path)
        return

    config = load_config(config_path)
    analyzer = JiraLeadTimeAnalyzer(config)

    # Fetch and analyze stories
    stories = analyzer.get_completed_stories()
    if not stories:
        logging.info("No completed stories found. Check your configuration.")
        return

    analysis = analyzer.analyze_lead_times(stories)

    # Save results
    analyzer.save_detailed_csv(analysis)
    analyzer.save_summary_csv(analysis)

    # Print summary
    analyzer.print_analysis(analysis)


if __name__ == "__main__":
    main()
