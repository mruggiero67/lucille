#!/usr/bin/env python3
"""
Jira Deploy Stage Duration Analysis
Analyzes how long issues stay in "Deploy" status across specified projects
"""

import argparse
import csv
import logging
import statistics
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import requests
import yaml
from utils import fetch_all_issues, create_jira_session


class JiraDeployAnalyzer:
    def __init__(self, config_path: str):
        """Initialize the analyzer with configuration from YAML file."""
        self.logger = logging.getLogger(__name__)
        self.config = self._load_config(config_path)

        self.logger.debug(f"Configuration: {self.config}")
        self.session = self._setup_session()

        # Configuration constants
        self.PROJECTS = self.config.get("projects", ["OOT", "FED"])
        self.WEEKS_BACK = self.config.get("weeks_back", 8)
        self.TARGET_STATUS = self.config.get("target_status", "Deploy")
        self.MAX_RESULTS = self.config.get("max_results", 1000)
        self.jira_url = self.config["base_url"]

        # Calculate start date
        self.start_date = (datetime.now() - timedelta(weeks=self.WEEKS_BACK)).strftime(
            "%Y-%m-%d"
        )

        self.logger.info(
            f"Initialized analyzer for projects {', '.join(self.PROJECTS)} since {self.start_date}"
        )

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file."""
        try:
            with open(config_path, "r") as file:
                config = yaml.safe_load(file)

            required_fields = ["jira", "stage_durations"]
            for field in required_fields:
                if field not in config:
                    raise ValueError(f"Missing field '{field}' in config file")
            jira_config = config["jira"]
            global_config = config["stage_durations"]
            return jira_config | global_config
        except FileNotFoundError:
            self.logger.error(f"Config file not found: {config_path}")
            sys.exit(1)
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML config: {e}")
            sys.exit(1)
        except ValueError as e:
            self.logger.error(f"Config validation error: {e}")
            sys.exit(1)

    def _setup_session(self) -> requests.Session:
        """Setup requests session with authentication."""
        return create_jira_session(
            self.config["base_url"],
            self.config["username"],
            self.config["api_token"]
        )

    def _make_jira_request(self,
                           endpoint: str,
                           params: Optional[Dict] = None) -> Dict:
        """Make authenticated request to Jira API."""
        url = f"{self.config['base_url'].rstrip('/')}/rest/api/3/{endpoint}"

        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Jira API request failed: {e}")
            raise

    def _parse_jira_timestamp(self, timestamp: str) -> datetime:
        """Parse Jira timestamp handling various formats
        and return timezone-aware datetime."""
        from datetime import timezone

        try:
            # Handle UTC 'Z' format
            if timestamp.endswith("Z"):
                return datetime.fromisoformat(timestamp.replace("Z", "+00:00"))

            # Handle timezone offsets like -0700, +0530
            if timestamp[-5] in ["+", "-"] and timestamp[-2:].isdigit():
                # Convert -0700 to -07:00 format
                tz_part = timestamp[-5:]
                timestamp_part = timestamp[:-5]
                formatted_tz = f"{tz_part[:3]}:{tz_part[3:]}"
                return datetime.fromisoformat(timestamp_part + formatted_tz)

            # Try parsing as-is (might already be in correct format)
            dt = datetime.fromisoformat(timestamp)
            # If no timezone info, assume UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt

        except ValueError as e:
            self.logger.error(f"Failed to parse timestamp '{timestamp}': {e}")
            # Fallback: return current time with UTC timezone
            self.logger.warning(
                f"Using current UTC time as fallback for unparseable timestamp: {timestamp}"
            )
            return datetime.now(timezone.utc)

    def calculate_duration_hours(
        self, start_time: str, end_time: Optional[str] = None
    ) -> int:
        """Calculate duration in hours between two timestamps."""
        from datetime import timezone

        start = self._parse_jira_timestamp(start_time)
        end = (
            self._parse_jira_timestamp(end_time)
            if end_time
            else datetime.now(timezone.utc)
        )
        return round((end - start).total_seconds() / 3600)

    def calculate_business_hours(
        self, start_time: str, end_time: Optional[str] = None
    ) -> int:
        """Calculate business hours (excluding weekends) between timestamps."""
        from datetime import timezone

        start = self._parse_jira_timestamp(start_time)
        end = (
            self._parse_jira_timestamp(end_time)
            if end_time
            else datetime.now(timezone.utc)
        )

        business_hours = 0
        current = start.replace(hour=0, minute=0, second=0, microsecond=0)

        while current < end:
            # Skip weekends (0 = Monday, 6 = Sunday)
            if current.weekday() < 5:  # Monday to Friday
                next_day = current + timedelta(days=1)
                day_end = min(next_day, end)
                business_hours += (day_end - max(current, start)).total_seconds() / 3600

            current += timedelta(days=1)

        return round(business_hours)

    def search_issues(self) -> List[Dict]:
        """Search for issues updated within the time window."""
        jql = f"project in ({','.join(self.PROJECTS)}) AND updated >= \"{self.start_date}\" ORDER BY updated DESC"

        self.logger.info(f"Executing JQL: {jql}")
        fields = ['key', 'summary', 'status', 'created', 'updated', 'project']
        issues = fetch_all_issues(session=self.session,
                                  base_url=self.jira_url,
                                  jql=jql,
                                  fields=fields,
                                  max_results=self.MAX_RESULTS)

        self.logger.info(f"Found {len(issues)} issues to analyze")
        return issues

    def get_issue_changelog(self, issue_key: str) -> Dict:
        """Get detailed issue information including changelog."""
        self.logger.debug(f"Getting changelog for {issue_key}")
        return self._make_jira_request(f"issue/{issue_key}", {"expand": "changelog"})

    def find_deploy_transitions(
        self, changelog: Dict
    ) -> Tuple[Optional[str], Optional[str]]:
        """Find Deploy status transitions in issue changelog."""
        deploy_start_time = None
        deploy_end_time = None

        if not changelog.get("changelog", {}).get("histories"):
            return deploy_start_time, deploy_end_time

        for history in changelog["changelog"]["histories"]:
            for item in history.get("items", []):
                if item.get("field") == "status":
                    # Transitioned TO Deploy
                    if item.get("toString") == self.TARGET_STATUS:
                        deploy_start_time = history["created"]
                    # Transitioned FROM Deploy
                    elif item.get("fromString") == self.TARGET_STATUS:
                        deploy_end_time = history["created"]

        return deploy_start_time, deploy_end_time

    def process_issue(self, issue: Dict) -> Optional[Dict]:
        """Process a single issue to extract deploy duration data."""
        issue_key = issue["key"]
        self.logger.debug(f"Processing {issue_key}")

        try:
            detailed_issue = self.get_issue_changelog(issue_key)
            current_status = detailed_issue["fields"]["status"]["name"]

            deploy_start_time, deploy_end_time = self.find_deploy_transitions(
                detailed_issue
            )

            if not deploy_start_time:
                self.logger.debug(f"No Deploy transition found for {issue_key}")
                return None

            still_in_deploy = current_status == self.TARGET_STATUS
            duration_hours = self.calculate_duration_hours(
                deploy_start_time, deploy_end_time
            )
            # we don't use business hours for now.
            # business_hours = self.calculate_business_hours(
            #    deploy_start_time, deploy_end_time
            # )

            record = {
                "issue_key": issue_key,
                "summary": issue["fields"]["summary"],
                "project": issue["fields"]["project"]["key"],
                "deploy_start_time": deploy_start_time,
                "deploy_end_time": deploy_end_time or "Still in Deploy",
                "current_status": current_status,
                "duration_hours": duration_hours,
                "still_in_deploy": still_in_deploy,
            }

            status_text = "ongoing" if still_in_deploy else "completed"
            self.logger.debug(
                f"{issue_key}: {duration_hours}h in Deploy ({status_text})"
            )

            return record

        except Exception as e:
            self.logger.error(f"Error processing {issue_key}: {e}")
            return None

    def calculate_statistics(self, durations: List[int]) -> Dict:
        """Calculate summary statistics for deploy durations."""
        if not durations:
            return {}

        return {
            "min_hours": min(durations),
            "max_hours": max(durations),
            "mean_hours": round(statistics.mean(durations), 2),
            "median_hours": statistics.median(durations),
            "std_dev_hours": round(
                statistics.stdev(durations) if len(durations) > 1 else 0, 2
            ),
        }

    def write_detailed_csv(self, data: List[Dict], filename: str) -> None:
        """Write detailed issue data to CSV file."""
        if not data:
            self.logger.warning("No data to write to detailed CSV")
            return

        fieldnames = [
            "issue_key",
            "summary",
            "project",
            "deploy_start_time",
            "deploy_end_time",
            "current_status",
            "duration_hours",
            "still_in_deploy",
        ]

        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

        self.logger.info(f"Detailed data written to {filename}")

    def write_summary_csv(
        self, stats: Dict, total_issues: int, still_in_deploy: int, filename: str
    ) -> None:
        """Write summary statistics to CSV file."""
        if not stats:
            self.logger.warning("No statistics to write to summary CSV")
            return

        summary_data = [
            {
                "metric": "Total Issues Analyzed",
                "value": total_issues,
                "unit": "count"
            },
            {
                "metric": "Issues Still in Deploy",
                "value": still_in_deploy,
                "unit": "count",
            },
            {
                "metric": "Minimum Duration",
                "value": stats["min_hours"],
                "unit": "hours",
            },
            {
                "metric": "Maximum Duration",
                "value": stats["max_hours"],
                "unit": "hours",
            },
            {
                "metric": "Mean Duration",
                "value": stats["mean_hours"],
                "unit": "hours"
            },
            {
                "metric": "Median Duration",
                "value": stats["median_hours"], "unit": "hours",
            },
            {
                "metric": "Standard Deviation",
                "value": stats["std_dev_hours"],
                "unit": "hours",
            },
            {
                "metric": "Mean Duration (Days)",
                "value": round(stats["mean_hours"] / 24, 2),
                "unit": "days",
            },
            {
                "metric": "Median Duration (Days)",
                "value": round(stats["median_hours"] / 24, 2),
                "unit": "days",
            },
        ]

        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=["metric", "value", "unit"])
            writer.writeheader()
            writer.writerows(summary_data)

        self.logger.info(f"Summary statistics written to {filename}")

    def print_summary(
        self, stats: Dict, total_issues: int, still_in_deploy: int
    ) -> None:
        """Print analysis summary to console."""
        print("\n" + "=" * 50)
        print("DEPLOY DURATION ANALYSIS SUMMARY")
        print("=" * 50)
        print(
            f"Analysis Period: Last {self.WEEKS_BACK} weeks (since {self.start_date})"
        )
        print(f"Projects: {', '.join(self.PROJECTS)}")
        print(f"Total Issues Analyzed: {total_issues}")
        print(f"Issues Still in Deploy: {still_in_deploy}")

        if stats:
            print(f"\nDuration Statistics (in hours):")
            print(f"  Minimum: {stats['min_hours']}")
            print(f"  Maximum: {stats['max_hours']}")
            print(f"  Mean: {stats['mean_hours']}")
            print(f"  Median: {stats['median_hours']}")
            print(f"  Standard Deviation: {stats['std_dev_hours']}")

            print(f"\nDuration Statistics (in days):")
            print(f"  Minimum: {round(stats['min_hours'] / 24, 2)}")
            print(f"  Maximum: {round(stats['max_hours'] / 24, 2)}")
            print(f"  Mean: {round(stats['mean_hours'] / 24, 2)}")
            print(f"  Median: {round(stats['median_hours'] / 24, 2)}")
            print(f"  Standard Deviation: {round(stats['std_dev_hours'] / 24, 2)}")

    def run_analysis(self) -> None:
        """Run the complete analysis workflow."""
        self.logger.info("Starting deploy duration analysis")

        # Search for issues
        issues = self.search_issues()
        if not issues:
            self.logger.warning("No issues found for analysis")
            return

        # Process each issue
        detailed_data = []
        for issue in issues:
            self.logger.info(f"Analyzing issue {issue['key']}")
            record = self.process_issue(issue)
            if record:
                detailed_data.append(record)

        if not detailed_data:
            self.logger.warning(f"No issues with {self.TARGET_STATUS}")
            return

        self.logger.info(f"Processed {len(detailed_data)} {self.TARGET_STATUS}")

        # Calculate statistics
        durations = [record["duration_hours"] for record in detailed_data]
        stats = self.calculate_statistics(durations)

        total_issues = len(detailed_data)
        still_in_deploy = sum(
            1 for record in detailed_data if record["still_in_deploy"]
        )

        # Generate output files
        csv_path = self.config.get("output_directory", ".")
        timestamp = datetime.now().strftime("%Y_%m_%d")

        detailed_filename = f"{timestamp}_stage_durations_detailed.csv"
        detailed_csv_path = Path(csv_path) / detailed_filename

        summary_filename = f"{timestamp}_stage_durations_summary.csv"
        summary_csv_path = Path(csv_path) / summary_filename

        self.write_detailed_csv(detailed_data, detailed_csv_path)
        self.write_summary_csv(
            stats, total_issues, still_in_deploy, summary_csv_path
        )

        # Print summary
        self.print_summary(stats, total_issues, still_in_deploy)

        self.logger.info("Analysis completed successfully")


def setup_logging(verbose: bool = False) -> None:
    """Setup logging configuration."""
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(levelname)-10s %(asctime)s %(filename)s %(lineno)d %(message)s",
        level=log_level
    )


def main():
    parser = argparse.ArgumentParser(
        description="Analyze Jira Deploy stage durations",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("config", help="Path to YAML configuration file")
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    try:
        analyzer = JiraDeployAnalyzer(args.config)
        analyzer.run_analysis()
    except KeyboardInterrupt:
        logging.getLogger(__name__).info("Analysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.getLogger(__name__).error(f"Analysis failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
