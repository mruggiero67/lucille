#!/usr/bin/env python3
"""
GitHub Contributions Analyzer

Analyzes GitHub repositories to track weekly contributions by author.
Uses YAML config file, outputs CSV reports.
"""

import yaml
import requests
import csv
import os
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List
import argparse
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class GitHubAnalyzer:
    def __init__(self, config_path: str):
        """Initialize the GitHub analyzer with configuration."""
        self.config = self.load_config(config_path)
        self.github_token = self.config["github_token"]
        self.csv_directory = self.config["csv_directory"]
        self.repositories = self.config["repositories"]

        # Ensure CSV directory exists
        os.makedirs(self.csv_directory, exist_ok=True)

        # GitHub API headers
        self.headers = {
            "Authorization": f"token {self.github_token}",
            "Accept": "application/vnd.github.v3+json",
        }

    def load_config(self, config_path: str) -> dict:
        """Load YAML configuration file."""
        try:
            with open(config_path, "r") as file:
                config = yaml.safe_load(file)
                logger.info(f"Loaded configuration from {config_path}")
                return config
        except FileNotFoundError:
            logger.error(f"Configuration file {config_path} not found")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing YAML configuration: {e}")
            raise

    def get_commits_for_repo(
        self, org: str, repo: str, since: datetime = None
    ) -> List[dict]:
        """Fetch commits for a specific repository."""
        url = f"https://api.github.com/repos/{org}/{repo}/commits"
        params = {"per_page": 100, "page": 1}

        if since:
            params["since"] = since.isoformat()

        all_commits = []

        while True:
            try:
                response = requests.get(url,
                                        headers=self.headers,
                                        params=params)
                response.raise_for_status()

                commits = response.json()
                if not commits:
                    break

                all_commits.extend(commits)

                # Check if there are more pages
                if "next" not in response.links:
                    break

                params["page"] += 1
                logger.info(
                    f"Fetched {len(all_commits)} commits for {org}/{repo}"
                )

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching commits for {org}/{repo}: {e}")
                break

        logger.info(f"Total commits ({org}/{repo}): {len(all_commits)}")
        return all_commits

    def get_week_start(self, date: datetime) -> datetime:
        """Get the start of the week (Monday) for a given date."""
        days_since_monday = date.weekday()
        week_start = date - timedelta(days=days_since_monday)
        return week_start.replace(hour=0, minute=0, second=0, microsecond=0)

    def analyze_weekly_contributions(
        self, weeks_back: int = 12
    ) -> Dict[str, Dict[str, Dict[str, int]]]:
        """Analyze weekly contributions across all repositories."""
        # Calculate the date range
        end_date = datetime.now()
        start_date = end_date - timedelta(weeks=weeks_back)

        logger.info(
            f"Analyzing {start_date.date()} to {end_date.date()}"
        )

        # Structure: {repo_name: {week: {author: commit_count}}}
        weekly_data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

        for repo_config in self.repositories:
            org = repo_config["org"]
            repo = repo_config["repo"]
            repo_name = f"{org}/{repo}"

            logger.info(f"Processing repository: {repo_name}")

            # Fetch commits
            commits = self.get_commits_for_repo(org, repo, since=start_date)

            for commit in commits:
                # Parse commit date
                commit_date_str = commit["commit"]["author"]["date"]
                commit_date = datetime.fromisoformat(
                    commit_date_str.replace("Z", "+00:00")
                )
                commit_date = commit_date.replace(
                    tzinfo=None
                )  # Remove timezone for simplicity

                # Skip commits outside our date range
                if commit_date < start_date or commit_date > end_date:
                    continue

                # Get week start
                week_start = self.get_week_start(commit_date)
                week_key = week_start.strftime("%Y-%m-%d")

                # Get author information
                author_name = commit["commit"]["author"]["name"]
                if commit["author"]:  # GitHub user info available
                    author_login = commit["author"]["login"]
                    author = f"{author_name} ({author_login})"
                else:
                    author = author_name

                # Count the commit
                weekly_data[repo_name][week_key][author] += 1

        return weekly_data

    def generate_csv_reports(self,
                             weekly_data: Dict[str, Dict[str, Dict[str, int]]]):
        """Generate CSV reports from the weekly data."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Generate overall summary report
        self.generate_overall_summary(weekly_data, timestamp)

        # Generate per-repository reports
        for repo_name in weekly_data:
            self.generate_repo_report(repo_name,
                                      weekly_data[repo_name],
                                      timestamp)

    def generate_overall_summary(
        self, weekly_data: Dict[str, Dict[str, Dict[str, int]]], timestamp: str
    ):
        """Generate an overall summary CSV across all repositories."""
        filename = os.path.join(
            self.csv_directory, f"github_contributions_summary_{timestamp}.csv"
        )

        # Collect all weeks and authors
        all_weeks = set()
        all_authors = set()

        for repo_data in weekly_data.values():
            all_weeks.update(repo_data.keys())
            for week_data in repo_data.values():
                all_authors.update(week_data.keys())

        all_weeks = sorted(all_weeks)
        all_authors = sorted(all_authors)

        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)

            # Write header
            header = ["Repository", "Week"] + all_authors + ["Total"]
            writer.writerow(header)

            # Write data for each repository and week
            for repo_name in sorted(weekly_data.keys()):
                repo_data = weekly_data[repo_name]

                for week in all_weeks:
                    row = [repo_name, week]
                    week_total = 0

                    for author in all_authors:
                        commits = repo_data.get(week, {}).get(author, 0)
                        row.append(commits)
                        week_total += commits

                    row.append(week_total)
                    writer.writerow(row)

        logger.info(f"Overall summary report saved to: {filename}")

    def generate_repo_report(
        self, repo_name: str, repo_data: Dict[str, Dict[str, int]], timestamp: str
    ):
        """Generate a detailed CSV report for a specific repository."""
        safe_repo_name = repo_name.replace("/", "_")
        filename = os.path.join(
            self.csv_directory, f"github_contributions_{safe_repo_name}_{timestamp}.csv"
        )

        # Collect all weeks and authors for this repo
        all_weeks = sorted(repo_data.keys())
        all_authors = set()
        for week_data in repo_data.values():
            all_authors.update(week_data.keys())
        all_authors = sorted(all_authors)

        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)

            # Write header
            header = ["Week"] + all_authors + ["Total"]
            writer.writerow(header)

            # Write data for each week
            for week in all_weeks:
                week_data = repo_data[week]
                row = [week]
                week_total = 0

                for author in all_authors:
                    commits = week_data.get(author, 0)
                    row.append(commits)
                    week_total += commits

                row.append(week_total)
                writer.writerow(row)

        logger.info(f"Repository report for {repo_name} saved to: {filename}")

    def run_analysis(self, weeks_back: int = 12):
        """Run the complete analysis pipeline."""
        logger.info("Starting GitHub contributions analysis")

        # Analyze contributions
        weekly_data = self.analyze_weekly_contributions(weeks_back)

        # Generate reports
        self.generate_csv_reports(weekly_data)

        logger.info("Analysis completed successfully!")


def main():
    parser = argparse.ArgumentParser(description="Analyze GitHub contributions by week")
    parser.add_argument(
        "--config",
        "-c",
        default="github_config.yaml",
        help="Path to YAML configuration file (default: github_config.yaml)",
    )
    parser.add_argument(
        "--weeks",
        "-w",
        type=int,
        default=12,
        help="Number of weeks to analyze (default: 12)",
    )
    args = parser.parse_args()

    try:
        analyzer = GitHubAnalyzer(args.config)
        analyzer.run_analysis(args.weeks)
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        raise


if __name__ == "__main__":
    main()
