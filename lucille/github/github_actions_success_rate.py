#!/usr/bin/env python3
"""
GitHub Actions Analyzer
Analyzes GitHub Actions across multiple repositories to identify failure patterns
and generate detailed reports on CI/CD health.
"""

import argparse
import csv
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import yaml
import requests
from pathlib import Path
from dataclasses import dataclass, asdict
import time


@dataclass
class WorkflowRun:
    """Represents a single workflow run"""

    repo: str
    workflow_name: str
    run_id: int
    status: str
    conclusion: str
    created_at: str
    updated_at: str
    head_branch: str
    head_sha: str
    run_number: int
    attempt_number: int
    html_url: str


@dataclass
class WeeklySummary:
    """Weekly summary statistics for a repository"""

    repo: str
    week_start: str
    total_runs: int
    successful_runs: int
    failed_runs: int
    cancelled_runs: int
    success_rate: float
    failure_rate: float


class GitHubActionsAnalyzer:
    """Analyzes GitHub Actions workflow runs across multiple repositories"""

    def __init__(self, config_path: str):
        """Initialize the analyzer with configuration"""
        self.config = self._load_config(config_path)
        self.token = self.config["github_token"]
        self.repos = [
            f"{repo['org']}/{repo['repo']}" for repo in self.config["repositories"]
        ]
        self.weeks_to_query = self.config.get("weeks_to_query", 8)
        self.base_url = "https://api.github.com"
        self.csv_directory = self.config.get("csv_directory", ".")

        # Set up headers for GitHub API
        self.headers = {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "GitHub-Actions-Analyzer/1.0",
        }

        # Rate limiting
        self.request_count = 0
        self.rate_limit_remaining = 5000

    def _load_config(self, config_path: str) -> dict:
        """Load configuration from YAML file"""
        try:
            with open(config_path, "r") as file:
                config = yaml.safe_load(file)

            # Validate required fields
            required_fields = ["github_token", "repositories"]
            for field in required_fields:
                if field not in config:
                    raise ValueError(f"Missing required field: {field}")

            # Validate repository format
            if not isinstance(config["repositories"], list):
                raise ValueError("repositories must be a list")

            for i, repo in enumerate(config["repositories"]):
                if (
                    not isinstance(repo, dict)
                    or "org" not in repo
                    or "repo" not in repo
                ):
                    raise ValueError(
                        f"Repository {i+1} must have 'org' and 'repo' fields"
                    )

            return config

        except FileNotFoundError:
            print(f"Error: Config file '{config_path}' not found")
            sys.exit(1)
        except yaml.YAMLError as e:
            print(f"Error parsing YAML config: {e}")
            sys.exit(1)
        except ValueError as e:
            print(f"Config validation error: {e}")
            sys.exit(1)

    def _make_request(self, url: str, params: dict = None) -> Optional[dict]:
        """Make a rate-limited request to GitHub API"""
        self.request_count += 1

        try:
            response = requests.get(url, headers=self.headers, params=params)

            # Update rate limit info
            self.rate_limit_remaining = int(
                response.headers.get("X-RateLimit-Remaining", 0)
            )

            if response.status_code == 403 and self.rate_limit_remaining == 0:
                reset_time = int(response.headers.get("X-RateLimit-Reset", 0))
                sleep_time = reset_time - int(time.time()) + 10
                print(f"Rate limit exceeded. Sleeping for {sleep_time} seconds...")
                time.sleep(sleep_time)
                return self._make_request(url, params)

            response.raise_for_status()
            return response.json()

        except requests.RequestException as e:
            print(f"API request failed: {e}")
            return None

    def _get_date_range(self) -> Tuple[str, str]:
        """Calculate the date range for querying workflow runs"""
        end_date = datetime.now()
        start_date = end_date - timedelta(weeks=self.weeks_to_query)

        return start_date.isoformat(), end_date.isoformat()

    def _get_workflow_runs(self, repo: str) -> List[WorkflowRun]:
        """Get all workflow runs for a repository within the date range"""
        start_date, end_date = self._get_date_range()

        print(f"Fetching workflow runs for {repo}...")

        runs = []
        page = 1
        per_page = 100

        while True:
            url = f"{self.base_url}/repos/{repo}/actions/runs"
            params = {
                "created": f"{start_date}..{end_date}",
                "per_page": per_page,
                "page": page,
            }

            data = self._make_request(url, params)
            if not data or "workflow_runs" not in data:
                break

            workflow_runs = data["workflow_runs"]
            if not workflow_runs:
                break

            for run in workflow_runs:
                workflow_run = WorkflowRun(
                    repo=repo,
                    workflow_name=run.get("name", "Unknown"),
                    run_id=run["id"],
                    status=run["status"],
                    conclusion=run.get("conclusion", ""),
                    created_at=run["created_at"],
                    updated_at=run["updated_at"],
                    head_branch=run.get("head_branch", ""),
                    head_sha=run.get("head_sha", ""),
                    run_number=run.get("run_number", 0),
                    attempt_number=run.get("run_attempt", 1),
                    html_url=run.get("html_url", ""),
                )
                runs.append(workflow_run)

            page += 1

            # GitHub API returns max 1000 results
            if len(workflow_runs) < per_page or len(runs) >= 1000:
                break

        print(f"Retrieved {len(runs)} workflow runs for {repo}")
        return runs

    def _calculate_weekly_summaries(
        self, runs: List[WorkflowRun]
    ) -> List[WeeklySummary]:
        """Calculate weekly summary statistics"""
        weekly_data = {}

        for run in runs:
            # Parse the created_at date
            created_date = datetime.fromisoformat(run.created_at.replace("Z", "+00:00"))

            # Calculate the start of the week (Monday)
            week_start = created_date - timedelta(days=created_date.weekday())
            week_key = (run.repo, week_start.strftime("%Y-%m-%d"))

            if week_key not in weekly_data:
                weekly_data[week_key] = {
                    "repo": run.repo,
                    "week_start": week_start.strftime("%Y-%m-%d"),
                    "total": 0,
                    "success": 0,
                    "failure": 0,
                    "cancelled": 0,
                }

            weekly_data[week_key]["total"] += 1

            if run.conclusion == "success":
                weekly_data[week_key]["success"] += 1
            elif run.conclusion == "failure":
                weekly_data[week_key]["failure"] += 1
            elif run.conclusion == "cancelled":
                weekly_data[week_key]["cancelled"] += 1

        # Convert to WeeklySummary objects
        summaries = []
        for week_data in weekly_data.values():
            total = week_data["total"]
            success_rate = (week_data["success"] / total * 100) if total > 0 else 0
            failure_rate = (week_data["failure"] / total * 100) if total > 0 else 0

            summary = WeeklySummary(
                repo=week_data["repo"],
                week_start=week_data["week_start"],
                total_runs=total,
                successful_runs=week_data["success"],
                failed_runs=week_data["failure"],
                cancelled_runs=week_data["cancelled"],
                success_rate=round(success_rate, 2),
                failure_rate=round(failure_rate, 2),
            )
            summaries.append(summary)

        return sorted(summaries, key=lambda x: (x.repo, x.week_start))

    def _write_detailed_csv(self, runs: List[WorkflowRun], filename: str):
        """Write detailed CSV with all workflow runs"""
        print(f"Writing detailed results to {filename}...")

        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "repo",
                "workflow_name",
                "run_id",
                "status",
                "conclusion",
                "created_at",
                "updated_at",
                "head_branch",
                "head_sha",
                "run_number",
                "attempt_number",
                "html_url",
            ]

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for run in runs:
                writer.writerow(asdict(run))

    def _write_summary_csv(self, summaries: List[WeeklySummary], filename: str):
        """Write summary CSV with weekly statistics"""
        print(f"Writing summary results to {filename}...")

        with open(filename, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "repo",
                "week_start",
                "total_runs",
                "successful_runs",
                "failed_runs",
                "cancelled_runs",
                "success_rate",
                "failure_rate",
            ]

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for summary in summaries:
                writer.writerow(asdict(summary))

    def analyze(self):
        """Run the complete analysis"""
        print("Starting GitHub Actions analysis...")
        print(
            f"Analyzing {len(self.repos)} repositories over {self.weeks_to_query} weeks"
        )
        print(f"Date range: {self._get_date_range()[0]} to {self._get_date_range()[1]}")
        print()

        all_runs = []

        # Collect workflow runs from all repositories
        for repo in self.repos:
            try:
                runs = self._get_workflow_runs(repo)
                all_runs.extend(runs)
            except Exception as e:
                print(f"Error processing repository {repo}: {e}")
                continue

        if not all_runs:
            print("No workflow runs found!")
            return

        # Sort runs by creation date
        all_runs.sort(key=lambda x: x.created_at)

        # Calculate summaries
        summaries = self._calculate_weekly_summaries(all_runs)

        # Generate output filenames with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        csv_path = Path(self.csv_directory)
        detailed_filename = csv_path / f"github_actions_detailed_{timestamp}.csv"
        summary_filename = csv_path / f"github_actions_summary_{timestamp}.csv"

        # Write output files
        self._write_detailed_csv(all_runs, detailed_filename)
        self._write_summary_csv(summaries, summary_filename)

        # Print summary statistics
        print("\n" + "=" * 60)
        print("ANALYSIS COMPLETE")
        print("=" * 60)
        print(f"Total workflow runs analyzed: {len(all_runs)}")
        print(f"Total API requests made: {self.request_count}")
        print(f"Remaining API rate limit: {self.rate_limit_remaining}")
        print(f"Files generated:")
        print(f"  - Detailed: {detailed_filename}")
        print(f"  - Summary: {summary_filename}")

        # Show top failing repositories
        repo_failures = {}
        for run in all_runs:
            if run.repo not in repo_failures:
                repo_failures[run.repo] = {"total": 0, "failed": 0}
            repo_failures[run.repo]["total"] += 1
            if run.conclusion == "failure":
                repo_failures[run.repo]["failed"] += 1

        print("\nFailure rates by repository:")
        for repo, stats in sorted(
            repo_failures.items(),
            key=lambda x: x[1]["failed"] / x[1]["total"] if x[1]["total"] > 0 else 0,
            reverse=True,
        ):
            if stats["total"] > 0:
                failure_rate = stats["failed"] / stats["total"] * 100
                print(
                    f"  {repo}: {failure_rate:.1f}% ({stats['failed']}/{stats['total']} runs)"
                )


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Analyze GitHub Actions workflow runs across multiple repositories"
    )
    parser.add_argument("config_path", help="Path to YAML configuration file")

    args = parser.parse_args()

    try:
        analyzer = GitHubActionsAnalyzer(args.config_path)
        analyzer.analyze()
    except KeyboardInterrupt:
        print("\nAnalysis interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
