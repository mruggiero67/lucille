#!/usr/bin/env python3
"""
GitHub Pull Request Age Analyzer
Analyzes multiple repositories, calculates PR age, and exports to CSV.
Configuration driven by YAML file.
"""

import requests
import csv
import yaml
from datetime import datetime, timezone
import os
import sys
from typing import List, Dict, Any
from pathlib import Path
import argparse


class GitHubPRAnalyzer:
    def __init__(self, token: str):
        """
        Initialize the analyzer with GitHub API credentials.

        Args:
            token: GitHub personal access token
        """
        self.token = token
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }

    def get_open_prs(self, org: str, repo: str) -> List[Dict[str, Any]]:
        """
        Fetch all open pull requests from a specific repository.

        Args:
            org: Repository org (organization or username)
            repo: Repository name

        Returns:
            List of pull request data dictionaries
        """
        prs = []
        page = 1
        per_page = 100
        base_url = f"https://api.github.com/repos/{org}/{repo}"

        while True:
            url = f"{base_url}/pulls"
            params = {
                "state": "open",
                "sort": "created",
                "direction": "asc",
                "page": page,
                "per_page": per_page,
            }

            try:
                response = requests.get(url, headers=self.headers, params=params)
                response.raise_for_status()

                page_prs = response.json()
                if not page_prs:
                    break

                # Add repository info to each PR
                for pr in page_prs:
                    pr["repo_org"] = org
                    pr["repo_name"] = repo

                prs.extend(page_prs)
                page += 1

            except requests.exceptions.RequestException as e:
                print(f"Error fetching PRs from {org}/{repo}: {e}")
                return []

        return prs

    def calculate_pr_age(self, created_at: str) -> tuple:
        """
        Calculate the age of a PR in days and hours.

        Args:
            created_at: ISO timestamp when PR was created

        Returns:
            Tuple of (days, hours, total_hours)
        """
        created_time = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)

        age_delta = now - created_time
        total_hours = age_delta.total_seconds() / 3600
        days = age_delta.days
        hours = int(total_hours % 24)

        return days, hours, round(total_hours, 1)

    def get_pr_reviews(self, org: str, repo: str, pr_number: int) -> Dict[str, Any]:
        """
        Get review information for a specific PR.

        Args:
            org: Repository org
            repo: Repository name
            pr_number: Pull request number

        Returns:
            Dictionary with review summary info
        """
        url = f"https://api.github.com/repos/{org}/{repo}/pulls/{pr_number}/reviews"

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            reviews = response.json()

            review_summary = {
                "total_reviews": len(reviews),
                "approved_reviews": len(
                    [r for r in reviews if r["state"] == "APPROVED"]
                ),
                "change_requested": len(
                    [r for r in reviews if r["state"] == "CHANGES_REQUESTED"]
                ),
                "pending_reviews": len([r for r in reviews if r["state"] == "PENDING"]),
            }

            return review_summary

        except requests.exceptions.RequestException as e:
            print(
                f"Warning: Could not fetch reviews for PR #{pr_number} in {org}/{repo}: {e}"
            )
            return {
                "total_reviews": 0,
                "approved_reviews": 0,
                "change_requested": 0,
                "pending_reviews": 0,
            }

    def analyze_repository_prs(self, org: str, repo: str) -> List[Dict[str, Any]]:
        """
        Analyze all open PRs for a specific repository.

        Args:
            org: Repository org
            repo: Repository name

        Returns:
            List of dictionaries with PR analysis data
        """
        print(f"Fetching open pull requests from {org}/{repo}...")
        prs = self.get_open_prs(org, repo)
        print(f"Found {len(prs)} open pull requests in {org}/{repo}")

        analyzed_prs = []

        for i, pr in enumerate(prs, 1):
            print(f"  Analyzing PR {i}/{len(prs)}: #{pr['number']}")

            days, hours, total_hours = self.calculate_pr_age(pr["created_at"])
            reviews = self.get_pr_reviews(org, repo, pr["number"])

            # Determine status
            if reviews["approved_reviews"] > 0 and reviews["change_requested"] == 0:
                status = "Approved - Ready to Merge"
            elif reviews["change_requested"] > 0:
                status = "Changes Requested"
            elif reviews["total_reviews"] == 0:
                status = "No Reviews Yet"
            else:
                status = "Under Review"

            # Calculate urgency based on age
            if total_hours >= 168:  # 1 week
                urgency = "High"
            elif total_hours >= 72:  # 3 days
                urgency = "Medium"
            else:
                urgency = "Low"

            analyzed_pr = {
                "repo_org": org,
                "repo_name": repo,
                "pr_number": pr["number"],
                "title": pr["title"],
                "author": pr["user"]["login"],
                "created_at": pr["created_at"][:10],  # Just the date
                "age_days": days,
                "age_hours": hours,
                "total_hours": total_hours,
                "urgency": urgency,
                "status": status,
                "total_reviews": reviews["total_reviews"],
                "approved_reviews": reviews["approved_reviews"],
                "changes_requested": reviews["change_requested"],
                "additions": pr.get("additions", 0),
                "deletions": pr.get("deletions", 0),
                "changed_files": pr.get("changed_files", 0),
                "pr_url": pr["html_url"],
                "draft": pr["draft"],
            }

            analyzed_prs.append(analyzed_pr)

        return analyzed_prs

    def analyze_all_repositories(
        self, repositories: List[Dict[str, str]]
    ) -> List[Dict[str, Any]]:
        """
        Analyze open PRs across multiple repositories.

        Args:
            repositories: List of dicts with 'org' and 'repo' keys

        Returns:
            Combined list of analyzed PRs from all repositories
        """
        all_prs = []

        for repo_config in repositories:
            org = repo_config["org"]
            repo = repo_config["repo"]

            repo_prs = self.analyze_repository_prs(org, repo)
            all_prs.extend(repo_prs)

        # Sort by total hours (oldest first)
        all_prs.sort(key=lambda x: x["total_hours"], reverse=True)

        return all_prs

    def save_to_csv(
        self, prs: List[Dict[str, Any]], output_dir: str, filename: str = None
    ):
        """
        Save PR analysis to CSV file.

        Args:
            prs: List of analyzed PR dictionaries
            output_dir: Directory to save the CSV file
            filename: Optional custom filename (will generate timestamp-based name if None)
        """
        if not prs:
            print("No PRs to save.")
            return

        # Create output directory if it doesn't exist
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Generate filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"pr_analysis_{timestamp}.csv"

        filepath = Path(output_dir) / filename

        fieldnames = [
            "repo_org",
            "repo_name",
            "pr_number",
            "title",
            "author",
            "created_at",
            "age_days",
            "age_hours",
            "total_hours",
            "urgency",
            "status",
            "total_reviews",
            "approved_reviews",
            "changes_requested",
            "additions",
            "deletions",
            "changed_files",
            "draft",
            "pr_url",
        ]

        with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(prs)

        print(f"Analysis saved to {filepath}")
        return str(filepath)

    def print_summary(self, prs: List[Dict[str, Any]]):
        """
        Print a summary of the PR analysis across all repositories.

        Args:
            prs: List of analyzed PR dictionaries
        """
        if not prs:
            print("No open PRs found across all repositories.")
            return

        # Overall stats
        total_prs = len(prs)
        high_urgency = len([pr for pr in prs if pr["urgency"] == "High"])
        medium_urgency = len([pr for pr in prs if pr["urgency"] == "Medium"])
        no_reviews = len([pr for pr in prs if pr["total_reviews"] == 0])
        ready_to_merge = len(
            [pr for pr in prs if pr["status"] == "Approved - Ready to Merge"]
        )

        # Repository breakdown
        repo_stats = {}
        for pr in prs:
            repo_key = f"{pr['repo_org']}/{pr['repo_name']}"
            if repo_key not in repo_stats:
                repo_stats[repo_key] = {"count": 0, "high_urgency": 0, "no_reviews": 0}
            repo_stats[repo_key]["count"] += 1
            if pr["urgency"] == "High":
                repo_stats[repo_key]["high_urgency"] += 1
            if pr["total_reviews"] == 0:
                repo_stats[repo_key]["no_reviews"] += 1

        oldest_pr = prs[0] if prs else None

        print("\n" + "=" * 60)
        print("PULL REQUEST ANALYSIS SUMMARY")
        print("=" * 60)
        print(f"Total Open PRs: {total_prs}")
        print(f"High Urgency (>1 week): {high_urgency}")
        print(f"Medium Urgency (3-7 days): {medium_urgency}")
        print(f"PRs with no reviews: {no_reviews}")
        print(f"Ready to merge: {ready_to_merge}")

        print(f"\nBreakdown by Repository:")
        for repo, stats in sorted(repo_stats.items()):
            print(
                f"  {repo}: {stats['count']} PRs ({stats['high_urgency']} high urgency, {stats['no_reviews']} no reviews)"
            )

        if oldest_pr:
            print(
                f"\nOldest PR: #{oldest_pr['pr_number']} in {oldest_pr['repo_org']}/{oldest_pr['repo_name']} ({oldest_pr['age_days']} days old)"
            )
            print(f"Title: {oldest_pr['title']}")
            print(f"Author: {oldest_pr['author']}")

        print("\nTop 10 Oldest PRs:")
        for pr in prs[:10]:
            repo_short = f"{pr['repo_org']}/{pr['repo_name']}"
            print(
                f"  {repo_short} #{pr['pr_number']}: {pr['age_days']}d {pr['age_hours']}h - {pr['title'][:40]}..."
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
    required_keys = ["github_token", "repositories", "csv_directory"]

    for key in required_keys:
        if key not in config:
            print(f"Error: Missing required configuration key '{key}'")
            return False

    if not isinstance(config["repositories"], list):
        print("Error: 'repositories' must be a list")
        return False

    for i, repo in enumerate(config["repositories"]):
        if not isinstance(repo, dict):
            print(f"Error: Repository {i+1} must be a dictionary")
            return False
        if "org" not in repo or "repo" not in repo:
            print(f"Error: Repository {i+1} must have 'org' and 'repo' keys")
            return False

    return True


def create_sample_config(config_path: str):
    """
    Create a sample configuration file.

    Args:
        config_path: Path where to create the sample config
    """
    sample_config = {
        "github_token": "your_github_personal_access_token_here",
        "output_directory": "./pr_analysis_output",
        "repositories": [
            {"org": "your-org", "repo": "frontend-app"},
            {"org": "your-org", "repo": "backend-api"},
            {"org": "your-org", "repo": "mobile-app"},
        ],
    }

    with open(config_path, "w") as file:
        yaml.dump(sample_config, file, default_flow_style=False, sort_keys=False)

    print(f"Sample configuration created at '{config_path}'")
    print(
        "Please edit this file with your actual GitHub token and repository information."
    )


def main(config_path):
    """
    Main function to run the PR analysis.
    """
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
    github_token = config["github_token"]
    repositories = config["repositories"]
    output_directory = config["csv_directory"]
    custom_filename = config.get("output_filename")  # Optional

    if github_token == "your_github_personal_access_token_here":
        print("Error: Please update the github_token in the configuration file.")
        print("Create a personal access token at: https://github.com/settings/tokens")
        sys.exit(1)

    # Initialize analyzer
    analyzer = GitHubPRAnalyzer(github_token)

    # Analyze PRs across all repositories
    print(f"Analyzing PRs across {len(repositories)} repositories...")
    prs = analyzer.analyze_all_repositories(repositories)

    # Save results
    output_file = analyzer.save_to_csv(prs, output_directory, custom_filename)

    # Print summary
    analyzer.print_summary(prs)

    print(f"\nCSV file '{output_file}' is ready to upload to Google Sheets!")
    print("Consider sharing this with the team to discuss PR review processes.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze all open GitHub PRs")
    parser.add_argument("config", type=str, help="path to config file")
    args = parser.parse_args()
    config_file = args.config

    main(config_file)
