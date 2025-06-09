import requests
import json
import csv
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import time
from dateutil import parser as date_parser
import os
import logging
import yaml
import traceback

# Configure logging
logging.basicConfig(
    format="%(levelname)-10s %(asctime)s %(filename)s %(lineno)d %(message)s",
    level=logging.DEBUG,
)
logger = logging.getLogger(__name__)


def load_config(config_file: str = "config.yaml") -> Dict:
    """Load configuration from YAML file"""
    try:
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
        logger.info(f"Configuration loaded from {config_file}")
        return config
    except FileNotFoundError:
        logger.error(f"Configuration file {config_file} not found")
        logger.info("Please create a config.yaml file with:")
        logger.info("github_token: your_token_here")
        logger.info("repositories:")
        logger.info("  - org: your-org")
        logger.info("    repo: your-repo")
        raise


class GitHubMetricsExtractor:
    def __init__(self, token: str, org: str, repo: str):
        self.token = token
        self.org = org
        self.repo = repo
        self.base_url = "https://api.github.com"
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _make_request(self, url: str, params: Dict = None) -> requests.Response:
        """Make API request with rate limit handling"""
        response = self.session.get(url, params=params)

        # Handle rate limiting
        if response.status_code == 403 and "X-RateLimit-Remaining" in response.headers:
            if int(response.headers["X-RateLimit-Remaining"]) == 0:
                reset_time = int(response.headers["X-RateLimit-Reset"])
                sleep_time = reset_time - int(time.time()) + 10
                print(f"Rate limit hit. Sleeping for {sleep_time} seconds...")
                time.sleep(sleep_time)
                response = self.session.get(url, params=params)

        response.raise_for_status()
        return response

    def _paginated_request(
        self, url: str, params: Dict = None, max_pages: int = None
    ) -> List[Dict]:
        """Handle paginated API requests"""
        all_data = []
        page = 1

        while True:
            if max_pages and page > max_pages:
                break

            current_params = (params or {}).copy()
            current_params["page"] = page
            current_params["per_page"] = 100  # Max per page

            response = self._make_request(url, current_params)
            data = response.json()

            if not data:  # Empty response means we're done
                break

            all_data.extend(data)
            print(
                f"Fetched page {page}, got {len(data)} items (total: {len(all_data)})"
            )

            # Check if there's a next page
            link_header = response.headers.get("Link", "")
            if 'rel="next"' not in link_header:
                break

            page += 1

        return all_data

    def get_commits(
        self, since_date: datetime, until_date: datetime = None
    ) -> List[Dict]:
        """Get all commits in date range"""
        url = f"{self.base_url}/repos/{self.org}/{self.repo}/commits"
        params = {
            "since": since_date.isoformat(),
        }
        if until_date:
            params["until"] = until_date.isoformat()

        print(f"Fetching commits since {since_date.date()}...")
        return self._paginated_request(url, params)

    def _parse_github_date(self, date_string: str) -> datetime:
        """Safely parse GitHub's ISO 8601 date strings"""
        try:
            # Use dateutil parser which handles various ISO formats
            return date_parser.parse(date_string)
        except Exception:
            # Fallback for manual parsing if dateutil fails
            try:
                # Remove 'Z' and add timezone info
                if date_string.endswith("Z"):
                    date_string = date_string[:-1] + "+00:00"
                return datetime.fromisoformat(date_string)
            except Exception as e:
                print(f"Warning: Could not parse date '{date_string}': {e}")
                return datetime.now()  # Fallback to now

    def get_pull_requests(self, since_date: datetime, state: str = "all") -> List[Dict]:
        """Get pull requests (for merge analysis)"""
        url = f"{self.base_url}/repos/{self.org}/{self.repo}/pulls"
        params = {"state": state, "sort": "updated", "direction": "desc"}

        print(f"Fetching pull requests...")
        all_prs = self._paginated_request(url, params)

        # Filter by date since GitHub doesn't support since parameter for PRs
        filtered_prs = []
        for pr in all_prs:
            try:
                updated_at = self._parse_github_date(pr["updated_at"])
                # Make since_date timezone-aware if it isn't already
                if since_date.tzinfo is None:
                    since_date = since_date.replace(tzinfo=updated_at.tzinfo)

                if updated_at >= since_date:
                    filtered_prs.append(pr)
                else:
                    # Since we're sorting by updated desc, we can break here
                    # But let's be more conservative and check a few more in case of date issues
                    if len(
                        [
                            p
                            for p in all_prs[:50]
                            if self._parse_github_date(p["updated_at"]) >= since_date
                        ]
                    ) == len(filtered_prs):
                        break
            except Exception as e:
                print(f"Warning: Skipping PR due to date parsing error: {e}")
                continue

        return filtered_prs

    def get_workflow_runs(self, since_date: datetime) -> List[Dict]:
        """Get GitHub Actions workflow runs (for deployment tracking)"""
        url = f"{self.base_url}/repos/{self.org}/{self.repo}/actions/runs"
        params = {"created": f">={since_date.strftime('%Y-%m-%d')}"}

        print(f"Fetching workflow runs since {since_date.date()}...")
        return self._paginated_request(url, params)

    def get_deployments(self, since_date: datetime) -> List[Dict]:
        """Get deployment data"""
        url = f"{self.base_url}/repos/{self.org}/{self.repo}/deployments"

        print(f"Fetching deployments...")
        all_deployments = self._paginated_request(url)

        # Filter by date
        filtered_deployments = []
        for deployment in all_deployments:
            try:
                created_at = self._parse_github_date(deployment["created_at"])
                # Make since_date timezone-aware if it isn't already
                if since_date.tzinfo is None:
                    since_date = since_date.replace(tzinfo=created_at.tzinfo)

                if created_at >= since_date:
                    filtered_deployments.append(deployment)
            except Exception as e:
                print(f"Warning: Skipping deployment due to date parsing error: {e}")
                continue

        return filtered_deployments

    def get_deployment_statuses(self, deployment_id: int) -> List[Dict]:
        """Get statuses for a specific deployment"""
        url = f"{self.base_url}/repos/{self.org}/{self.repo}/deployments/{deployment_id}/statuses"
        return self._paginated_request(url)

    def get_releases(self, since_date: datetime) -> List[Dict]:
        """Get release data"""
        url = f"{self.base_url}/repos/{self.org}/{self.repo}/releases"

        print(f"Fetching releases...")
        all_releases = self._paginated_request(url)

        # Filter by date
        filtered_releases = []
        for release in all_releases:
            try:
                created_at = self._parse_github_date(release["created_at"])
                # Make since_date timezone-aware if it isn't already
                if since_date.tzinfo is None:
                    since_date = since_date.replace(tzinfo=created_at.tzinfo)

                if created_at >= since_date:
                    filtered_releases.append(release)
            except Exception as e:
                print(f"Warning: Skipping release due to date parsing error: {e}")
                continue

        return filtered_releases

    def collect_all_metrics(self, months_back: int = 6) -> Dict:
        """Collect all metrics for the specified time period"""
        since_date = datetime.now() - timedelta(days=months_back * 30)

        print(f"Collecting GitHub metrics for {self.org}/{self.repo}")
        print(f"Date range: {since_date.date()} to {datetime.now().date()}")

        metrics = {
            "collection_date": datetime.now().isoformat(),
            "repo": f"{self.org}/{self.repo}",
            "date_range": {
                "since": since_date.isoformat(),
                "until": datetime.now().isoformat(),
            },
        }

        # Collect commits
        metrics["commits"] = self.get_commits(since_date)

        # Collect pull requests
        metrics["pull_requests"] = self.get_pull_requests(since_date)

        # Collect workflow runs (GitHub Actions)
        metrics["workflow_runs"] = self.get_workflow_runs(since_date)

        # Collect deployments
        metrics["deployments"] = self.get_deployments(since_date)

        # Get deployment statuses for each deployment
        print("Fetching deployment statuses...")
        for deployment in metrics["deployments"]:
            deployment["statuses"] = self.get_deployment_statuses(deployment["id"])

        # Collect releases
        metrics["releases"] = self.get_releases(since_date)

        return metrics

    def export_to_csv(
        self, metrics: Dict, output_dir: str = "github_metrics"
    ) -> Dict[str, str]:
        """Export metrics to CSV files for Pandas analysis"""
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        repo_safe_name = f"{self.org}_{self.repo}".replace("/", "_")

        csv_files = {}

        # Export commits
        if metrics.get("commits"):
            commits_file = f"{output_dir}/commits_{repo_safe_name}_{timestamp}.csv"
            with open(commits_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "repo",
                        "sha",
                        "author_name",
                        "author_email",
                        "author_date",
                        "committer_name",
                        "committer_email",
                        "committer_date",
                        "message",
                        "additions",
                        "deletions",
                        "total_changes",
                    ]
                )

                for commit in metrics["commits"]:
                    try:
                        commit_data = commit["commit"]
                        stats = commit.get("stats", {})
                        writer.writerow(
                            [
                                f"{self.org}/{self.repo}",
                                commit["sha"],
                                commit_data["author"]["name"],
                                commit_data["author"]["email"],
                                commit_data["author"]["date"],
                                commit_data["committer"]["name"],
                                commit_data["committer"]["email"],
                                commit_data["committer"]["date"],
                                commit_data["message"]
                                .replace("\n", " ")
                                .replace("\r", " ")[:500],  # Truncate long messages
                                stats.get("additions", 0),
                                stats.get("deletions", 0),
                                stats.get("total", 0),
                            ]
                        )
                    except Exception as e:
                        print(f"Warning: Skipping commit due to error: {e}")
            csv_files["commits"] = commits_file

        # Export pull requests
        if metrics.get("pull_requests"):
            prs_file = f"{output_dir}/pull_requests_{repo_safe_name}_{timestamp}.csv"
            with open(prs_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "repo",
                        "pr_number",
                        "title",
                        "state",
                        "author",
                        "created_at",
                        "updated_at",
                        "closed_at",
                        "merged_at",
                        "merge_commit_sha",
                        "additions",
                        "deletions",
                        "changed_files",
                        "commits_count",
                    ]
                )

                for pr in metrics["pull_requests"]:
                    try:
                        writer.writerow(
                            [
                                f"{self.org}/{self.repo}",
                                pr["number"],
                                pr["title"].replace("\n", " ").replace("\r", " ")[:200],
                                pr["state"],
                                pr["user"]["login"] if pr["user"] else "unknown",
                                pr["created_at"],
                                pr["updated_at"],
                                pr.get("closed_at", ""),
                                pr.get("merged_at", ""),
                                pr.get("merge_commit_sha", ""),
                                pr.get("additions", 0),
                                pr.get("deletions", 0),
                                pr.get("changed_files", 0),
                                pr.get("commits", 0),
                            ]
                        )
                    except Exception as e:
                        print(f"Warning: Skipping PR due to error: {e}")
            csv_files["pull_requests"] = prs_file

        # Export workflow runs
        if metrics.get("workflow_runs"):
            workflows_file = (
                f"{output_dir}/workflow_runs_{repo_safe_name}_{timestamp}.csv"
            )
            with open(workflows_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "repo",
                        "run_id",
                        "name",
                        "status",
                        "conclusion",
                        "workflow_id",
                        "created_at",
                        "updated_at",
                        "run_started_at",
                        "head_sha",
                        "head_branch",
                        "event",
                        "actor",
                        "run_attempt",
                    ]
                )

                for run in metrics["workflow_runs"]:
                    try:
                        writer.writerow(
                            [
                                f"{self.org}/{self.repo}",
                                run["id"],
                                run["name"],
                                run["status"],
                                run.get("conclusion", ""),
                                run["workflow_id"],
                                run["created_at"],
                                run["updated_at"],
                                run.get("run_started_at", ""),
                                run["head_sha"],
                                run["head_branch"],
                                run["event"],
                                (
                                    run["actor"]["login"]
                                    if run.get("actor")
                                    else "unknown"
                                ),
                                run.get("run_attempt", 1),
                            ]
                        )
                    except Exception as e:
                        print(f"Warning: Skipping workflow run due to error: {e}")
            csv_files["workflow_runs"] = workflows_file

        # Export deployments
        if metrics.get("deployments"):
            deployments_file = (
                f"{output_dir}/deployments_{repo_safe_name}_{timestamp}.csv"
            )
            with open(deployments_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "repo",
                        "deployment_id",
                        "sha",
                        "ref",
                        "environment",
                        "created_at",
                        "updated_at",
                        "creator",
                        "description",
                        "status",
                        "status_created_at",
                        "status_description",
                    ]
                )

                for deployment in metrics["deployments"]:
                    try:
                        # Get the latest status
                        statuses = deployment.get("statuses", [])
                        latest_status = statuses[0] if statuses else {}

                        writer.writerow(
                            [
                                f"{self.org}/{self.repo}",
                                deployment["id"],
                                deployment["sha"],
                                deployment["ref"],
                                deployment.get("environment", ""),
                                deployment["created_at"],
                                deployment["updated_at"],
                                (
                                    deployment["creator"]["login"]
                                    if deployment.get("creator")
                                    else "unknown"
                                ),
                                deployment.get("description", "")[:200],
                                latest_status.get("state", ""),
                                latest_status.get("created_at", ""),
                                latest_status.get("description", "")[:200],
                            ]
                        )
                    except Exception as e:
                        print(f"Warning: Skipping deployment due to error: {e}")
            csv_files["deployments"] = deployments_file

        # Export releases
        if metrics.get("releases"):
            releases_file = f"{output_dir}/releases_{repo_safe_name}_{timestamp}.csv"
            with open(releases_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "repo",
                        "release_id",
                        "tag_name",
                        "name",
                        "draft",
                        "prerelease",
                        "created_at",
                        "published_at",
                        "author",
                        "body",
                        "target_commitish",
                    ]
                )

                for release in metrics["releases"]:
                    try:
                        writer.writerow(
                            [
                                f"{self.org}/{self.repo}",
                                release["id"],
                                release["tag_name"],
                                release.get("name", "")[:200],
                                release["draft"],
                                release["prerelease"],
                                release["created_at"],
                                release.get("published_at", ""),
                                (
                                    release["author"]["login"]
                                    if release.get("author")
                                    else "unknown"
                                ),
                                release.get("body", "")[:500]
                                .replace("\n", " ")
                                .replace("\r", " "),
                                release.get("target_commitish", ""),
                            ]
                        )
                    except Exception as e:
                        print(f"Warning: Skipping release due to error: {e}")
            csv_files["releases"] = releases_file

        print(f"CSV files exported to {output_dir}/")
        for data_type, filename in csv_files.items():
            print(f"  {data_type}: {filename}")

        logger.info(f"CSV files exported to {output_dir}/")
        for data_type, filename in csv_files.items():
            logger.info(f"  {data_type}: {filename}")

        return csv_files


class MultiRepoMetricsCollector:
    def __init__(self, token: str):
        self.token = token
        self.results = []

    def collect_from_repos(
        self, repo_configs: List[Dict], months_back: int = 6
    ) -> List[Dict]:
        """
        Collect metrics from multiple repositories

        repo_configs: List of {"org": "org_name", "repo": "repo_name"} dicts
        """
        logger.info(f"Starting collection from {len(repo_configs)} repositories...")

        for i, config in enumerate(repo_configs, 1):
            logger.info(
                f"Processing repository {i}/{len(repo_configs)}: {config['org']}/{config['repo']}"
            )

            try:
                extractor = GitHubMetricsExtractor(
                    self.token, config["org"], config["repo"]
                )
                metrics = extractor.collect_all_metrics(months_back)

                # Add repository info to metrics
                metrics["repo_config"] = config
                self.results.append(metrics)

                # Export individual repo CSV files
                csv_files = extractor.export_to_csv(metrics)

                logger.info(f"✓ Completed {config['org']}/{config['repo']}")

                # Small delay to be nice to GitHub's API
                time.sleep(1)

            except Exception as e:
                logger.error(
                    f"✗ Failed to process {config['org']}/{config['repo']}: {e}"
                )
                logger.error(f"Stack trace: {traceback.format_exc()}")
                continue

        return self.results

    def create_summary_csvs(self, output_dir: str = "github_metrics") -> Dict[str, str]:
        """Create combined CSV files across all repositories"""
        if not self.results:
            print("No results to summarize")
            return {}

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_files = {}

        # Combined commits across all repos
        commits_file = f"{output_dir}/summary_commits_{timestamp}.csv"
        with open(commits_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "repo",
                    "sha",
                    "author_name",
                    "author_email",
                    "author_date",
                    "committer_name",
                    "committer_email",
                    "committer_date",
                    "message",
                    "additions",
                    "deletions",
                    "total_changes",
                ]
            )

            for result in self.results:
                repo_name = (
                    f"{result['repo_config']['org']}/{result['repo_config']['repo']}"
                )
                for commit in result.get("commits", []):
                    try:
                        commit_data = commit["commit"]
                        stats = commit.get("stats", {})
                        writer.writerow(
                            [
                                repo_name,
                                commit["sha"],
                                commit_data["author"]["name"],
                                commit_data["author"]["email"],
                                commit_data["author"]["date"],
                                commit_data["committer"]["name"],
                                commit_data["committer"]["email"],
                                commit_data["committer"]["date"],
                                commit_data["message"]
                                .replace("\n", " ")
                                .replace("\r", " ")[:500],
                                stats.get("additions", 0),
                                stats.get("deletions", 0),
                                stats.get("total", 0),
                            ]
                        )
                    except Exception as e:
                        print(f"Warning: Skipping commit in summary: {e}")
        summary_files["commits"] = commits_file

        # Combined deployments across all repos
        deployments_file = f"{output_dir}/summary_deployments_{timestamp}.csv"
        with open(deployments_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "repo",
                    "deployment_id",
                    "sha",
                    "ref",
                    "environment",
                    "created_at",
                    "updated_at",
                    "creator",
                    "description",
                    "status",
                    "status_created_at",
                    "status_description",
                ]
            )

            for result in self.results:
                repo_name = (
                    f"{result['repo_config']['org']}/{result['repo_config']['repo']}"
                )
                for deployment in result.get("deployments", []):
                    try:
                        statuses = deployment.get("statuses", [])
                        latest_status = statuses[0] if statuses else {}

                        writer.writerow(
                            [
                                repo_name,
                                deployment["id"],
                                deployment["sha"],
                                deployment["ref"],
                                deployment.get("environment", ""),
                                deployment["created_at"],
                                deployment["updated_at"],
                                (
                                    deployment["creator"]["login"]
                                    if deployment.get("creator")
                                    else "unknown"
                                ),
                                deployment.get("description", "")[:200],
                                latest_status.get("state", ""),
                                latest_status.get("created_at", ""),
                                latest_status.get("description", "")[:200],
                            ]
                        )
                    except Exception as e:
                        print(f"Warning: Skipping deployment in summary: {e}")
        summary_files["deployments"] = deployments_file

        # Combined releases across all repos
        releases_file = f"{output_dir}/summary_releases_{timestamp}.csv"
        with open(releases_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "repo",
                    "release_id",
                    "tag_name",
                    "name",
                    "draft",
                    "prerelease",
                    "created_at",
                    "published_at",
                    "author",
                    "body",
                    "target_commitish",
                ]
            )

            for result in self.results:
                repo_name = (
                    f"{result['repo_config']['org']}/{result['repo_config']['repo']}"
                )
                for release in result.get("releases", []):
                    try:
                        writer.writerow(
                            [
                                repo_name,
                                release["id"],
                                release["tag_name"],
                                release.get("name", "")[:200],
                                release["draft"],
                                release["prerelease"],
                                release["created_at"],
                                release.get("published_at", ""),
                                (
                                    release["author"]["login"]
                                    if release.get("author")
                                    else "unknown"
                                ),
                                release.get("body", "")[:500]
                                .replace("\n", " ")
                                .replace("\r", " "),
                                release.get("target_commitish", ""),
                            ]
                        )
                    except Exception as e:
                        print(f"Warning: Skipping release in summary: {e}")
        summary_files["releases"] = releases_file

        # Repository summary statistics
        repo_summary_file = f"{output_dir}/repository_summary_{timestamp}.csv"
        with open(repo_summary_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "repo",
                    "total_commits",
                    "total_prs",
                    "total_workflow_runs",
                    "total_releases",
                    "total_deployments",
                    "unique_authors",
                    "date_range_start",
                    "date_range_end",
                ]
            )

            for result in self.results:
                repo_name = (
                    f"{result['repo_config']['org']}/{result['repo_config']['repo']}"
                )

                # Calculate unique authors
                authors = set()
                for commit in result.get("commits", []):
                    try:
                        authors.add(commit["commit"]["author"]["name"])
                    except:
                        pass

                writer.writerow(
                    [
                        repo_name,
                        len(result.get("commits", [])),
                        len(result.get("pull_requests", [])),
                        len(result.get("workflow_runs", [])),
                        len(result.get("releases", [])),
                        len(result.get("deployments", [])),
                        len(authors),
                        result["date_range"]["since"],
                        result["date_range"]["until"],
                    ]
                )
        summary_files["repository_summary"] = repo_summary_file

        logger.info(f"Summary CSV files created:")
        for data_type, filename in summary_files.items():
            logger.info(f"  {data_type}: {filename}")

        return summary_files

    def analyze_repository_metrics(self, metrics: Dict) -> Dict:
        """Analyze metrics for a single repository including lead times"""
        analysis = {
            "repo": metrics["repo"],
            "basic_stats": {},
            "deployment_analysis": {},
            "release_analysis": {},
            "contributor_analysis": {},
        }

        commits = metrics.get("commits", [])
        deployments = metrics.get("deployments", [])
        releases = metrics.get("releases", [])

        # Basic statistics
        analysis["basic_stats"] = {
            "total_commits": len(commits),
            "total_deployments": len(deployments),
            "total_releases": len(releases),
            "total_prs": len(metrics.get("pull_requests", [])),
            "total_workflow_runs": len(metrics.get("workflow_runs", [])),
        }

        # Analyze deployment frequency
        if deployments:
            deployment_dates = []
            for d in deployments:
                try:
                    date_obj = self._parse_github_date(d["created_at"])
                    deployment_dates.append(date_obj)
                except Exception as e:
                    logger.warning(f"Could not parse deployment date: {e}")
                    continue

            deployment_dates.sort()

            # Calculate days between deployments
            intervals = []
            for i in range(1, len(deployment_dates)):
                interval = (deployment_dates[i] - deployment_dates[i - 1]).days
                intervals.append(interval)

            if intervals:
                analysis["deployment_analysis"] = {
                    "avg_days_between_deployments": sum(intervals) / len(intervals),
                    "min_days_between_deployments": min(intervals),
                    "max_days_between_deployments": max(intervals),
                    "deployments_per_month": len(deployments)
                    / 6,  # Assuming 6 months of data
                }

        # Analyze release frequency
        if releases:
            release_dates = []
            for r in releases:
                try:
                    date_obj = self._parse_github_date(r["created_at"])
                    release_dates.append(date_obj)
                except Exception as e:
                    logger.warning(f"Could not parse release date: {e}")
                    continue

            release_dates.sort()

            # Calculate days between releases
            intervals = []
            for i in range(1, len(release_dates)):
                interval = (release_dates[i] - release_dates[i - 1]).days
                intervals.append(interval)

            if intervals:
                analysis["release_analysis"] = {
                    "avg_days_between_releases": sum(intervals) / len(intervals),
                    "min_days_between_releases": min(intervals),
                    "max_days_between_releases": max(intervals),
                    "releases_per_month": len(releases)
                    / 6,  # Assuming 6 months of data
                }

        # Analyze commit patterns and contributors
        if commits:
            commit_authors = {}
            for commit in commits:
                author = commit["commit"]["author"]["name"]
                commit_authors[author] = commit_authors.get(author, 0) + 1

            sorted_authors = sorted(
                commit_authors.items(), key=lambda x: x[1], reverse=True
            )

            analysis["contributor_analysis"] = {
                "total_contributors": len(commit_authors),
                "top_contributors": dict(sorted_authors[:10]),
                "commits_per_contributor": (
                    sum(commit_authors.values()) / len(commit_authors)
                    if commit_authors
                    else 0
                ),
            }

        return analysis

    def _parse_github_date(self, date_string: str) -> datetime:
        """Safely parse GitHub's ISO 8601 date strings"""
        try:
            return date_parser.parse(date_string)
        except Exception:
            try:
                if date_string.endswith("Z"):
                    date_string = date_string[:-1] + "+00:00"
                return datetime.fromisoformat(date_string)
            except Exception as e:
                logger.warning(f"Could not parse date '{date_string}': {e}")
                return datetime.now()

    def print_overall_summary(self):
        """Print high-level statistics across all repositories"""
        if not self.results:
            logger.warning("No results to summarize")
            return

        logger.info("=" * 60)
        logger.info(f"OVERALL SUMMARY - {len(self.results)} Repositories")
        logger.info("=" * 60)

        total_commits = sum(len(result.get("commits", [])) for result in self.results)
        total_releases = sum(len(result.get("releases", [])) for result in self.results)
        total_prs = sum(len(result.get("pull_requests", [])) for result in self.results)
        total_workflows = sum(
            len(result.get("workflow_runs", [])) for result in self.results
        )
        total_deployments = sum(
            len(result.get("deployments", [])) for result in self.results
        )

        # Collect all unique authors across repos
        all_authors = set()
        for result in self.results:
            for commit in result.get("commits", []):
                try:
                    all_authors.add(commit["commit"]["author"]["name"])
                except (KeyError, TypeError):
                    pass

        logger.info(f"Total Commits: {total_commits:,}")
        logger.info(f"Total Releases: {total_releases:,}")
        logger.info(f"Total Pull Requests: {total_prs:,}")
        logger.info(f"Total Workflow Runs: {total_workflows:,}")
        logger.info(f"Total Deployments: {total_deployments:,}")
        logger.info(f"Unique Contributors: {len(all_authors)}")

        logger.info("\nPer Repository Breakdown:")
        for result in self.results:
            repo_name = (
                f"{result['repo_config']['org']}/{result['repo_config']['repo']}"
            )
            commits = len(result.get("commits", []))
            releases = len(result.get("releases", []))
            deployments = len(result.get("deployments", []))
            logger.info(
                f"  {repo_name}: {commits} commits, {releases} releases, {deployments} deployments"
            )

        # Analyze each repository
        logger.info("\nDetailed Repository Analysis:")
        for result in self.results:
            analysis = self.analyze_repository_metrics(result)
            repo_name = analysis["repo"]
            logger.info(f"\n  {repo_name}:")

            if analysis["deployment_analysis"]:
                logger.info(
                    f"    Avg days between deployments: {analysis['deployment_analysis']['avg_days_between_deployments']:.1f}"
                )
                logger.info(
                    f"    Deployments per month: {analysis['deployment_analysis']['deployments_per_month']:.1f}"
                )

            if analysis["release_analysis"]:
                logger.info(
                    f"    Avg days between releases: {analysis['release_analysis']['avg_days_between_releases']:.1f}"
                )
                logger.info(
                    f"    Releases per month: {analysis['release_analysis']['releases_per_month']:.1f}"
                )

            if analysis["contributor_analysis"]:
                logger.info(
                    f"    Total contributors: {analysis['contributor_analysis']['total_contributors']}"
                )
                logger.info(
                    f"    Top contributor: {list(analysis['contributor_analysis']['top_contributors'].items())[0] if analysis['contributor_analysis']['top_contributors'] else 'N/A'}"
                )


# Example usage
if __name__ == "__main__":
    try:
        # Load configuration from YAML file
        config = load_config("config.yaml")

        github_token = config.get("github_token")
        csv_directory = config.get("csv_directory")
        if not github_token:
            logger.error("github_token not found in config.yaml")
            raise ValueError("github_token is required in config.yaml")

        # Get repositories from config, with fallback to examples
        repositories = config.get(
            "repositories",
            [
                {"org": "facebook", "repo": "react"},
                {"org": "microsoft", "repo": "vscode"},
            ],
        )

        logger.info(f"Starting analysis of {len(repositories)} repositories...")
        logger.info("Repositories to analyze:")
        for i, repo_config in enumerate(repositories, 1):
            logger.info(f"  {i}. {repo_config['org']}/{repo_config['repo']}")

        collector = MultiRepoMetricsCollector(github_token)
        results = collector.collect_from_repos(repositories, months_back=6)

        if results:
            logger.info(
                f"✅ Successfully collected data from {len(results)} repositories"
            )

            # Create summary CSV files
            summary_files = collector.create_summary_csvs(csv_directory)

            # Print overall statistics
            collector.print_overall_summary()

            logger.info("=" * 60)
            logger.info("🐼 PANDAS USAGE EXAMPLES:")
            logger.info("=" * 60)
            logger.info("# Load the summary data into pandas:")
            logger.info("import pandas as pd")
            logger.info("")
            logger.info("# Load commits across all repos")
            logger.info(
                f"commits_df = pd.read_csv('{summary_files.get('commits', 'summary_commits.csv')}')"
            )
            logger.info(
                "commits_df['author_date'] = pd.to_datetime(commits_df['author_date'])"
            )
            logger.info("")
            logger.info("# Load releases across all repos")
            logger.info(
                f"releases_df = pd.read_csv('{summary_files.get('releases', 'summary_releases.csv')}')"
            )
            logger.info(
                "releases_df['created_at'] = pd.to_datetime(releases_df['created_at'])"
            )
            logger.info("")
            logger.info("# Load repository summary")
            logger.info(
                f"repo_summary_df = pd.read_csv('{summary_files.get('repository_summary', 'repository_summary.csv')}')"
            )

        else:
            logger.error("❌ No data collected. Please check:")
            logger.error("1. Your GitHub token has the right permissions")
            logger.error("2. Repository org/repo names are correct")
            logger.error("3. You have access to the specified repositories")

    except Exception as e:
        logger.error(f"❌ Error during execution: {e}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        logger.info("\nCreate a config.yaml file with:")
        logger.info("github_token: your_token_here")
        logger.info("repositories:")
        logger.info("  - org: your-org")
        logger.info("    repo: your-repo")
