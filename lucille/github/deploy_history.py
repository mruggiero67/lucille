#!/usr/bin/env python3
"""
GitHub-based deployment history — replaces the manual Slack scrape pipeline.

Fetches all GitHub releases for a set of repos since a start date, writes a CSV
in the same schema as slack_deploys.py output, and produces a weekly bar graph
and ASCII summary using the same functions as weekly_deployment_trends.py.

Activate venv before running:
  source ~/venv/basic-pandas/bin/activate

Usage:
  # Default: deploy_history.repos from jira_epic_config.yaml, since 2025-05-12
  python deploy_history.py

  # Override repos or date
  python deploy_history.py --since 2025-01-01
  python deploy_history.py --repos analytics auth public-api

  # Dry run (prints table, no files written)
  python deploy_history.py --dry-run

Output layout (configurable via CLI or jira_epic_config.yaml):
  CSV  → cfr.output_directory              (default: ~/Desktop/debris)
  PNG  → deploy_history.graph_output_directory  (default: ~/Desktop/debris/2x2/deployments)
  TXT  → deploy_history.graph_output_directory  (same folder as PNG)

Config loaded from (no new YAML files):
  ~/bin/github_config.yaml      — github_token, org, repositories
  ~/bin/jira_epic_config.yaml   — deploy_history.repos / graph_output_directory,
                                   cfr.output_directory
"""

import argparse
import csv
import json
import logging
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml
import pandas as pd

# Reuse the pure charting functions from the existing weekly_deployment_trends module
try:
    from lucille.weekly_deployment_trends import (
        calculate_weekly_deployments,
        calculate_statistics,
        create_weekly_trend_graph,
        create_summary_report,
    )
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from lucille.weekly_deployment_trends import (
        calculate_weekly_deployments,
        calculate_statistics,
        create_weekly_trend_graph,
        create_summary_report,
    )

logging.basicConfig(
    format="%(levelname)-8s %(asctime)s %(filename)s:%(lineno)d %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

DEFAULT_SINCE = "2025-05-12"
DEFAULT_GITHUB_CONFIG = Path.home() / "bin" / "github_config.yaml"
DEFAULT_JIRA_EPIC_CONFIG = Path.home() / "bin" / "jira_epic_config.yaml"

# CSV columns — identical to slack_deploys.py output for downstream compatibility
CSV_COLUMNS = ["date", "time", "user", "service", "version", "timestamp", "raw_message"]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config(
    github_config_path: Path = DEFAULT_GITHUB_CONFIG,
    jira_epic_config_path: Path = DEFAULT_JIRA_EPIC_CONFIG,
) -> Dict[str, Any]:
    def _load(p: Path) -> Dict:
        try:
            with open(p) as f:
                return yaml.safe_load(f) or {}
        except FileNotFoundError:
            logger.error(f"Config file not found: {p}")
            sys.exit(1)

    gh = _load(github_config_path)
    epic = _load(jira_epic_config_path)

    token = gh.get("github_token")
    org = gh.get("org")
    if not token or not org:
        logger.error("github_token and org are required in github_config.yaml")
        sys.exit(1)

    all_repos = [r["repo"] for r in gh.get("repositories", [])]
    deploy_history_cfg = epic.get("deploy_history", {})
    cfr = epic.get("cfr", {})
    # deploy_history.repos is the authoritative full production service list.
    # cfr.scoped_repos is a narrower subset used only for CFR analysis.
    deploy_repos = deploy_history_cfg.get("repos") or []
    output_dir = cfr.get("output_directory", str(Path.home() / "Desktop" / "debris"))
    graph_output_dir = deploy_history_cfg.get(
        "graph_output_directory",
        str(Path.home() / "Desktop" / "debris" / "2x2" / "deployments"),
    )

    return {
        "token": token,
        "org": org,
        "all_repos": all_repos,
        "deploy_repos": deploy_repos,
        "output_dir": Path(output_dir),
        "graph_output_dir": Path(graph_output_dir),
    }


# ---------------------------------------------------------------------------
# GitHub fetching (self-contained — no cfr dependency)
# ---------------------------------------------------------------------------

class _GitHubReleases:
    """Minimal GitHub releases fetcher with caching and rate-limit backoff."""

    BASE = "https://api.github.com"

    def __init__(self, token: str, org: str, cache_dir: Optional[Path] = None):
        self.org = org
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }
        self.cache_dir = cache_dir or (Path.home() / "Desktop" / "debris" / "cfr_cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_key(self, repo: str) -> Path:
        return self.cache_dir / f"releases__{self.org}__{repo}.json"

    def _get(self, url: str, params: Dict) -> List[Any]:
        results, page = [], 1
        while True:
            params["page"] = page
            params["per_page"] = 100
            for attempt in range(5):
                try:
                    resp = requests.get(url, headers=self.headers, params=params, timeout=30)
                    if resp.status_code == 403 and "rate limit" in resp.text.lower():
                        wait = 2 ** attempt * 30
                        logger.warning(f"Rate limited — sleeping {wait}s")
                        time.sleep(wait)
                        continue
                    resp.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    if attempt == 4:
                        raise
                    time.sleep(2 ** attempt)
            page_data = resp.json()
            if not page_data:
                break
            results.extend(page_data)
            if len(page_data) < 100:
                break
            page += 1
        return results

    def fetch(self, repo: str, since: datetime) -> List[Dict]:
        """Return all releases for `repo` published on or after `since`."""
        url = f"{self.BASE}/repos/{self.org}/{repo}/releases"
        raw = self._get(url, {})
        releases = []
        for r in raw:
            if not r.get("published_at"):
                continue
            published = datetime.fromisoformat(r["published_at"].replace("Z", "+00:00"))
            if published < since:
                continue
            releases.append(
                {
                    "repo": repo,
                    "tag": r["tag_name"],
                    "published_at": published,
                    "author": (r.get("author") or {}).get("login", "github"),
                    "name": r.get("name") or r["tag_name"],
                    "url": r["html_url"],
                }
            )
        return releases


# ---------------------------------------------------------------------------
# Transform to rows
# ---------------------------------------------------------------------------

def releases_to_rows(releases: List[Dict]) -> List[Dict[str, str]]:
    """Convert release dicts to CSV row dicts matching slack_deploys.py schema."""
    rows = []
    for r in releases:
        dt: datetime = r["published_at"]
        date_str = dt.strftime("%Y-%m-%d")
        time_str = dt.strftime("%-I:%M %p")   # e.g. "9:04 AM"
        rows.append(
            {
                "date": date_str,
                "time": time_str,
                "user": r["author"],
                "service": r["repo"],
                "version": r["tag"],
                "timestamp": f"{date_str} {time_str}",
                "raw_message": f"{r['repo']} {r['tag']} released — {r['url']}",
            }
        )
    # Sort chronologically
    rows.sort(key=lambda r: r["timestamp"])
    return rows


# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

def write_csv(rows: List[Dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"CSV written: {path}  ({len(rows)} rows)")


def write_graph_and_summary(
    rows: List[Dict], graph_output_dir: Path, since: datetime
) -> None:
    """
    Produce a weekly deployment bar graph (PNG) and ASCII summary (TXT),
    both written to graph_output_dir.
    Reuses create_weekly_trend_graph and create_summary_report from
    weekly_deployment_trends.py.
    """
    graph_output_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    weekly = calculate_weekly_deployments(df, date_column="date")
    stats = calculate_statistics(weekly)

    timestamp = datetime.now().strftime("%Y_%m_%d")
    since_label = since.strftime("%Y-%m-%d")
    title = f"Weekly Deployments (GitHub releases since {since_label})"

    graph_path = graph_output_dir / f"{timestamp}_github_deploy_history.png"
    create_weekly_trend_graph(weekly, graph_path, title=title)

    summary_path = graph_output_dir / f"{timestamp}_github_deploy_history_summary.txt"
    create_summary_report(weekly, stats, summary_path)

    logger.info(
        f"Graph:   {graph_path}\n"
        f"Summary: {summary_path}\n"
        f"  Weeks: {stats['total_weeks']}  |  "
        f"Total: {stats['total_deployments']}  |  "
        f"Avg/week: {stats['average_per_week']:.1f}"
    )


def print_table(rows: List[Dict]) -> None:
    """Quick console preview."""
    if not rows:
        print("No releases found.")
        return
    # Group by week for a compact summary
    weekly: Dict[str, int] = defaultdict(int)
    for r in rows:
        dt = datetime.strptime(r["date"], "%Y-%m-%d")
        week_start = dt - __import__("datetime").timedelta(days=dt.weekday())
        weekly[week_start.strftime("%Y-%m-%d")] += 1

    print(f"\n{'Week starting':<15}  {'Deployments':>11}")
    print("-" * 30)
    for week in sorted(weekly):
        print(f"{week:<15}  {weekly[week]:>11}")
    print(f"\nTotal: {len(rows)} releases across {len(weekly)} weeks")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch GitHub release history and produce deployment CSV + bar graph."
    )
    p.add_argument(
        "--since",
        default=DEFAULT_SINCE,
        metavar="YYYY-MM-DD",
        help=f"Fetch releases on or after this date (default: {DEFAULT_SINCE})",
    )
    p.add_argument(
        "--repos",
        nargs="+",
        default=None,
        metavar="REPO",
        help="Repos to scan (default: cfr.scoped_repos from jira_epic_config.yaml)",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for CSV (default: cfr.output_directory from jira_epic_config.yaml)",
    )
    p.add_argument(
        "--graph-output-dir",
        type=Path,
        default=None,
        help="Directory for PNG + TXT summary (default: deploy_history.graph_output_directory)",
    )
    p.add_argument(
        "--github-config",
        type=Path,
        default=DEFAULT_GITHUB_CONFIG,
        help=f"Path to github_config.yaml (default: {DEFAULT_GITHUB_CONFIG})",
    )
    p.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_JIRA_EPIC_CONFIG,
        help=f"Path to jira_epic_config.yaml (default: {DEFAULT_JIRA_EPIC_CONFIG})",
    )
    p.add_argument(
        "--no-cache",
        action="store_true",
        help="Bypass local cache; always re-fetch from GitHub",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print weekly summary table; do not write files",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    cfg = load_config(args.github_config, args.config)
    output_dir = args.output_dir or cfg["output_dir"]
    graph_output_dir = args.graph_output_dir or cfg["graph_output_dir"]

    # Resolve repo list: CLI → deploy_history.repos → all repos
    repos = args.repos
    if not repos:
        repos = cfg["deploy_repos"] or cfg["all_repos"]
    if not repos:
        logger.error("No repos specified and none found in config.")
        sys.exit(1)

    # Validate repos against the known list (warn, don't block)
    unknown = [r for r in repos if r not in cfg["all_repos"]]
    if unknown:
        logger.warning(f"Repos not found in github_config.yaml (will still attempt): {unknown}")

    since = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    logger.info(f"Fetching releases since {args.since} for {len(repos)} repos: {repos}")

    fetcher = _GitHubReleases(cfg["token"], cfg["org"])
    all_releases = []
    for repo in repos:
        try:
            releases = fetcher.fetch(repo, since)
            logger.info(f"  {repo}: {len(releases)} releases")
            all_releases.extend(releases)
        except Exception as e:
            logger.warning(f"  {repo}: failed — {e}")

    if not all_releases:
        logger.warning("No releases found. Check repo names, token scope, and --since date.")
        return

    rows = releases_to_rows(all_releases)

    if args.dry_run:
        print_table(rows)
        print("\n[dry-run] No files written.")
        return

    timestamp = datetime.now().strftime("%Y_%m_%d")
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / f"{timestamp}_github_deploy_history.csv"
    write_csv(rows, csv_path)
    write_graph_and_summary(rows, graph_output_dir, since)

    print(f"\nDone. {len(rows)} releases from {len(repos)} repos since {args.since}.")
    print(f"CSV:   {csv_path}")
    print(f"PNG + TXT: {graph_output_dir}/")


if __name__ == "__main__":
    main()
