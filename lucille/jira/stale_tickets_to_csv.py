#!/usr/bin/env python3
"""
Fetch stale Jira tickets matching a JQL query and save results to CSV.

The CSV includes the date each ticket entered its current status, so you can
review it before feeding it to comment_stale_tickets.py.

Usage:
    python stale_tickets_to_csv.py path/to/stale_tickets.yaml
    python stale_tickets_to_csv.py path/to/stale_tickets.yaml --verbose
"""

import argparse
import csv
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests
import yaml

try:
    from .utils import create_jira_session, fetch_all_issues
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from lucille.jira.utils import create_jira_session, fetch_all_issues


logger = logging.getLogger(__name__)

CSV_FIELDS = [
    "issue_key",
    "summary",
    "assignee",
    "assignee_account_id",
    "status",
    "status_since",
    "url",
]


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------


def load_config(config_path: str) -> Dict:
    """Load and validate YAML config. Returns merged flat dict."""
    with open(config_path, "r") as fh:
        raw = yaml.safe_load(fh)

    for section in ("jira", "query"):
        if section not in raw:
            raise ValueError(f"Config missing required section: '{section}'")

    jira = raw["jira"]
    for key in ("base_url", "username", "api_token"):
        if key not in jira:
            raise ValueError(f"Config jira section missing required key: '{key}'")

    if "jql" not in raw["query"]:
        raise ValueError("Config query section missing required key: 'jql'")

    return raw


def mk_output_path(directory: str, label: str) -> Path:
    """Build a datestamped output file path."""
    date_prefix = datetime.now().strftime("%Y_%m_%d")
    safe_label = label.lower().replace(" ", "_")
    return Path(directory).expanduser() / f"{date_prefix}_{safe_label}.csv"


def parse_jira_timestamp(timestamp: str) -> datetime:
    """Parse a Jira ISO timestamp into a timezone-aware datetime."""
    if not timestamp:
        return datetime.now(timezone.utc)
    if timestamp.endswith("Z"):
        return datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    # Handle offsets like -0700 → -07:00
    if len(timestamp) >= 5 and timestamp[-5] in ("+", "-") and timestamp[-2:].isdigit():
        tz_part = timestamp[-5:]
        ts_part = timestamp[:-5]
        formatted_tz = f"{tz_part[:3]}:{tz_part[3:]}"
        return datetime.fromisoformat(ts_part + formatted_tz)
    dt = datetime.fromisoformat(timestamp)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def find_status_since(changelog: Dict, current_status: str) -> Optional[str]:
    """
    Walk the changelog histories to find the most recent transition INTO
    current_status. Returns an ISO date string (YYYY-MM-DD) or None.
    """
    histories = changelog.get("changelog", {}).get("histories", [])
    last_entry_date = None

    for history in histories:
        for item in history.get("items", []):
            if item.get("field") == "status" and item.get("toString") == current_status:
                last_entry_date = history.get("created")

    if last_entry_date is None:
        return None

    try:
        dt = parse_jira_timestamp(last_entry_date)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return last_entry_date[:10]  # fallback: first 10 chars is usually YYYY-MM-DD


def flatten_issue(issue: Dict, status_since: Optional[str], base_url: str) -> Dict:
    """Extract the fields we care about from a Jira issue dict."""
    fields = issue.get("fields", {})
    assignee = fields.get("assignee") or {}
    status = fields.get("status") or {}
    key = issue.get("key", "")

    return {
        "issue_key": key,
        "summary": fields.get("summary", ""),
        "assignee": assignee.get("displayName", "Unassigned"),
        "assignee_account_id": assignee.get("accountId", ""),
        "status": status.get("name", ""),
        "status_since": status_since or "",
        "url": f"{base_url.rstrip('/')}/browse/{key}",
    }


# ---------------------------------------------------------------------------
# Side-effecting functions
# ---------------------------------------------------------------------------


def fetch_issue_with_changelog(
    session: requests.Session, base_url: str, issue_key: str
) -> Dict:
    """Fetch a single issue with its full changelog."""
    url = f"{base_url.rstrip('/')}/rest/api/3/issue/{issue_key}"
    response = session.get(url, params={"expand": "changelog"})
    response.raise_for_status()
    return response.json()


def build_rows(
    session: requests.Session,
    base_url: str,
    issues: List[Dict],
) -> List[Dict]:
    """For each issue, fetch changelog and build a flat row dict."""
    rows = []
    total = len(issues)
    for i, issue in enumerate(issues, 1):
        key = issue["key"]
        logger.info(f"[{i}/{total}] Fetching changelog for {key}")
        try:
            detailed = fetch_issue_with_changelog(session, base_url, key)
            current_status = (detailed.get("fields", {}).get("status") or {}).get("name", "")
            status_since = find_status_since(detailed, current_status)
            rows.append(flatten_issue(detailed, status_since, base_url))
        except Exception as exc:
            logger.error(f"Skipping {key}: {exc}")
    return rows


def write_csv(rows: List[Dict], output_path: Path) -> None:
    """Write list of row dicts to a CSV file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"Wrote {len(rows)} rows to {output_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(levelname)-10s %(asctime)s %(filename)s %(lineno)d %(message)s",
        level=level,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch stale Jira tickets and save to CSV for review.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("config", help="Path to YAML configuration file")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    setup_logging(args.verbose)

    try:
        config = load_config(args.config)
    except (FileNotFoundError, ValueError, yaml.YAMLError) as exc:
        logger.error(f"Config error: {exc}")
        sys.exit(1)

    jira_cfg = config["jira"]
    query_cfg = config["query"]
    directory = config["output_directory"]

    base_url = jira_cfg["base_url"]
    jql = query_cfg["jql"]
    max_results = query_cfg.get("max_results", 500)
    label = query_cfg.get("name", "stale_tickets")

    logger.info(f"Connecting to {base_url}")
    session = create_jira_session(base_url, jira_cfg["username"], jira_cfg["api_token"])

    fields = ["summary", "status", "assignee"]
    logger.info(f"Running JQL: {jql}")
    issues = fetch_all_issues(
        session=session,
        base_url=base_url,
        jql=jql,
        fields=fields,
        max_results=max_results,
    )
    logger.info(f"Found {len(issues)} issues")

    if not issues:
        logger.warning("No issues returned — nothing to write.")
        sys.exit(0)

    rows = build_rows(session, base_url, issues)

    output_path = mk_output_path(directory, label)
    write_csv(rows, output_path)
    logger.info(f"Done. Review {output_path}, remove rows you don't want, then run comment_stale_tickets.py.")


if __name__ == "__main__":
    main()
