#!/usr/bin/env python3
"""
Post a "any update?" comment on stale Jira tickets listed in a CSV.

Intended workflow:
    1. Run stale_tickets_to_csv.py  → generates a dated CSV
    2. Edit the CSV, deleting rows you do NOT want to comment on
    3. Run this script against the edited CSV

The comment @-mentions the assignee (if present) and says when the ticket
entered its current status.

Usage:
    python comment_stale_tickets.py path/to/edited.csv path/to/stale_tickets.yaml
    python comment_stale_tickets.py path/to/edited.csv path/to/stale_tickets.yaml --dry-run
"""

import argparse
import csv
import logging
import sys
from pathlib import Path
from typing import Dict, List

import requests
import yaml

try:
    from .utils import create_jira_session
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from lucille.jira.utils import create_jira_session


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------


def load_config(config_path: str) -> Dict:
    """Load YAML config and return the jira sub-section."""
    with open(config_path, "r") as fh:
        raw = yaml.safe_load(fh)
    if "jira" not in raw:
        raise ValueError("Config missing required section: 'jira'")
    jira = raw["jira"]
    for key in ("base_url", "username", "api_token"):
        if key not in jira:
            raise ValueError(f"Config jira section missing required key: '{key}'")
    return jira


def read_csv(csv_path: str) -> List[Dict]:
    """Read the (possibly edited) CSV and return a list of row dicts."""
    rows = []
    with open(csv_path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(row)
    return rows


def build_comment_adf(
    assignee_name: str,
    assignee_account_id: str,
    status: str,
    status_since: str,
) -> Dict:
    """
    Build an Atlassian Document Format (ADF) comment body.

    Produces:  "Hey @Assignee, this ticket has been in <Status> since <date>. Any update?"

    If there is no assignee_account_id the mention node is omitted and the
    assignee_name is used as plain text instead.
    """
    intro_text = "Hey "
    outro_text = f", this ticket has been in {status} since {status_since}. Any update?"

    if assignee_account_id:
        content = [
            {"type": "text", "text": intro_text},
            {
                "type": "mention",
                "attrs": {
                    "id": assignee_account_id,
                    "text": f"@{assignee_name}",
                },
            },
            {"type": "text", "text": outro_text},
        ]
    else:
        name = assignee_name if assignee_name and assignee_name != "Unassigned" else "team"
        content = [
            {"type": "text", "text": f"{intro_text}{name}{outro_text}"},
        ]

    return {
        "body": {
            "type": "doc",
            "version": 1,
            "content": [
                {
                    "type": "paragraph",
                    "content": content,
                }
            ],
        }
    }


def format_status_since(raw: str) -> str:
    """Return a human-friendly date string, falling back to raw value."""
    if not raw:
        return "an unknown date"
    # Already YYYY-MM-DD from stale_tickets_to_csv; just return as-is.
    return raw


# ---------------------------------------------------------------------------
# Side-effecting functions
# ---------------------------------------------------------------------------


def post_comment(
    session: requests.Session,
    base_url: str,
    issue_key: str,
    comment_body: Dict,
) -> None:
    """POST the ADF comment body to the given issue."""
    url = f"{base_url.rstrip('/')}/rest/api/3/issue/{issue_key}/comment"
    response = session.post(url, json=comment_body)
    response.raise_for_status()
    logger.debug(f"Comment posted to {issue_key}: HTTP {response.status_code}")


def process_rows(
    rows: List[Dict],
    session: requests.Session,
    base_url: str,
    dry_run: bool,
) -> None:
    """Iterate over CSV rows and post (or preview) a comment on each ticket."""
    total = len(rows)
    for i, row in enumerate(rows, 1):
        issue_key = row.get("issue_key", "").strip()
        if not issue_key:
            logger.warning(f"Row {i}: empty issue_key, skipping")
            continue

        assignee = row.get("assignee", "Unassigned").strip()
        account_id = row.get("assignee_account_id", "").strip()
        status = row.get("status", "").strip()
        status_since = format_status_since(row.get("status_since", "").strip())

        comment = build_comment_adf(assignee, account_id, status, status_since)

        if dry_run:
            logger.info(
                f"[{i}/{total}] DRY RUN — would comment on {issue_key}: "
                f"assignee={assignee}, status={status}, since={status_since}"
            )
        else:
            logger.info(f"[{i}/{total}] Posting comment to {issue_key}")
            try:
                post_comment(session, base_url, issue_key, comment)
            except requests.exceptions.RequestException as exc:
                logger.error(f"Failed to post comment on {issue_key}: {exc}")


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
        description="Post 'any update?' comments on stale Jira tickets from a CSV.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("csv", help="Path to the (edited) stale tickets CSV")
    parser.add_argument("config", help="Path to YAML configuration file (for auth)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log what would happen without posting any comments",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    setup_logging(args.verbose)

    try:
        jira_cfg = load_config(args.config)
    except (FileNotFoundError, ValueError, yaml.YAMLError) as exc:
        logger.error(f"Config error: {exc}")
        sys.exit(1)

    try:
        rows = read_csv(args.csv)
    except FileNotFoundError:
        logger.error(f"CSV not found: {args.csv}")
        sys.exit(1)

    if not rows:
        logger.warning("CSV is empty — nothing to do.")
        sys.exit(0)

    logger.info(f"Loaded {len(rows)} ticket(s) from {args.csv}")

    if args.dry_run:
        logger.info("DRY RUN mode — no comments will be posted.")

    base_url = jira_cfg["base_url"]
    session = create_jira_session(base_url, jira_cfg["username"], jira_cfg["api_token"])

    process_rows(rows, session, base_url, dry_run=args.dry_run)
    logger.info("Done.")


if __name__ == "__main__":
    main()
