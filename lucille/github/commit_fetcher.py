"""
GitHub commit fetcher — fetches releases and commits between tags, extracts Jira ticket refs.
"""
import logging
import re
import time
from datetime import datetime
from typing import Any, Dict, List

import requests

from lucille.github.session import GITHUB_API_BASE, create_github_session, paginate

logger = logging.getLogger(__name__)

DEFAULT_TICKET_PATTERN = r"(?:OOT|SSJ|DEVOPS|DIP)-\d+"


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------

def parse_ticket_keys(commit_message: str, pattern: str = DEFAULT_TICKET_PATTERN) -> List[str]:
    """Return all Jira ticket keys found in a commit message.
    Uses group(0) so patterns with capturing groups still return the full match."""
    return [m.group(0) for m in re.finditer(pattern, commit_message)]


def deduplicate_ticket_keys(keys: List[str]) -> List[str]:
    """Return sorted unique ticket keys."""
    return sorted(set(keys))


def extract_project_key(ticket_key: str) -> str:
    """Return the project prefix from a ticket key, e.g. 'OOT-123' -> 'OOT'."""
    return ticket_key.split("-")[0]


# ---------------------------------------------------------------------------
# Side-effecting functions
# ---------------------------------------------------------------------------

def _fetch_releases(token: str, org: str, repo: str, since: datetime) -> List[Dict[str, Any]]:
    """Return all releases for repo published on or after since."""
    session = create_github_session(token)
    url = f"{GITHUB_API_BASE}/repos/{org}/{repo}/releases"
    try:
        raw = list(paginate(session, url))
    except requests.exceptions.RequestException as e:
        logger.warning(f"{repo}: releases fetch failed — {e}")
        return []

    releases = []
    for r in raw:
        if not r.get("published_at"):
            continue
        published = datetime.fromisoformat(r["published_at"].replace("Z", "+00:00"))
        if published < since:
            continue
        releases.append({"tag": r["tag_name"], "published_at": published})
    return releases


def fetch_commits_between_tags(
    token: str, org: str, repo: str, base_tag: str, head_tag: str
) -> List[Dict[str, str]]:
    """Return commits included in head_tag but not base_tag via the GitHub compare API.

    Each dict has keys: sha, message. Returns [] on 404 or repeated failure.

    Note: the compare API returns a single object (not paginated), so we use
    ``session.get`` directly rather than ``paginate``.
    """
    session = create_github_session(token)
    url = f"{GITHUB_API_BASE}/repos/{org}/{repo}/compare/{base_tag}...{head_tag}"
    for attempt in range(5):
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 404:
                logger.warning(f"{repo}: compare {base_tag}...{head_tag} not found")
                return []
            if resp.status_code == 403 and "rate limit" in resp.text.lower():
                wait = 2 ** attempt * 30
                logger.warning(f"Rate limited on compare API — sleeping {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return [
                {"sha": c["sha"], "message": c["commit"]["message"]}
                for c in resp.json().get("commits", [])
            ]
        except requests.exceptions.RequestException as e:
            if attempt == 4:
                logger.error(f"{repo}: compare fetch failed after retries — {e}")
                return []
            time.sleep(2 ** attempt)
    return []


def fetch_all_releases_with_commits(
    token: str,
    org: str,
    repos: List[str],
    since: datetime,
    ticket_pattern: str = DEFAULT_TICKET_PATTERN,
) -> List[Dict[str, Any]]:
    """
    For each repo, fetch releases since `since`, then retrieve commits between adjacent
    release tags via the GitHub compare API. Parses Jira ticket keys from commit messages.

    Returns a list of deployment dicts:
        {
            "repo": str,
            "version": str,
            "deployed_at": datetime,
            "commits": [{"sha": str, "message": str, "ticket_keys": List[str]}]
        }

    The oldest release per repo cannot be diffed (no prior tag), so it is recorded as
    a deployment with an empty commits list.
    """
    deployments: List[Dict[str, Any]] = []

    for repo in repos:
        releases = _fetch_releases(token, org, repo, since)
        if not releases:
            continue

        releases.sort(key=lambda r: r["published_at"])
        logger.info(f"{repo}: {len(releases)} release(s) since {since.date()}")

        for i, release in enumerate(releases):
            if i == 0:
                logger.debug(
                    f"{repo}/{release['tag']}: oldest release in window, skipping commit diff"
                )
                deployments.append(
                    {
                        "repo": repo,
                        "version": release["tag"],
                        "deployed_at": release["published_at"],
                        "commits": [],
                    }
                )
                continue

            prev_tag = releases[i - 1]["tag"]
            raw_commits = fetch_commits_between_tags(token, org, repo, prev_tag, release["tag"])

            enriched = []
            for c in raw_commits:
                keys = deduplicate_ticket_keys(parse_ticket_keys(c["message"], ticket_pattern))
                enriched.append({"sha": c["sha"], "message": c["message"], "ticket_keys": keys})

            deployments.append(
                {
                    "repo": repo,
                    "version": release["tag"],
                    "deployed_at": release["published_at"],
                    "commits": enriched,
                }
            )

    logger.info(f"Total deployments collected across {len(repos)} repos: {len(deployments)}")
    return deployments
