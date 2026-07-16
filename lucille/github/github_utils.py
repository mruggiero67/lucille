#!/usr/bin/env python3
"""Shared GitHub utility functions."""

import logging
from typing import List

from lucille.github.session import GITHUB_API_BASE, create_github_session, paginate

logger = logging.getLogger(__name__)


def fetch_org_repos(org: str, token: str) -> List[str]:
    """Fetch all non-archived repository names for a GitHub organization.

    Side-effecting function that makes GitHub API calls.

    Args:
        org: GitHub organization name
        token: GitHub personal access token

    Returns:
        List of repository names (name only, not org/repo format)
    """
    logger.info(f"Fetching repositories for organization: {org}")
    session = create_github_session(token)
    url = f"{GITHUB_API_BASE}/orgs/{org}/repos"
    repos = [
        r["name"]
        for r in paginate(session, url, {"type": "all"})
        if not r.get("archived", False)
    ]
    logger.info(f"Found {len(repos)} active repositories in {org}")
    return repos
