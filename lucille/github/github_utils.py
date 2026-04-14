#!/usr/bin/env python3
"""
Shared GitHub utility functions.
"""

import logging
from typing import List

import requests

logger = logging.getLogger(__name__)


def fetch_org_repos(org: str, token: str) -> List[str]:
    """
    Fetch all non-archived repository names for a GitHub organization.

    Side-effecting function that makes GitHub API calls.

    Args:
        org: GitHub organization name
        token: GitHub personal access token

    Returns:
        List of repository names (name only, not org/repo format)
    """
    logger.info(f"Fetching repositories for organization: {org}")

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    repos = []
    page = 1
    per_page = 100

    while True:
        url = f"https://api.github.com/orgs/{org}/repos"
        params = {"page": page, "per_page": per_page, "type": "all"}

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        page_repos = response.json()
        if not page_repos:
            break

        repos.extend(repo["name"] for repo in page_repos if not repo.get("archived", False))
        logger.debug(f"Fetched page {page}: {len(page_repos)} repositories")

        page += 1

        if len(page_repos) < per_page:
            break

    logger.info(f"Found {len(repos)} active repositories in {org}")
    return repos
