#!/usr/bin/env python3
"""
GitHub API client for the CFR tool.

Fetches releases, pull requests, and labels via the GitHub REST API v3.
Responses are cached to ~/Desktop/debris/cfr_cache/ for offline re-runs.
"""

import json
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

JIRA_KEY_RE = re.compile(r"\b([A-Z]{2,6}-\d+)\b")


@dataclass
class GitHubRelease:
    tag_name: str
    published_at: datetime
    repo: str
    html_url: str


@dataclass
class GitHubPR:
    number: int
    title: str
    body: str
    author: str
    labels: List[str]
    merged_at: Optional[datetime]
    base_branch: str
    html_url: str
    repo: str


class GitHubClient:
    BASE_URL = "https://api.github.com"

    def __init__(
        self,
        token: str,
        org: str,
        cache_dir: Optional[Path] = None,
        use_cache: bool = True,
    ):
        self.org = org
        self.headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }
        self.use_cache = use_cache
        self.cache_dir = cache_dir or (Path.home() / "Desktop" / "debris" / "cfr_cache")
        if use_cache:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self, key: str) -> Path:
        safe_key = re.sub(r"[^a-zA-Z0-9_.-]", "_", key)[:200]
        return self.cache_dir / f"{safe_key}.json"

    def _get(self, url: str, params: Optional[Dict] = None) -> Any:
        """GET with local caching and exponential backoff on rate limits."""
        cache_key = url + str(sorted((params or {}).items()))
        cp = self._cache_path(cache_key)
        if self.use_cache and cp.exists():
            logger.debug(f"Cache hit: {url}")
            return json.loads(cp.read_text())

        for attempt in range(5):
            try:
                resp = requests.get(url, headers=self.headers, params=params, timeout=30)
                if resp.status_code == 403 and "rate limit" in resp.text.lower():
                    wait = 2**attempt * 30
                    logger.warning(f"Rate limited, waiting {wait}s")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                data = resp.json()
                if self.use_cache:
                    cp.write_text(json.dumps(data))
                return data
            except requests.exceptions.RequestException as e:
                if attempt == 4:
                    raise
                wait = 2**attempt
                logger.warning(f"Request failed ({e}), retrying in {wait}s")
                time.sleep(wait)

    def _get_paginated(self, url: str, params: Optional[Dict] = None) -> List[Any]:
        """Fetch all pages of a paginated GitHub endpoint (100 items/page)."""
        params = dict(params or {})
        params["per_page"] = 100
        results = []
        page = 1
        while True:
            params["page"] = page
            data = self._get(url, params)
            if not data:
                break
            results.extend(data)
            if len(data) < 100:
                break
            page += 1
        return results

    def get_releases(self, repo: str, since: Optional[datetime] = None) -> List[GitHubRelease]:
        """Return all releases for a repo, optionally filtered to those after `since`."""
        url = f"{self.BASE_URL}/repos/{self.org}/{repo}/releases"
        raw = self._get_paginated(url)
        releases = []
        for r in raw:
            published_at = datetime.fromisoformat(r["published_at"].replace("Z", "+00:00"))
            if since and published_at < since:
                continue
            releases.append(
                GitHubRelease(
                    tag_name=r["tag_name"],
                    published_at=published_at,
                    repo=repo,
                    html_url=r["html_url"],
                )
            )
        return sorted(releases, key=lambda r: r.published_at)

    def get_prs_merged_between(
        self, repo: str, after: datetime, before: datetime
    ) -> List[GitHubPR]:
        """Return closed PRs merged to main between two timestamps."""
        url = f"{self.BASE_URL}/repos/{self.org}/{repo}/pulls"
        raw = self._get_paginated(
            url, {"state": "closed", "sort": "updated", "direction": "desc"}
        )
        prs = []
        for pr in raw:
            if not pr.get("merged_at"):
                continue
            merged_at = datetime.fromisoformat(pr["merged_at"].replace("Z", "+00:00"))
            if merged_at < after or merged_at > before:
                continue
            prs.append(self._parse_pr(pr, repo))
        return prs

    def get_prs_merged_after(self, repo: str, after: datetime) -> List[GitHubPR]:
        """Return all closed PRs merged to main after `after` (for hotfix detection)."""
        url = f"{self.BASE_URL}/repos/{self.org}/{repo}/pulls"
        raw = self._get_paginated(
            url, {"state": "closed", "sort": "updated", "direction": "desc"}
        )
        prs = []
        for pr in raw:
            if not pr.get("merged_at"):
                continue
            merged_at = datetime.fromisoformat(pr["merged_at"].replace("Z", "+00:00"))
            if merged_at < after:
                continue
            prs.append(self._parse_pr(pr, repo))
        return prs

    def _parse_pr(self, pr: Dict, repo: str) -> GitHubPR:
        merged_at = None
        if pr.get("merged_at"):
            merged_at = datetime.fromisoformat(pr["merged_at"].replace("Z", "+00:00"))
        return GitHubPR(
            number=pr["number"],
            title=pr.get("title", ""),
            body=pr.get("body", "") or "",
            author=pr["user"]["login"],
            labels=[lbl["name"] for lbl in pr.get("labels", [])],
            merged_at=merged_at,
            base_branch=pr["base"]["ref"],
            html_url=pr["html_url"],
            repo=repo,
        )

    @staticmethod
    def extract_jira_keys(pr: GitHubPR) -> List[str]:
        """Extract unique Jira issue keys from a PR's title and body."""
        text = f"{pr.title} {pr.body}"
        return list(dict.fromkeys(JIRA_KEY_RE.findall(text)))
