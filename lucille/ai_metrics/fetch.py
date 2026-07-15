"""GitHub API layer for ai_metrics.

Everything that hits ``api.github.com`` lives here so ``analyze.py`` and
``detect.py`` can stay pure and test-friendly.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

GITHUB_API_BASE = "https://api.github.com"


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class PRRecord:
    """Everything we need about one pull request."""
    repo: str
    number: int
    title: str
    author_login: Optional[str]
    author_type: Optional[str]
    state: str                              # 'open' | 'closed'
    merged: bool
    created_at: datetime
    closed_at: Optional[datetime]
    merged_at: Optional[datetime]
    head_sha: str
    commit_messages: List[str] = field(default_factory=list)
    commit_shas: List[str] = field(default_factory=list)
    # Populated by main.py after fetching:
    ai_touched: bool = False
    ai_signatures: List[str] = field(default_factory=list)
    ticket_keys: List[str] = field(default_factory=list)
    is_revert: bool = False

    @property
    def url(self) -> str:
        return f"https://github.com/{self.repo}/pull/{self.number}"


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _github_session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    })
    return s


def _paginate(
    session: requests.Session,
    url: str,
    params: Optional[Dict[str, Any]] = None,
) -> Iterable[Any]:
    """Yield items from a GitHub REST endpoint, following ``Link: rel=next``."""
    params = dict(params or {})
    params.setdefault("per_page", 100)
    while url:
        r = session.get(url, params=params, timeout=30)
        _handle_rate_limit(r)
        r.raise_for_status()
        for item in r.json():
            yield item
        url = r.links.get("next", {}).get("url")
        params = None  # subsequent requests use the absolute next URL


def _handle_rate_limit(response: requests.Response) -> None:
    """If we're within 5 requests of the limit, sleep until the reset."""
    remaining = int(response.headers.get("X-RateLimit-Remaining", "5000"))
    if remaining > 5:
        return
    reset = int(response.headers.get("X-RateLimit-Reset", "0"))
    wait = max(0, reset - int(time.time())) + 5
    logger.warning(f"GitHub rate limit low ({remaining} left); sleeping {wait}s")
    time.sleep(wait)


# ---------------------------------------------------------------------------
# Datetime helpers
# ---------------------------------------------------------------------------


def _parse_iso(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    if dt_str.endswith("Z"):
        dt_str = dt_str.replace("Z", "+00:00")
    return datetime.fromisoformat(dt_str)


# ---------------------------------------------------------------------------
# PR fetching
# ---------------------------------------------------------------------------


def fetch_prs_since(
    session: requests.Session,
    org: str,
    repo: str,
    since: datetime,
) -> List[Dict[str, Any]]:
    """Fetch every PR in ``org/repo`` created on or after ``since``, any state."""
    logger.info(f"Fetching PRs from {org}/{repo} since {since.date()}")
    url = f"{GITHUB_API_BASE}/repos/{org}/{repo}/pulls"
    params = {"state": "all", "sort": "created", "direction": "desc", "per_page": 100}
    result: List[Dict[str, Any]] = []
    for pr in _paginate(session, url, params):
        created = _parse_iso(pr.get("created_at"))
        if created is None:
            continue
        if created < since:
            # Sorted desc, so anything older can be skipped.
            break
        result.append(pr)
    logger.info(f"  fetched {len(result)} PRs from {org}/{repo}")
    return result


def fetch_pr_commit_messages(
    session: requests.Session,
    org: str,
    repo: str,
    pr_number: int,
) -> Tuple[List[str], List[str]]:
    """Fetch every commit's message + SHA for a single PR.

    Returns ``(messages, shas)``. GitHub caps a single PR at 250 commits per
    this endpoint; that's more than enough for our uses.
    """
    url = f"{GITHUB_API_BASE}/repos/{org}/{repo}/pulls/{pr_number}/commits"
    messages: List[str] = []
    shas: List[str] = []
    for c in _paginate(session, url):
        messages.append((c.get("commit") or {}).get("message", "") or "")
        shas.append(c.get("sha", ""))
    return messages, shas


# ---------------------------------------------------------------------------
# Caching wrapper
# ---------------------------------------------------------------------------


class PRCache:
    """Filesystem cache keyed by ``org_repo/pr_<n>.json``.

    A cache hit avoids the /pulls/{n}/commits call entirely. Set ``enabled``
    to False to force a full refresh.
    """

    def __init__(self, cache_dir: Path, enabled: bool = True):
        self.cache_dir = cache_dir
        self.enabled = enabled
        if enabled:
            cache_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, org: str, repo: str, number: int) -> Path:
        return self.cache_dir / f"{org}__{repo}" / f"pr_{number}.json"

    def get_commits(self, org: str, repo: str, number: int) -> Optional[Tuple[List[str], List[str]]]:
        if not self.enabled:
            return None
        p = self._path(org, repo, number)
        if not p.exists():
            return None
        try:
            data = json.loads(p.read_text())
            return data["messages"], data["shas"]
        except (json.JSONDecodeError, KeyError):
            return None

    def put_commits(self, org: str, repo: str, number: int, messages: List[str], shas: List[str]) -> None:
        if not self.enabled:
            return
        p = self._path(org, repo, number)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({"messages": messages, "shas": shas}))


# ---------------------------------------------------------------------------
# High-level driver
# ---------------------------------------------------------------------------


def fetch_all_prs(
    token: str,
    org: str,
    repos: List[str],
    since: datetime,
    cache: Optional[PRCache] = None,
) -> List[PRRecord]:
    """Return one ``PRRecord`` per PR opened in any of ``repos`` since ``since``.

    Commits are fetched (once per PR) and included in the record.
    """
    session = _github_session(token)
    records: List[PRRecord] = []
    for repo in repos:
        raw_prs = fetch_prs_since(session, org, repo, since)
        for i, pr in enumerate(raw_prs, 1):
            number = pr["number"]
            slug = f"{org}/{repo}"

            cached = cache.get_commits(org, repo, number) if cache else None
            if cached is not None:
                messages, shas = cached
            else:
                try:
                    messages, shas = fetch_pr_commit_messages(session, org, repo, number)
                except requests.HTTPError as e:
                    logger.warning(f"Skipping {slug}#{number}: {e}")
                    continue
                if cache:
                    cache.put_commits(org, repo, number, messages, shas)

            user = pr.get("user") or {}
            records.append(PRRecord(
                repo=slug,
                number=number,
                title=pr.get("title") or "",
                author_login=user.get("login"),
                author_type=user.get("type"),
                state=pr.get("state") or "closed",
                merged=bool(pr.get("merged_at")),
                created_at=_parse_iso(pr.get("created_at")) or datetime.now(timezone.utc),
                closed_at=_parse_iso(pr.get("closed_at")),
                merged_at=_parse_iso(pr.get("merged_at")),
                head_sha=(pr.get("head") or {}).get("sha", ""),
                commit_messages=messages,
                commit_shas=shas,
            ))
            if i % 25 == 0:
                logger.info(f"  {slug}: processed {i}/{len(raw_prs)} PRs")
    logger.info(f"Fetched {len(records)} PRs total across {len(repos)} repos")
    return records


def resolve_reverted_prs(
    token: str,
    org: str,
    records: List[PRRecord],
    sha_to_number: Dict[Tuple[str, str], int],
) -> Dict[int, int]:
    """Map revert-PR-number → original-PR-number, using an in-memory SHA index.

    Args:
        records: All PRs from the fetch window.
        sha_to_number: ``{(repo, sha): pr_number}`` for every commit SHA seen
            in ``records``.

    Returns:
        ``{revert_pr_number: original_pr_number}`` — only entries for reverts
        whose original PR is *also* in ``records``. Reverts of older PRs
        (outside the window) are looked up via one API call each.
    """
    from lucille.ai_metrics.detect import extract_reverted_shas
    session = _github_session(token)
    result: Dict[int, int] = {}
    for r in records:
        reverted_shas = extract_reverted_shas(r.commit_messages)
        if not reverted_shas:
            continue
        repo_full = r.repo  # 'org/repo'
        found = None
        for sha in reverted_shas:
            found = sha_to_number.get((repo_full, sha))
            if found:
                break
            # Fall back to a live lookup for older reverts.
            org_only, repo_only = repo_full.split("/", 1)
            url = f"{GITHUB_API_BASE}/repos/{org_only}/{repo_only}/commits/{sha}/pulls"
            try:
                resp = session.get(url, timeout=30)
                _handle_rate_limit(resp)
                if resp.status_code == 200:
                    pulls = resp.json()
                    if pulls:
                        found = pulls[0]["number"]
                        break
            except requests.RequestException as e:
                logger.debug(f"revert lookup failed for {repo_full}@{sha[:7]}: {e}")
        if found and found != r.number:
            result[r.number] = found
    logger.info(f"Resolved {len(result)} revert relationships")
    return result
