"""Shared GitHub HTTP session + pagination.

Prior to this module, seven files in lucille each built their own
``Authorization`` headers and their own paginator. This module offers one
canonical session factory and one pagination iterator so:

  - Auth headers change in exactly one place when GitHub deprecates something.
  - Rate-limit handling (both preemptive and reactive) is consistent.
  - Cursor-based endpoints (e.g. ``/dependabot/alerts``) and page-counter
    endpoints (e.g. ``/repos/{o}/{r}/pulls``) work through the same code path.

Both are handled by following the ``Link: rel="next"`` header, which every
paginated GitHub v3 endpoint returns.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Iterator, Optional

import requests

logger = logging.getLogger(__name__)

GITHUB_API_BASE = "https://api.github.com"

# Preemptive: if fewer than this many requests remain in the current
# rate-limit window, sleep until the window resets rather than pressing on.
_RATE_LIMIT_FLOOR = 5

# Reactive: how many times to retry on a rate-limit-abusive 403 before giving
# up. Exponential backoff base = 30s (matching the pre-refactor behavior in
# ``commit_fetcher._paginate_get``).
_MAX_RATE_LIMIT_RETRIES = 5
_RATE_LIMIT_BACKOFF_BASE = 30

# Transient error retry configuration.
_MAX_TRANSIENT_RETRIES = 5

# HTTP status codes that indicate a transient server-side problem and are
# worth retrying. GitHub returns these under load, during deploys, or when
# a specific backend is briefly unhealthy.
_RETRIABLE_STATUS_CODES = frozenset({500, 502, 503, 504})
_MAX_SERVER_ERROR_RETRIES = 4


def create_github_session(token: str) -> requests.Session:
    """Return a ``requests.Session`` pre-loaded with GitHub auth headers.

    Callers are expected to pass this session to :func:`paginate` (or to
    ``session.get`` directly for single-page endpoints).
    """
    s = requests.Session()
    s.headers.update({
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    })
    return s


def paginate(
    session: requests.Session,
    url: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    max_pages: Optional[int] = None,
    per_page: int = 100,
) -> Iterator[Any]:
    """Yield items from a paginated GitHub REST endpoint.

    Follows the ``Link: rel="next"`` header — works for both cursor-based
    endpoints (dependabot alerts, etc.) and page-counter endpoints. Handles
    rate limits and transient errors internally.

    Args:
        session: A session from :func:`create_github_session`.
        url: Absolute URL to fetch.
        params: Query parameters for the *first* request. Subsequent requests
            follow the absolute URL from the Link header and drop these params.
        max_pages: Optional cap on the number of pages to fetch (useful for
            tests and diagnostics).
        per_page: Default page size if the caller didn't specify one.

    Yields:
        Each item in each response body, one at a time.
    """
    current_params: Optional[Dict[str, Any]] = dict(params or {})
    current_params.setdefault("per_page", per_page)
    current_url: Optional[str] = url
    pages_fetched = 0

    while current_url:
        if max_pages is not None and pages_fetched >= max_pages:
            return

        resp = _get_with_retries(session, current_url, current_params)
        if resp is None:
            return

        _sleep_if_rate_limit_low(resp)

        body = resp.json()
        if not body:
            return

        for item in body:
            yield item

        pages_fetched += 1
        # Subsequent requests follow the absolute next-URL and must not
        # re-append page params (they're already encoded in that URL).
        next_url = resp.links.get("next", {}).get("url")
        # Defensive: only follow if we got a real string URL. Guards against
        # malformed Link headers and test doubles with auto-mock attributes.
        current_url = next_url if isinstance(next_url, str) else None
        current_params = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_with_retries(
    session: requests.Session,
    url: str,
    params: Optional[Dict[str, Any]],
) -> Optional[requests.Response]:
    """GET ``url`` with retries for transient errors, rate limits, and 5xx.

    Returns None only if every retry attempt failed; otherwise raises for
    non-retriable HTTP errors via ``response.raise_for_status()``.
    """
    rate_limit_attempts = 0
    transient_attempts = 0
    server_error_attempts = 0
    while True:
        try:
            resp = session.get(url, params=params, timeout=30)
        except requests.exceptions.RequestException as e:
            if transient_attempts >= _MAX_TRANSIENT_RETRIES - 1:
                logger.error(f"GET {url} failed after {transient_attempts + 1} attempts: {e}")
                raise
            wait = 2 ** transient_attempts
            logger.warning(f"GET {url} transient failure ({e}); retrying in {wait}s")
            time.sleep(wait)
            transient_attempts += 1
            continue

        # 403 with a rate-limit body → back off and retry.
        if resp.status_code == 403 and "rate limit" in resp.text.lower():
            if rate_limit_attempts >= _MAX_RATE_LIMIT_RETRIES - 1:
                logger.error(f"GitHub rate limit exhausted for {url}")
                resp.raise_for_status()
            wait = _RATE_LIMIT_BACKOFF_BASE * (2 ** rate_limit_attempts)
            logger.warning(f"Rate-limited on {url}; sleeping {wait}s")
            time.sleep(wait)
            rate_limit_attempts += 1
            continue

        # Transient server-side error (500/502/503/504) → back off and retry.
        # GitHub emits these under load, during deploys, or when a backend is
        # briefly unhealthy. They are worth retrying; a persistent 5xx will
        # still raise once we exhaust our budget.
        if resp.status_code in _RETRIABLE_STATUS_CODES:
            if server_error_attempts >= _MAX_SERVER_ERROR_RETRIES - 1:
                logger.error(
                    f"GitHub {resp.status_code} for {url} after "
                    f"{server_error_attempts + 1} attempts; giving up"
                )
                resp.raise_for_status()
            wait = 2 ** server_error_attempts
            logger.warning(
                f"GitHub {resp.status_code} on {url}; retrying in {wait}s"
            )
            time.sleep(wait)
            server_error_attempts += 1
            continue

        resp.raise_for_status()
        return resp


def _sleep_if_rate_limit_low(response: requests.Response) -> None:
    """If we're near the request-window floor, sleep until reset."""
    try:
        remaining = int(response.headers.get("X-RateLimit-Remaining", "5000"))
    except ValueError:
        return
    if remaining > _RATE_LIMIT_FLOOR:
        return
    try:
        reset = int(response.headers.get("X-RateLimit-Reset", "0"))
    except ValueError:
        return
    wait = max(0, reset - int(time.time())) + 5
    if wait > 0:
        logger.warning(f"GitHub rate limit low ({remaining} left); sleeping {wait}s")
        time.sleep(wait)
