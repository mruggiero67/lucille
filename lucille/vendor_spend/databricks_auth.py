"""
Resolve a Databricks bearer token for the account-level billable-usage API.

Two paths, in order of preference:

1. **OAuth client-credentials** (preferred). If
   ``DATABRICKS_CLIENT_ID`` + ``DATABRICKS_CLIENT_SECRET`` are set, mint a
   short-lived bearer token at
   ``{accounts_host}/oidc/accounts/{account_id}/v1/token`` and use it.
2. **Static token** (escape hatch / legacy account-admin PAT). If
   ``DATABRICKS_TOKEN`` is set, use it as-is.

The minted token is cached for the lifetime of the process so a single run
of the report performs at most one ``oidc/.../v1/token`` exchange.

Note on precedence: we try the static token *first* if present, because it's
an explicit escape hatch — if a user set it, they meant to use it. Otherwise
we fall back to OAuth. If neither is configured we raise.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Optional

import requests

from lucille.vendor_spend.config import DatabricksConfig

logger = logging.getLogger(__name__)


# Process-wide single-token cache, keyed by (host, account_id, client_id).
_TOKEN_CACHE: dict[tuple[str, str, str], str] = {}
_CACHE_LOCK = threading.Lock()


def _oidc_token_url(accounts_host: str, account_id: str) -> str:
    return f"{accounts_host.rstrip('/')}/oidc/accounts/{account_id}/v1/token"


def mint_oauth_token(
    accounts_host: str,
    account_id: str,
    client_id: str,
    client_secret: str,
    *,
    session: Optional[requests.Session] = None,
    timeout: float = 30.0,
) -> str:
    """
    Exchange client credentials for a short-lived OAuth bearer token.

    Side-effecting (HTTP). Pure-ish in the sense that the inputs are all
    explicit parameters — no env reads — so it's easy to unit test.
    """
    url = _oidc_token_url(accounts_host, account_id)
    s = session or requests.Session()
    logger.info("Databricks OAuth: POST %s (client_credentials)", url)
    r = s.post(
        url,
        auth=(client_id, client_secret),
        data={"grant_type": "client_credentials", "scope": "all-apis"},
        timeout=timeout,
    )
    r.raise_for_status()
    payload = r.json()
    token = payload.get("access_token")
    if not token:
        raise RuntimeError(
            f"Databricks OIDC response missing access_token: keys={list(payload)}"
        )
    return token


def resolve_bearer_token(
    cfg: DatabricksConfig,
    *,
    session: Optional[requests.Session] = None,
    use_cache: bool = True,
) -> str:
    """
    Return a bearer token suitable for ``Authorization: Bearer ...``.

    Order of resolution:
      1. ``$DATABRICKS_TOKEN`` (or whatever ``cfg.token_env`` names) if set.
      2. OAuth client-credentials using ``cfg.client_id_env`` /
         ``cfg.client_secret_env``; cached per process.

    Raises ``RuntimeError`` if neither path is configured.
    """
    static = os.environ.get(cfg.token_env)
    if static:
        logger.debug("Databricks: using static token from $%s", cfg.token_env)
        return static

    client_id = os.environ.get(cfg.client_id_env)
    client_secret = os.environ.get(cfg.client_secret_env)
    if not (client_id and client_secret):
        raise RuntimeError(
            f"No Databricks credentials available: set ${cfg.token_env} OR both "
            f"${cfg.client_id_env} and ${cfg.client_secret_env}."
        )

    cache_key = (cfg.accounts_host, cfg.account_id, client_id)
    if use_cache:
        with _CACHE_LOCK:
            cached = _TOKEN_CACHE.get(cache_key)
            if cached:
                logger.debug("Databricks: using cached OAuth token")
                return cached

    token = mint_oauth_token(
        cfg.accounts_host,
        cfg.account_id,
        client_id,
        client_secret,
        session=session,
    )
    if use_cache:
        with _CACHE_LOCK:
            _TOKEN_CACHE[cache_key] = token
    return token


def clear_token_cache() -> None:
    """Drop all cached OAuth tokens. Intended for tests."""
    with _CACHE_LOCK:
        _TOKEN_CACHE.clear()
