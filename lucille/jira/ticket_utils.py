"""
Shared utilities for Jira ticket generators.

Imported by ticket_generator.py and grouped_ticket_generator.py.
"""

import calendar
import json
import logging
import re
import time
from datetime import date
from pathlib import Path
from typing import Dict, Optional

import requests
import yaml


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Built-in derived variables
# ---------------------------------------------------------------------------

def _quarter_end_date(run_date: date) -> str:
    q = (run_date.month - 1) // 3          # 0-indexed quarter (0–3)
    end_month = (q + 1) * 3               # last month of quarter: 3, 6, 9, 12
    end_day = calendar.monthrange(run_date.year, end_month)[1]
    return date(run_date.year, end_month, end_day).isoformat()


DERIVED_REGISTRY: Dict[str, callable] = {
    "quarter":          lambda d: str((d.month - 1) // 3 + 1),
    "year":             lambda d: str(d.year),
    "month":            lambda d: str(d.month),
    "today":            lambda d: d.isoformat(),
    "quarter_end_date": _quarter_end_date,
}


def compute_derived_variables(requested: list, run_date: date) -> dict:
    unknown = [n for n in requested if n not in DERIVED_REGISTRY]
    if unknown:
        raise ValueError(
            f"Unknown derived_variable(s): {unknown}. "
            f"Supported: {sorted(DERIVED_REGISTRY)}"
        )
    return {name: DERIVED_REGISTRY[name](run_date) for name in requested}


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

def load_credentials(path: str) -> dict:
    with Path(path).expanduser().open() as f:
        cfg = yaml.safe_load(f)
    jira = cfg.get("jira", cfg)
    return {
        "base_url": jira["base_url"].rstrip("/"),
        "username": jira["username"],
        "api_token": jira["api_token"],
    }


# ---------------------------------------------------------------------------
# Template resolution
# ---------------------------------------------------------------------------

class _LenientMap(dict):
    """Returns the original {key} string for missing keys.

    Used when filling the description body so that instructional placeholder
    examples like {user/account} or {reason} pass through as literal text
    rather than raising a KeyError.
    """
    def __missing__(self, key: str) -> str:
        return f"{{{key}}}"


def resolve(template_str: str, row: dict, ctx: dict, strict: bool = True) -> str:
    """Fill {placeholders} by merging ctx (derived variables) and row (CSV columns).

    ctx is applied first; row values take priority on key collision.
    When strict=True (default), raises KeyError for any unresolved placeholder.
    When strict=False, unresolved placeholders are left as literal {name} text.
    """
    merged = {**ctx, **row}
    if strict:
        return template_str.format_map(merged)
    return template_str.format_map(_LenientMap(merged))


# ---------------------------------------------------------------------------
# ADF conversion
# ---------------------------------------------------------------------------

def _is_heading(block: str) -> bool:
    """A single short line that looks like a section title."""
    lines = block.strip().splitlines()
    if len(lines) != 1:
        return False
    line = lines[0].strip()
    return (
        0 < len(line) < 70
        and not line.startswith(('"', "\u201c"))  # not a quoted string
        and ":" not in line                        # not a field label like "Resource: foo"
        and not line[0].islower()                  # starts with uppercase
    )


def text_to_adf(text: str) -> dict:
    """Convert plain text to a minimal Atlassian Document Format (ADF) doc.

    Rules:
    - Blank lines separate blocks.
    - Single short title-case lines → heading level 3.
    - Everything else → paragraph, with hardBreak between lines.
    """
    content = []
    raw_blocks = re.split(r"\n{2,}", text)

    for raw in raw_blocks:
        block = raw.strip()
        if not block or set(block) <= {"_", " "}:
            continue

        if _is_heading(block):
            content.append({
                "type": "heading",
                "attrs": {"level": 3},
                "content": [{"type": "text", "text": block.strip()}],
            })
        else:
            inline = []
            for line in block.splitlines():
                cleaned = line.strip()
                if not cleaned:
                    continue
                if inline:
                    inline.append({"type": "hardBreak"})
                inline.append({"type": "text", "text": cleaned})
            if inline:
                content.append({"type": "paragraph", "content": inline})

    if not content:
        content.append({"type": "paragraph", "content": [{"type": "text", "text": ""}]})

    return {"version": 1, "type": "doc", "content": content}


# ---------------------------------------------------------------------------
# Jira API
# ---------------------------------------------------------------------------

def lookup_account_id(
    email: str,
    session: requests.Session,
    base_url: str,
    cache: dict,
) -> Optional[str]:
    if email in cache:
        return cache[email]
    resp = session.get(f"{base_url}/rest/api/3/user/search", params={"query": email})
    resp.raise_for_status()
    results = resp.json()
    account_id = results[0]["accountId"] if results else None
    cache[email] = account_id
    if account_id is None:
        logger.warning(f"No Jira account found for email: {email}")
    return account_id


def create_issue(
    payload: dict,
    session: Optional[requests.Session],
    base_url: str,
    dry_run: bool,
) -> dict:
    if dry_run:
        print(json.dumps(payload, indent=2))
        return {"key": "DRY-RUN", "self": ""}

    for attempt in range(3):
        resp = session.post(f"{base_url}/rest/api/3/issue", json=payload)
        if resp.status_code == 429:
            wait = 2 ** attempt
            logger.warning(f"Rate limited; retrying in {wait}s")
            time.sleep(wait)
            continue
        if not resp.ok:
            logger.error(f"Jira API error {resp.status_code}: {resp.text}")
            return {"key": None, "error": resp.text}
        return resp.json()

    logger.error("Exhausted retries on rate limit")
    return {"key": None, "error": "rate_limit"}
