"""
Jira ticket changelog utilities — determines when work actually started on a ticket.
"""
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

try:
    from .utils import make_jira_request
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from lucille.jira.utils import make_jira_request

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------

def _parse_jira_datetime(date_string: Optional[str]) -> Optional[datetime]:
    """Parse a Jira datetime string to a timezone-aware datetime."""
    if not date_string:
        return None
    try:
        if date_string.endswith("Z"):
            return datetime.fromisoformat(date_string.replace("Z", "+00:00"))
        if "." in date_string:
            date_part, rest = date_string.split(".", 1)
            tz_start = max(rest.rfind("+"), rest.rfind("-"))
            if tz_start > 0:
                tz = rest[tz_start:]
                if len(tz) == 5:  # e.g. -0700 → -07:00
                    tz = tz[:-2] + ":" + tz[-2:]
                date_string = date_part + tz
        return datetime.fromisoformat(date_string)
    except Exception as e:
        logger.debug(f"Could not parse datetime '{date_string}': {e}")
        return None


def find_ticket_start_date(
    changelog_histories: List[Dict[str, Any]], dev_statuses: List[str]
) -> Optional[datetime]:
    """
    Scan Jira changelog histories for the earliest transition to any dev status.
    Returns None if no such transition exists; caller should fall back to created date.
    Status comparison is case-insensitive.
    """
    upper_dev = {s.upper() for s in dev_statuses}
    events: List[datetime] = []

    for history in changelog_histories:
        for item in history.get("items", []):
            if item.get("field") != "status":
                continue
            to_status = (item.get("toString") or "").upper()
            if to_status in upper_dev:
                dt = _parse_jira_datetime(history.get("created"))
                if dt:
                    events.append(dt)

    return min(events) if events else None


def select_start_date(
    first_dev_transition: Optional[datetime], created_date: datetime
) -> datetime:
    """Return the first dev-status transition if known, otherwise the ticket creation date."""
    return first_dev_transition if first_dev_transition is not None else created_date


# ---------------------------------------------------------------------------
# Side-effecting functions
# ---------------------------------------------------------------------------

def fetch_ticket_start_dates(
    session: requests.Session,
    base_url: str,
    ticket_keys: List[str],
    dev_statuses: List[str],
) -> Dict[str, datetime]:
    """
    For each ticket key fetch Jira changelog and determine when work started.
    Returns {ticket_key: start_datetime}. Tickets that cannot be fetched are omitted.
    """
    result: Dict[str, datetime] = {}

    for key in ticket_keys:
        try:
            data = make_jira_request(
                session,
                base_url,
                f"issue/{key}",
                params={"expand": "changelog", "fields": "created,status"},
            )
        except Exception as e:
            logger.warning(f"Could not fetch Jira ticket {key}: {e}")
            continue

        created_str = (data.get("fields") or {}).get("created")
        created_date = _parse_jira_datetime(created_str)
        if not created_date:
            logger.warning(f"Ticket {key} has no parseable created date — skipping")
            continue

        histories = (data.get("changelog") or {}).get("histories", [])
        first_dev = find_ticket_start_date(histories, dev_statuses)
        result[key] = select_start_date(first_dev, created_date)

    logger.info(f"Resolved start dates for {len(result)}/{len(ticket_keys)} tickets")
    return result
