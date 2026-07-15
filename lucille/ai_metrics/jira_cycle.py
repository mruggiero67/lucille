"""Jira 'In Progress → Done' cycle-time lookup for ai_metrics.

Reuses ``lucille.jira.ticket_changelog._parse_jira_datetime`` and
``find_ticket_start_date`` for the start-time half; adds a mirror function
for the done-time half.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from lucille.jira.ticket_changelog import (
    _parse_jira_datetime,
    find_ticket_start_date,
)
from lucille.jira.utils import make_jira_request

logger = logging.getLogger(__name__)


DEFAULT_DEV_STATUSES = ["In Progress", "In Development", "In Dev"]
DEFAULT_DONE_STATUSES = ["Done", "Resolved", "Closed", "Deployed"]


@dataclass
class TicketCycle:
    """Cycle-time result for one Jira ticket."""
    key: str
    started_at: Optional[datetime]
    done_at: Optional[datetime]

    @property
    def cycle_time_days(self) -> Optional[float]:
        if self.started_at and self.done_at and self.done_at > self.started_at:
            return (self.done_at - self.started_at).total_seconds() / 86400
        return None


def find_ticket_done_date(
    changelog_histories: List[Dict[str, Any]],
    done_statuses: List[str],
) -> Optional[datetime]:
    """Latest transition INTO any done status. Returns None if never done."""
    upper_done = {s.upper() for s in done_statuses}
    events: List[datetime] = []
    for history in changelog_histories:
        for item in history.get("items", []):
            if item.get("field") != "status":
                continue
            to_status = (item.get("toString") or "").upper()
            if to_status in upper_done:
                dt = _parse_jira_datetime(history.get("created"))
                if dt:
                    events.append(dt)
    return max(events) if events else None


def fetch_ticket_cycles(
    session: requests.Session,
    base_url: str,
    ticket_keys: List[str],
    dev_statuses: List[str] = DEFAULT_DEV_STATUSES,
    done_statuses: List[str] = DEFAULT_DONE_STATUSES,
) -> Dict[str, TicketCycle]:
    """Fetch changelog for each ticket key and compute start/done timestamps.

    Tickets that can't be fetched are omitted from the result.
    """
    result: Dict[str, TicketCycle] = {}
    for i, key in enumerate(ticket_keys, 1):
        try:
            data = make_jira_request(
                session,
                base_url,
                f"issue/{key}",
                params={"expand": "changelog", "fields": "created,status,resolutiondate"},
            )
        except Exception as e:
            logger.warning(f"Could not fetch Jira ticket {key}: {e}")
            continue
        histories = (data.get("changelog") or {}).get("histories", [])
        started = find_ticket_start_date(histories, dev_statuses)
        done = find_ticket_done_date(histories, done_statuses)
        # Fallback to resolutiondate if no explicit 'Done' transition was found
        # (happens when tickets are auto-resolved by workflows).
        if done is None:
            done = _parse_jira_datetime((data.get("fields") or {}).get("resolutiondate"))
        result[key] = TicketCycle(key=key, started_at=started, done_at=done)
        if i % 25 == 0:
            logger.info(f"  fetched {i}/{len(ticket_keys)} ticket changelogs")
    logger.info(f"Resolved cycle times for {len(result)}/{len(ticket_keys)} tickets")
    return result
