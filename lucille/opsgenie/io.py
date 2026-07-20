"""CSV loading for OpsGenie alerts.

The CSV columns as of OpsGenie's 2026 export:

  Alert ID, Alias, TinyID, Message, Status, IsSeen, Acknowledged,
  Snoozed, CreatedAt, CreatedAtDate, UpdatedAt, UpdatedAtDate, Count,
  Owner, Teams

``CreatedAt`` is milliseconds since epoch (integer, UTC).
``Acknowledged`` is the string ``"true"`` or ``"false"``.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Union


@dataclass(frozen=True)
class Alert:
    """One row of the OpsGenie CSV, typed."""

    alert_id: str
    alias: str
    message: str
    status: str            # 'open' | 'closed'
    acknowledged: bool
    created_at: datetime   # UTC
    owner: str
    team: str
    count: int             # OpsGenie's dedup counter for this alert


def _parse_bool(s: str) -> bool:
    """OpsGenie CSVs use lowercase 'true'/'false' strings."""
    return s.strip().lower() == "true"


def _parse_created_at(ms_str: str) -> datetime:
    """Parse the CSV's millisecond-epoch ``CreatedAt`` column to a UTC datetime."""
    return datetime.fromtimestamp(int(ms_str) / 1000, tz=timezone.utc)


def _parse_count(s: str) -> int:
    """Return int(s), defaulting to 1 for blank/malformed values."""
    s = (s or "").strip()
    if not s:
        return 1
    try:
        return int(s)
    except ValueError:
        return 1


def load_alerts(csv_path: Union[str, Path]) -> List[Alert]:
    """Load the OpsGenie CSV export into a list of ``Alert`` records.

    Rows with unparseable ``CreatedAt`` are skipped (rare, usually from a
    partially-written export). All other fields are string-typed and
    stripped of surrounding whitespace.
    """
    alerts: List[Alert] = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                created = _parse_created_at(row["CreatedAt"])
            except (ValueError, KeyError):
                continue
            alerts.append(
                Alert(
                    alert_id=(row.get("Alert ID") or "").strip(),
                    alias=(row.get("Alias") or "").strip(),
                    message=(row.get("Message") or "").strip(),
                    status=(row.get("Status") or "").strip().lower(),
                    acknowledged=_parse_bool(row.get("Acknowledged", "")),
                    created_at=created,
                    owner=(row.get("Owner") or "").strip(),
                    team=(row.get("Teams") or "").strip(),
                    count=_parse_count(row.get("Count", "")),
                )
            )
    return alerts
