#!/usr/bin/env python3
"""
OpsGenie Export Script
Exports: On-call schedules, Alert routing/escalation policies,
         Team & user configs, Integrations → CSV files
"""

import csv
import json
import logging
import os
import sys
import time
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

API_KEY = os.environ.get("OPSGENIE_API_KEY", "")
BASE_URL = "https://api.opsgenie.com"
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "./opsgenie_export"))
REQUEST_DELAY = 0.3   # seconds between paginated calls (rate-limit safety)

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_headers() -> dict:
    if not API_KEY:
        log.error("OPSGENIE_API_KEY environment variable is not set.")
        sys.exit(1)
    return {"Authorization": f"GenieKey {API_KEY}", "Content-Type": "application/json"}


def paginated_get(path: str, list_key: str, params={}) -> list:
    """Fetch all pages from an OpsGenie list endpoint."""
    results = []
    params = dict(params or {})
    params.setdefault("limit", 100)
    offset = 0

    while True:
        params["offset"] = offset
        resp = requests.get(f"{BASE_URL}{path}", headers=get_headers(), params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        page = data.get("data", data.get(list_key, []))
        if isinstance(page, dict):
            # Some endpoints wrap the list inside data.<key>
            page = page.get(list_key, [])
        results.extend(page)
        paging = data.get("paging", {})
        if not paging.get("next"):
            break
        offset += len(page)
        time.sleep(REQUEST_DELAY)

    return results


def simple_get(path: str) -> dict:
    resp = requests.get(f"{BASE_URL}{path}", headers=get_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json().get("data", {})


def write_csv(filename: str, rows: list[dict], fieldnames: list[str]) -> None:
    if not rows:
        log.warning("No data for %s — skipping.", filename)
        return
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUTPUT_DIR / filename
    if fieldnames is None:
        fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    log.info("Wrote %d rows → %s", len(rows), path)


def flatten(value, sep="; ") -> str:
    """Flatten lists/dicts to a readable string for CSV cells."""
    if isinstance(value, list):
        return sep.join(flatten(v) for v in value)
    if isinstance(value, dict):
        return sep.join(f"{k}={flatten(v)}" for k, v in value.items())
    return str(value) if value is not None else ""

# ── Exporters ─────────────────────────────────────────────────────────────────

def export_teams() -> list[dict]:
    log.info("Fetching teams …")
    teams = paginated_get("/v2/teams", "teams")
    rows = []
    for t in teams:
        # Fetch full team detail (includes members)
        try:
            detail = simple_get(f"/v2/teams/{t['id']}?identifierType=id")
        except Exception as e:
            log.warning("Could not fetch detail for team %s: %s", t.get("name"), e)
            detail = t
        members = detail.get("members", [])
        rows.append({
            "id": t.get("id"),
            "name": t.get("name"),
            "description": t.get("description", ""),
            "member_count": len(members),
            "members": flatten([m.get("user", {}).get("username", "") for m in members]),
            "member_roles": flatten([m.get("role", "") for m in members]),
        })
    write_csv("teams.csv", rows, fieldnames=["id", "name", "description", "member_count", "members", "member_roles"])
    return teams  # raw list reused by schedules


def export_users() -> None:
    log.info("Fetching users …")
    users = paginated_get("/v2/users", "users")
    rows = []
    for u in users:
        rows.append({
            "id": u.get("id"),
            "username": u.get("username"),
            "full_name": u.get("fullName"),
            "role": u.get("role", {}).get("name") if isinstance(u.get("role"), dict) else u.get("role"),
            "locale": u.get("locale"),
            "timezone": u.get("timeZone"),
            "verified": u.get("verified"),
            "blocked": u.get("blocked"),
            "tags": flatten(u.get("tags", [])),
        })
    write_csv("users.csv", rows, fieldnames=["id", "username", "full_name", "role", "locale", "timezone", "verified", "blocked", "tags"])


def export_schedules() -> None:
    log.info("Fetching schedules …")
    schedules = paginated_get("/v2/schedules", "schedules", {"expand": "rotation"})
    sched_rows = []
    rotation_rows = []

    for s in schedules:
        sched_rows.append({
            "id": s.get("id"),
            "name": s.get("name"),
            "description": s.get("description", ""),
            "timezone": s.get("timezone"),
            "enabled": s.get("enabled"),
            "team_id": s.get("ownerTeam", {}).get("id"),
            "team_name": s.get("ownerTeam", {}).get("name"),
        })
        for r in s.get("rotations", []):
            participants = r.get("participants", [])
            rotation_rows.append({
                "schedule_id": s.get("id"),
                "schedule_name": s.get("name"),
                "rotation_id": r.get("id"),
                "rotation_name": r.get("name"),
                "type": r.get("type"),
                "length": r.get("length"),
                "start_date": r.get("startDate"),
                "end_date": r.get("endDate", ""),
                "participants": flatten([p.get("username") or p.get("name", "") for p in participants]),
                "participant_types": flatten([p.get("type", "") for p in participants]),
                "time_restrictions": flatten(r.get("timeRestriction", {})),
            })

    write_csv("schedules.csv", sched_rows, fieldnames=["id", "name", "description", "timezone", "enabled", "team_id", "team_name"])
    write_csv("schedule_rotations.csv", rotation_rows, fieldnames=["schedule_id", "schedule_name", "rotation_id", "rotation_name", "type", "length", "start_date", "end_date", "participants", "participant_types", "time_restrictions"])


def export_escalation_policies() -> None:
    log.info("Fetching escalation policies …")
    policies = paginated_get("/v2/escalations", "escalations")
    policy_rows = []
    rule_rows = []

    for p in policies:
        try:
            detail = simple_get(f"/v2/escalations/{p['id']}")
        except Exception as e:
            log.warning("Could not fetch escalation detail %s: %s", p.get("name"), e)
            detail = p

        policy_rows.append({
            "id": detail.get("id"),
            "name": detail.get("name"),
            "description": detail.get("description", ""),
            "owner_team_id": detail.get("ownerTeam", {}).get("id"),
            "owner_team_name": detail.get("ownerTeam", {}).get("name"),
            "repeat_wait_mins": detail.get("repeat", {}).get("waitInterval"),
            "repeat_count": detail.get("repeat", {}).get("count"),
            "repeat_reset_recipient": detail.get("repeat", {}).get("resetRecipientStates"),
            "repeat_close_alert": detail.get("repeat", {}).get("closeAlertAfterAll"),
        })

        for idx, rule in enumerate(detail.get("rules", [])):
            recipient = rule.get("recipient", {})
            rule_rows.append({
                "policy_id": detail.get("id"),
                "policy_name": detail.get("name"),
                "rule_order": idx + 1,
                "condition": rule.get("condition"),
                "notify_type": rule.get("notifyType"),
                "delay_mins": rule.get("delay", {}).get("timeAmount"),
                "recipient_type": recipient.get("type"),
                "recipient_name": recipient.get("name") or recipient.get("username", ""),
                "recipient_id": recipient.get("id", ""),
            })

    write_csv("escalation_policies.csv", policy_rows, fieldnames=["id", "name", "description", "owner_team_id", "owner_team_name", "repeat_wait_mins", "repeat_count", "repeat_reset_recipient", "repeat_close_alert"])
    write_csv("escalation_rules.csv", rule_rows, fieldnames=["policy_id", "policy_name", "rule_order", "condition", "notify_type", "delay_mins", "recipient_type", "recipient_name", "recipient_id"])


def export_integrations() -> None:
    log.info("Fetching integrations …")
    integrations = paginated_get("/v2/integrations", "integrations")
    rows = []
    for i in integrations:
        rows.append({
            "id": i.get("id"),
            "name": i.get("name"),
            "type": i.get("type"),
            "enabled": i.get("enabled"),
            "team_id": i.get("ownerTeam", {}).get("id", ""),
            "team_name": i.get("ownerTeam", {}).get("name", ""),
            "is_global": i.get("isGlobal", ""),
        })
    write_csv("integrations.csv", rows, fieldnames=["id", "name", "type", "enabled", "team_id", "team_name", "is_global"])

# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    log.info("Starting OpsGenie export → %s", OUTPUT_DIR.resolve())
    export_teams()
    export_users()
    export_schedules()
    export_escalation_policies()
    export_integrations()
    log.info("✅ Export complete. Files written to: %s", OUTPUT_DIR.resolve())


if __name__ == "__main__":
    main()
