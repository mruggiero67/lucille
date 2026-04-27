#!/usr/bin/env python3
"""
OpsGenie → Datadog On-Call Migration Script
============================================
Reads the CSVs produced by opsgenie_export.py and creates the equivalent
resources in Datadog On-Call via the Datadog API v2.

Order of operations (dependency chain):
  1. Teams          (no dependencies)
  2. Users          (invite users to Datadog if missing)
  3. Schedules      (depend on teams + users)
  4. Escalation policies (depend on teams + schedules)

Usage:
  python opsgenie_to_datadog.py [--config PATH] [--dry-run] [--input-dir PATH] [--site SITE]
"""

import argparse
import csv
import json
import logging
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import requests
import yaml
from typing import Optional

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Rotation type mapping (OpsGenie → Datadog) ────────────────────────────────

ROTATION_SECONDS = {
    "weekly":  7 * 24 * 3600,
    "daily":   24 * 3600,
    "hourly":  3600,
    "none":    7 * 24 * 3600,
}

# ── Config ────────────────────────────────────────────────────────────────────

DEFAULT_CONFIG_PATH = Path.home() / "bin" / "datadog_config.yaml"


@dataclass
class Config:
    api_key: str
    app_key: str
    site: str
    input_dir: Path
    dry_run: bool
    request_delay: float

    @property
    def base_url(self) -> str:
        return f"https://api.{self.site}"


def load_config(config_path: Path, overrides: Optional[dict] = None) -> Config:
    overrides = overrides or {}
    with config_path.open() as f:
        raw = yaml.safe_load(f)

    dd = raw.get("datadog", {})
    mig = raw.get("migration", {})

    api_key = dd.get("api_key", "")
    app_key = dd.get("app_key", "")
    site = overrides.get("site") or dd.get("site", "datadoghq.com")
    input_dir = Path(overrides.get("input_dir") or mig.get("input_dir", "./opsgenie_export"))
    dry_run = overrides["dry_run"] if overrides.get("dry_run") is not None else mig.get("dry_run", False)
    request_delay = mig.get("request_delay", 0.25)

    missing = [k for k, v in [("datadog.api_key", api_key), ("datadog.app_key", app_key)] if not v]
    if missing:
        log.error("Missing required config values: %s", ", ".join(missing))
        sys.exit(1)

    return Config(
        api_key=api_key,
        app_key=app_key,
        site=site,
        input_dir=input_dir,
        dry_run=dry_run,
        request_delay=request_delay,
    )


# ── Argument parsing ──────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate OpsGenie on-call resources to Datadog On-Call"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to YAML config file (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=None,
        help="Preview without creating anything in Datadog",
    )
    parser.add_argument(
        "--input-dir",
        default=None,
        metavar="PATH",
        help="Directory containing CSVs from opsgenie_export.py",
    )
    parser.add_argument(
        "--site",
        default=None,
        help="Datadog site (e.g. datadoghq.com, datadoghq.eu)",
    )
    return parser.parse_args()


# ── Pure helpers ──────────────────────────────────────────────────────────────


def split_cell(value: str, sep: str = "; ") -> list[str]:
    """Reverse the flatten() from the export script."""
    if not value:
        return []
    return [v.strip() for v in value.split(sep) if v.strip()]


def build_team_handle(name: str) -> str:
    return name.lower().replace(" ", "-").replace("_", "-")


def build_team_payload(name: str, description: str) -> dict:
    return {
        "data": {
            "type": "teams",
            "attributes": {
                "name": name,
                "handle": build_team_handle(name),
                "description": description,
            },
        }
    }


def build_schedule_layer(rot: dict, user_id_map: dict[str, str]) -> Optional[dict]:
    participants = split_cell(rot.get("participants", ""))
    member_ids = []
    for p in participants:
        uid = user_id_map.get(p)
        if uid:
            member_ids.append({"type": "users", "id": uid})
        else:
            log.warning("    User '%s' not found in Datadog — skipping from rotation.", p)

    if not member_ids:
        log.warning(
            "  Rotation '%s' has no resolvable members — skipping layer.",
            rot.get("rotation_name"),
        )
        return None

    og_type = (rot.get("type") or "weekly").lower()
    seconds_per_unit = ROTATION_SECONDS.get(og_type, 7 * 24 * 3600)
    length = max(int(rot.get("length") or 0), 1)

    start = rot.get("start_date") or "2025-01-01T00:00:00Z"
    layer: dict = {
        "name": rot.get("rotation_name") or "Primary",
        "effective_date": start,
        "rotation_start": start,
        "interval": {"seconds": seconds_per_unit * length},
        "members": member_ids,
    }
    if rot.get("end_date"):
        layer["end"] = rot["end_date"]
    return layer


def build_escalation_step(
    rule: dict,
    user_id_map: dict[str, str],
    sched_id_map: dict[str, str],
) -> Optional[dict]:
    rtype = (rule.get("recipient_type") or "").lower()
    rname = rule.get("recipient_name", "")
    delay_seconds = min(max(int(rule.get("delay_mins") or 0) * 60, 60), 36000)

    targets: list[dict] = []
    if rtype == "user":
        uid = user_id_map.get(rname)
        if uid:
            targets.append({"type": "users", "id": uid})
    elif rtype == "team":
        log.warning("  Skipping team target '%s' — On-Call API does not accept general team IDs.", rname)
        return None
    elif rtype == "schedule":
        sid = sched_id_map.get(rname)
        if sid:
            targets.append({"type": "schedules", "id": sid})
    else:
        uid = user_id_map.get(rname)
        sid = sched_id_map.get(rname)
        if uid:
            targets.append({"type": "users", "id": uid})
        elif sid:
            targets.append({"type": "schedules", "id": sid})
        else:
            log.warning(
                "  Could not resolve recipient '%s' (%s) — skipping step.", rname, rtype
            )
            return None

    if not targets:
        return None

    return {"escalate_after_seconds": delay_seconds, "targets": targets}


# ── API helpers ───────────────────────────────────────────────────────────────


def _headers(cfg: Config) -> dict:
    return {
        "DD-API-KEY": cfg.api_key,
        "DD-APPLICATION-KEY": cfg.app_key,
        "Content-Type": "application/json",
    }


def dd_get(path: str, params: dict, cfg: Config) -> dict:
    resp = requests.get(
        f"{cfg.base_url}{path}", headers=_headers(cfg), params=params, timeout=30
    )
    resp.raise_for_status()
    return resp.json()


def dd_post(path: str, payload: dict, cfg: Config) -> dict:
    if cfg.dry_run:
        log.info("[DRY RUN] POST %s\n%s", path, json.dumps(payload, indent=2))
        return {"data": {"id": f"dry-run-{path.split('/')[-1]}"}}
    log.info("POST %s payload: %s", path, json.dumps(payload))
    time.sleep(cfg.request_delay)
    resp = requests.post(
        f"{cfg.base_url}{path}", headers=_headers(cfg), json=payload, timeout=30
    )
    if not resp.ok:
        log.error("POST %s failed %d: %s", path, resp.status_code, resp.text)
        resp.raise_for_status()
    return resp.json()


def read_csv(filename: str, cfg: Config) -> list[dict]:
    path = cfg.input_dir / filename
    if not path.exists():
        log.warning("CSV not found, skipping: %s", path)
        return []
    with path.open(encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ── Step 1: Teams ─────────────────────────────────────────────────────────────


def import_teams(cfg: Config) -> dict[str, str]:
    """Returns {opsgenie_team_name: datadog_team_id}"""
    log.info("── Step 1: Importing teams ──")
    rows = read_csv("teams.csv", cfg)
    team_id_map: dict[str, str] = {}

    existing: dict[str, str] = {}
    try:
        data = dd_get("/api/v2/teams", {"page[size]": 100}, cfg)
        for t in data.get("data", []):
            existing[t["attributes"]["name"]] = t["id"]
        log.info("Found %d existing Datadog teams.", len(existing))
    except Exception as e:
        log.warning("Could not fetch existing teams: %s", e)

    for row in rows:
        name = row["name"]
        if name in existing:
            log.info("  Team '%s' already exists → %s", name, existing[name])
            team_id_map[name] = existing[name]
            continue

        payload = build_team_payload(name, row.get("description", ""))
        result = dd_post("/api/v2/teams", payload, cfg)
        tid = result["data"]["id"]
        team_id_map[name] = tid
        log.info("  Created team '%s' → %s", name, tid)

    return team_id_map


# ── Step 2: Users ─────────────────────────────────────────────────────────────


def import_users(cfg: Config) -> dict[str, str]:
    """Returns {email: datadog_user_id}"""
    log.info("── Step 2: Mapping/inviting users ──")
    rows = read_csv("users.csv", cfg)
    user_id_map: dict[str, str] = {}

    existing: dict[str, str] = {}
    try:
        page = 0
        while True:
            data = dd_get("/api/v2/users", {"page[size]": 100, "page[number]": page}, cfg)
            for u in data.get("data", []):
                email = u["attributes"].get("email", "")
                if email:
                    existing[email] = u["id"]
            if not data.get("links", {}).get("next"):
                break
            page += 1
        log.info("Found %d existing Datadog users.", len(existing))
    except Exception as e:
        log.warning("Could not fetch existing users: %s", e)

    for row in rows:
        email = row.get("username", "")
        if not email:
            continue
        if email in existing:
            user_id_map[email] = existing[email]
            continue

        payload = {
            "data": {
                "type": "users",
                "attributes": {"email": email, "name": row.get("full_name", email)},
            }
        }
        try:
            result = dd_post("/api/v2/users", payload, cfg)
            uid = result["data"]["id"]
            user_id_map[email] = uid
            log.info("  Invited user '%s' → %s", email, uid)
        except Exception as e:
            log.warning("  Could not invite '%s': %s", email, e)

    return user_id_map


# ── Step 3: Schedules ─────────────────────────────────────────────────────────


def import_schedules(
    user_id_map: dict[str, str],
    cfg: Config,
) -> dict[str, str]:
    """Returns {opsgenie_schedule_name: datadog_schedule_id}"""
    log.info("── Step 3: Importing schedules ──")
    schedules = read_csv("schedules.csv", cfg)
    rotations = read_csv("schedule_rotations.csv", cfg)
    sched_id_map: dict[str, str] = {}

    existing: dict[str, str] = {}
    try:
        data = dd_get("/api/v2/on-call/schedules", {"page[size]": 100}, cfg)
        for s in data.get("data", []):
            existing[s["attributes"]["name"]] = s["id"]
        log.info("Found %d existing Datadog schedules.", len(existing))
    except Exception as e:
        log.warning("Could not fetch existing schedules: %s", e)

    rot_by_sched: dict[str, list[dict]] = {}
    for r in rotations:
        rot_by_sched.setdefault(r["schedule_name"], []).append(r)

    for sched in schedules:
        name = sched["name"]
        if name in existing:
            log.info("  Schedule '%s' already exists → %s", name, existing[name])
            sched_id_map[name] = existing[name]
            continue
        timezone = sched.get("timezone", "UTC") or "UTC"

        layers = []
        for rot in rot_by_sched.get(name, []):
            layer = build_schedule_layer(rot, user_id_map)
            if layer:
                layers.append(layer)

        if not layers:
            log.warning("  Schedule '%s' has no valid layers — creating empty schedule.", name)

        payload: dict = {
            "data": {
                "type": "schedules",
                "attributes": {"name": name, "time_zone": timezone, "layers": layers},
            }
        }
        result = dd_post("/api/v2/on-call/schedules", payload, cfg)
        sid = result["data"]["id"]
        sched_id_map[name] = sid
        log.info("  Created schedule '%s' → %s", name, sid)

    return sched_id_map


# ── Step 4: Escalation Policies ───────────────────────────────────────────────


def import_escalation_policies(
    user_id_map: dict[str, str],
    sched_id_map: dict[str, str],
    cfg: Config,
) -> dict[str, str]:
    """Returns {opsgenie_policy_name: datadog_policy_id}"""
    log.info("── Step 4: Importing escalation policies ──")
    policies = read_csv("escalation_policies.csv", cfg)
    rules = read_csv("escalation_rules.csv", cfg)
    policy_id_map: dict[str, str] = {}

    existing: dict[str, str] = {}
    try:
        data = dd_get("/api/v2/on-call/escalation-policies", {"page[size]": 100}, cfg)
        for p in data.get("data", []):
            existing[p["attributes"]["name"]] = p["id"]
        log.info("Found %d existing Datadog escalation policies.", len(existing))
    except Exception as e:
        log.warning("Could not fetch existing escalation policies: %s", e)

    rules_by_policy: dict[str, list[dict]] = {}
    for r in rules:
        rules_by_policy.setdefault(r["policy_name"], []).append(r)
    for pname in rules_by_policy:
        rules_by_policy[pname].sort(key=lambda x: int(x.get("rule_order") or 0))

    for pol in policies:
        name = pol["name"]
        if name in existing:
            log.info("  Escalation policy '%s' already exists → %s", name, existing[name])
            policy_id_map[name] = existing[name]
            continue

        steps = []
        for rule in rules_by_policy.get(name, []):
            step = build_escalation_step(rule, user_id_map, sched_id_map)
            if step:
                steps.append(step)

        if not steps:
            log.warning("  Escalation policy '%s' has no valid steps — skipping.", name)
            continue

        payload: dict = {
            "data": {
                "type": "policies",
                "attributes": {
                    "name": name,
                    "steps": steps,
                    "retries": int(pol.get("repeat_count") or 0),
                },
            }
        }
        if pol.get("repeat_close_alert", "").lower() == "true":
            payload["data"]["attributes"]["resolve_page_on_policy_end"] = True

        result = dd_post("/api/v2/on-call/escalation-policies", payload, cfg)
        pid = result["data"]["id"]
        policy_id_map[name] = pid
        log.info("  Created escalation policy '%s' → %s", name, pid)

    return policy_id_map


# ── Summary report ────────────────────────────────────────────────────────────


def write_id_map_report(
    team_id_map: dict[str, str],
    user_id_map: dict[str, str],
    sched_id_map: dict[str, str],
    policy_id_map: dict[str, str],
    cfg: Config,
) -> None:
    report = {
        "teams": team_id_map,
        "users": user_id_map,
        "schedules": sched_id_map,
        "escalation_policies": policy_id_map,
    }
    out = cfg.input_dir / "datadog_id_map.json"
    out.write_text(json.dumps(report, indent=2))
    log.info("ID mapping written to %s", out)


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    args = parse_args()
    overrides = {
        "dry_run": True if args.dry_run else None,
        "input_dir": args.input_dir,
        "site": args.site,
    }
    cfg = load_config(args.config, overrides)

    if cfg.dry_run:
        log.info("DRY RUN mode — no resources will be created in Datadog.")

    log.info("Starting OpsGenie → Datadog On-Call import (site: %s)", cfg.site)

    team_id_map = import_teams(cfg)
    user_id_map = import_users(cfg)
    sched_id_map = import_schedules(user_id_map, cfg)
    policy_id_map = import_escalation_policies(user_id_map, sched_id_map, cfg)

    write_id_map_report(team_id_map, user_id_map, sched_id_map, policy_id_map, cfg)

    log.info(
        "Import complete. Created: %d teams, %d schedules, %d escalation policies.",
        len(team_id_map),
        len(sched_id_map),
        len(policy_id_map),
    )
    log.info("Next steps:")
    log.info("  1. Review resources in Datadog On-Call UI")
    log.info("  2. Configure routing rules per team (On-Call > Teams > Routing Rules)")
    log.info("  3. Ask team members to set up their On-Call profile / notification preferences")
    log.info("  4. Run a test alert to verify escalation chains end-to-end")
    log.info("  5. Decommission OpsGenie once stable")


if __name__ == "__main__":
    main()
