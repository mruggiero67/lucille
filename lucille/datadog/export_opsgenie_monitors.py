#!/usr/bin/env python3
"""
Export Datadog monitors that notify Opsgenie to a CSV.
"""

import argparse
import csv
import logging
import re
import time
from datetime import date
from pathlib import Path

import requests
import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path.home() / "bin" / "datadog_config.yaml"
DEFAULT_OUTPUT_DIR = Path.home() / "Desktop" / "debris"
PAGE_SIZE = 100
REQUEST_DELAY = 0.25

OPSGENIE_PATTERN = re.compile(r"@opsgenie[-\w]*", re.IGNORECASE)


# ── Pure helpers ──────────────────────────────────────────────────────────────


def extract_opsgenie_handles(message: str) -> list[str]:
    return sorted(set(OPSGENIE_PATTERN.findall(message or "")))


def flatten_tags(tags: list) -> str:
    return "; ".join(tags) if tags else ""


def monitor_to_row(monitor: dict) -> dict:
    message = monitor.get("message", "")
    handles = extract_opsgenie_handles(message)
    creator = monitor.get("creator", {})
    return {
        "id": monitor.get("id"),
        "name": monitor.get("name", ""),
        "type": monitor.get("type", ""),
        "status": monitor.get("overall_state", ""),
        "opsgenie_handles": "; ".join(handles),
        "tags": flatten_tags(monitor.get("tags", [])),
        "created": monitor.get("created", ""),
        "modified": monitor.get("modified", ""),
        "creator_email": creator.get("email", "") if isinstance(creator, dict) else "",
        "message_snippet": (message[:200] + "…") if len(message) > 200 else message,
    }


def has_opsgenie_notification(monitor: dict) -> bool:
    return bool(extract_opsgenie_handles(monitor.get("message", "")))


def output_path(output_dir: Path) -> Path:
    today = date.today().strftime("%Y_%m_%d")
    return output_dir / f"{today}_datadog_opsgenie_monitors.csv"


# ── I/O helpers ───────────────────────────────────────────────────────────────


def load_config(config_path: Path) -> dict:
    with config_path.open() as f:
        return yaml.safe_load(f)


def fetch_all_monitors(api_key: str, app_key: str, site: str) -> list[dict]:
    base_url = f"https://api.{site}/api/v1/monitor"
    headers = {"DD-API-KEY": api_key, "DD-APPLICATION-KEY": app_key}
    monitors: list[dict] = []
    page = 0

    while True:
        params = {"page": page, "page_size": PAGE_SIZE}
        resp = requests.get(base_url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        monitors.extend(batch)
        log.info("Fetched page %d (%d monitors so far)", page, len(monitors))
        if len(batch) < PAGE_SIZE:
            break
        page += 1
        time.sleep(REQUEST_DELAY)

    return monitors


def write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        log.warning("No Opsgenie-notifying monitors found — no CSV written.")
        return
    fieldnames = [
        "id", "name", "type", "status", "opsgenie_handles",
        "tags", "created", "modified", "creator_email", "message_snippet",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    log.info("Wrote %d rows → %s", len(rows), path)


# ── Main ──────────────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Datadog monitors that notify Opsgenie to CSV"
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help=f"Path to YAML config file (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory to write CSV (default: {DEFAULT_OUTPUT_DIR})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    dd = config.get("datadog", {})

    api_key = dd.get("api_key", "")
    app_key = dd.get("app_key", "")
    site = dd.get("site", "datadoghq.com")

    if not api_key or not app_key:
        log.error("Missing datadog.api_key or datadog.app_key in config.")
        raise SystemExit(1)

    log.info("Fetching all monitors from %s …", site)
    monitors = fetch_all_monitors(api_key, app_key, site)
    log.info("Total monitors fetched: %d", len(monitors))

    opsgenie_monitors = [m for m in monitors if has_opsgenie_notification(m)]
    log.info("Monitors with Opsgenie notifications: %d", len(opsgenie_monitors))

    rows = [monitor_to_row(m) for m in opsgenie_monitors]
    write_csv(rows, output_path(args.output_dir))


if __name__ == "__main__":
    main()
