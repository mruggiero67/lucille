#!/usr/bin/env python3
"""
publish.py — Publish weekly engineering metrics to Confluence.

Usage:
    python publish.py --output-dir ./output

Expected directory structure in --output-dir:
    deployments/
        deployment_trends.png
        summary.txt            # "Date range: ... \n Average deployments per day: ... // Average per week: ..."
    opsgenie/
        alerts_by_team.png
        alerts_daily.png
        summary.txt            # "Date range: ... \n Average alerts per day: ... // Average alerts per week: ..."
    github_security/
        severity_chart.png
        security_alerts.csv    # repository, alert_type, link, created_at, age_days, severity
    pull_requests/
        aging_prs.csv          # repo_name, author, created_at, age_days, link, owners, pr_title

Environment variables (or .env file):
    CONFLUENCE_BASE_URL   — e.g. https://yourcompany.atlassian.net/wiki
    CONFLUENCE_USER       — your email
    CONFLUENCE_API_TOKEN  — your Atlassian API token
    CONFLUENCE_SPACE_KEY  — e.g. ENG
    CONFLUENCE_PARENT_PAGE_TITLE — default: "Weekly Metrics"
"""

import argparse
import csv
import os
import sys
import json
import requests
import yaml
from datetime import date, timedelta, datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def get_config(config_path="config.yaml"):
    """Load configuration from a YAML file.

    Expected YAML structure:
        jira:
          base_url: 'https://jarisinc.atlassian.net/'
          api_token: <token>
          username: michael@jaris.io

        confluence:
          space_key: ENG
          parent_page_title: Weekly Metrics   # optional, defaults to "Weekly Metrics"
    """
    path = Path(config_path)
    if not path.exists():
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)

    with open(path, "r") as f:
        cfg = yaml.safe_load(f)

    jira = cfg.get("jira", {})
    confluence = cfg.get("confluence", {})

    # Validate required fields
    required = {
        "jira.base_url": jira.get("base_url"),
        "jira.api_token": jira.get("api_token"),
        "jira.username": jira.get("username"),
        "confluence.space_key": confluence.get("space_key"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print(f"ERROR: Missing config fields: {', '.join(missing)}")
        sys.exit(1)

    # Confluence Cloud shares the same Atlassian domain — derive from jira base_url
    base_url = jira["base_url"].rstrip("/")
    # Ensure we have the /wiki path for Confluence API
    confluence_base = f"{base_url}/wiki" if not base_url.endswith("/wiki") else base_url

    return {
        "CONFLUENCE_BASE_URL": confluence_base,
        "CONFLUENCE_USER": jira["username"],
        "CONFLUENCE_API_TOKEN": jira["api_token"],
        "CONFLUENCE_SPACE_KEY": confluence["space_key"],
        "CONFLUENCE_PARENT_PAGE_TITLE": confluence.get(
            "parent_page_title", "Weekly Metrics"
        ),
    }


# ---------------------------------------------------------------------------
# Confluence API helpers
# ---------------------------------------------------------------------------

class ConfluenceClient:
    def __init__(self, base_url, user, token, space_key):
        self.base_url = base_url.rstrip("/")
        self.api = f"{self.base_url}/rest/api"
        self.auth = (user, token)
        self.space_key = space_key

    def _get(self, path, params=None):
        r = requests.get(f"{self.api}{path}", auth=self.auth, params=params)
        r.raise_for_status()
        return r.json()

    def _post(self, path, json_data=None, **kwargs):
        r = requests.post(f"{self.api}{path}", auth=self.auth, json=json_data, **kwargs)
        r.raise_for_status()
        return r.json()

    def _put(self, path, json_data):
        r = requests.put(f"{self.api}{path}", auth=self.auth, json=json_data)
        r.raise_for_status()
        return r.json()

    def find_page(self, title):
        """Find a page by title in the configured space. Returns page dict or None."""
        data = self._get("/content", params={
            "spaceKey": self.space_key,
            "title": title,
            "expand": "version",
        })
        results = data.get("results", [])
        return results[0] if results else None

    def create_page(self, title, body_html, parent_id=None):
        """Create a new Confluence page."""
        payload = {
            "type": "page",
            "title": title,
            "space": {"key": self.space_key},
            "body": {
                "storage": {
                    "value": body_html,
                    "representation": "storage",
                }
            },
        }
        if parent_id:
            payload["ancestors"] = [{"id": parent_id}]
        return self._post("/content", json_data=payload)

    def update_page(self, page_id, title, body_html, current_version):
        """Update an existing Confluence page."""
        payload = {
            "id": page_id,
            "type": "page",
            "title": title,
            "body": {
                "storage": {
                    "value": body_html,
                    "representation": "storage",
                }
            },
            "version": {"number": current_version + 1},
        }
        return self._put(f"/content/{page_id}", json_data=payload)

    def upload_attachment(self, page_id, filepath):
        """Upload (or update) a file attachment on a page."""
        filename = Path(filepath).name
        headers = {"X-Atlassian-Token": "nocheck"}
        with open(filepath, "rb") as f:
            r = requests.put(
                f"{self.api}/content/{page_id}/child/attachment",
                auth=self.auth,
                headers=headers,
                files={"file": (filename, f, "application/octet-stream")},
            )
        # 200 = updated existing, if 404 try POST for new attachment
        if r.status_code == 404:
            with open(filepath, "rb") as f:
                r = requests.post(
                    f"{self.api}/content/{page_id}/child/attachment",
                    auth=self.auth,
                    headers=headers,
                    files={"file": (filename, f, "application/octet-stream")},
                )
        r.raise_for_status()
        return filename


# ---------------------------------------------------------------------------
# Content builders — each section returns Confluence storage-format HTML
# ---------------------------------------------------------------------------

def read_summary(filepath):
    """Read a summary.txt file and return lines as a list."""
    if not filepath.exists():
        return []
    return [line.strip() for line in filepath.read_text().splitlines() if line.strip()]


def build_image_macro(filename):
    """Confluence storage-format macro for an attached image."""
    return (
        f'<ac:image ac:width="750">'
        f'<ri:attachment ri:filename="{filename}" />'
        f'</ac:image>'
    )


def build_summary_block(lines):
    """Render summary stat lines as simple paragraphs."""
    return "".join(f"<p>{line}</p>" for line in lines)


def format_cell(value):
    """Format a cell value for HTML output."""
    value = value.strip() if isinstance(value, str) else value
    if value.startswith(("http://", "https://")):
        label = value
        if len(value) > 57:
            label = value[:57] + "..."
        return f'<a href="{value}">{label}</a>'
    return value


def build_table_from_csv(csv_path, columns=None):
    """
    Build an HTML table from a CSV file.
    If columns is provided, only include those columns (in order).
    """
    if not csv_path.exists():
        return f"<p><em>No data: {csv_path.name} not found</em></p>"

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return "<p><em>No data</em></p>"

    headers = columns if columns else list(rows[0].keys())

    html = ['<table><thead><tr>']
    for h in headers:
        html.append(f'<th>{h}</th>')
    html.append('</tr></thead><tbody>')
    for row in rows:
        html.append('<tr>')
        for h in headers:
            html.append(f'<td>{format_cell(row.get(h, ""))}</td>')
        html.append('</tr>')
    html.append('</tbody></table>')
    return "".join(html)


def build_section(title, content):
    """Wrap a section with an h2 heading."""
    return f"<h2>{title}</h2>\n{content}\n"


def build_page_body(output_dir):
    """Assemble the full page body from all sections."""
    d = Path(output_dir)
    sections = []
    datestring = datetime.now().strftime("%Y_%m_%d")

    # --- Deployment frequency ---
    deploy_content = ""
    deploy_summary = read_summary(d / "deployments" / f"{datestring}_weekly_deployment_summary.txt")
    deploy_chart = d / "deployments" / f"{datestring}_weekly_deployment_trends.png"
    if deploy_chart.exists():
        deploy_content += build_image_macro(deploy_chart.name)
    deploy_content += build_summary_block(deploy_summary)
    sections.append(build_section("Deployment frequency", deploy_content))

    # --- OpsGenie alerts ---
    ops_content = ""
    ops_summary = read_summary(d / "opsgenie" / f"{datestring}_daily_opsgenie_alerts_analysis_summary.txt")
    ops_team_chart = d / "opsgenie" / f"{datestring}_opsgenie_alerts_last_6_weeks.png"
    ops_daily_chart = d / "opsgenie" / f"{datestring}_opsgenie_alerts_analysis.png"
    if ops_team_chart.exists():
        ops_content += build_image_macro(ops_team_chart.name)
    if ops_daily_chart.exists():
        ops_content += build_image_macro(ops_daily_chart.name)
    ops_content += build_summary_block(ops_summary)
    sections.append(build_section("Opsgenie Alerts", ops_content))

    # --- GitHub security alerts ---
    sec_content = ""
    sec_chart = d / "github" / f"{datestring}_github_security_alerts_severity.png"
    if sec_chart.exists():
        sec_content += build_image_macro(sec_chart.name)
    sec_content += build_table_from_csv(
        d / "github" / f"{datestring}_github_security_alerts_all.csv",
        columns=["repository", "alert_type", "alert_link", "created_at", "age_days", "severity"],
    )
    sections.append(build_section("Github Security Alerts", sec_content))

    # --- Pull requests 7-21 days ---
    pr_content = build_table_from_csv(
        d / "pull_requests" / f"{datestring}_aging_prs.csv",
        columns=["repo_name", "title", "pr_url", "author", "created_at", "age_days"],
    )
    sections.append(build_section("Pull requests: age between 7 and 21 days", pr_content))

    return "\n".join(sections)


# ---------------------------------------------------------------------------
# Image collection — find all PNGs to upload
# ---------------------------------------------------------------------------

def collect_images(output_dir):
    """Return list of all PNG paths to upload."""
    return sorted(Path(output_dir).rglob("*.png"))


# ---------------------------------------------------------------------------
# Date helper
# ---------------------------------------------------------------------------

def next_monday():
    """Return the next Monday (or today if it's Monday) as YYYY-MM-DD."""
    today = date.today()
    days_ahead = 0 - today.weekday()  # Monday = 0
    if days_ahead < 0:
        days_ahead += 7
    return (today + timedelta(days=days_ahead)).isoformat()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Publish weekly metrics to Confluence")
    parser.add_argument(
        "--output-dir", required=True,
        help="Directory containing subdirectories with CSVs and PNGs",
    )
    parser.add_argument(
        "--config", default="config.yaml",
        help="Path to YAML config file (default: config.yaml)",
    )
    parser.add_argument(
        "--page-title", default=None,
        help="Override page title (default: next Monday's date, e.g. 2026-02-09)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Generate HTML body and print it without publishing",
    )
    args = parser.parse_args()

    config = get_config(args.config) if not args.dry_run else {}
    title = args.page_title or next_monday()

    print(f"Building page: {title}")
    body = build_page_body(args.output_dir)

    if args.dry_run:
        print("\n--- Generated Confluence storage format ---\n")
        print(body)
        print(f"\n--- Images to upload ---")
        for img in collect_images(args.output_dir):
            print(f"  {img}")
        return

    client = ConfluenceClient(
        config["CONFLUENCE_BASE_URL"],
        config["CONFLUENCE_USER"],
        config["CONFLUENCE_API_TOKEN"],
        config["CONFLUENCE_SPACE_KEY"],
    )

    # Find parent page
    parent = client.find_page(config["CONFLUENCE_PARENT_PAGE_TITLE"])
    if not parent:
        print(f"ERROR: Parent page '{config['CONFLUENCE_PARENT_PAGE_TITLE']}' not found in space {config['CONFLUENCE_SPACE_KEY']}")
        sys.exit(1)
    parent_id = parent["id"]
    print(f"Found parent page: {config['CONFLUENCE_PARENT_PAGE_TITLE']} (id={parent_id})")

    # Create or update the page
    existing = client.find_page(title)
    if existing:
        page_id = existing["id"]
        version = existing["version"]["number"]
        print(f"Updating existing page (id={page_id}, version={version})")
        client.update_page(page_id, title, body, version)
    else:
        print(f"Creating new page under parent")
        result = client.create_page(title, body, parent_id=parent_id)
        page_id = result["id"]

    # Upload all images as attachments
    images = collect_images(args.output_dir)
    print(f"Uploading {len(images)} image(s)...")
    for img in images:
        fname = client.upload_attachment(page_id, img)
        print(f"  ✓ {fname}")

    page_url = f"{config['CONFLUENCE_BASE_URL']}/spaces/{config['CONFLUENCE_SPACE_KEY']}/pages/{page_id}"
    print(f"\nDone! Page: {page_url}")


if __name__ == "__main__":
    main()
