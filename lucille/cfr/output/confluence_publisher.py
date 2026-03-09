#!/usr/bin/env python3
"""
Publishes CFR summary to Confluence.

Reuses confluence.space_key and confluence.parent_page_title from
~/bin/jira_epic_config.yaml (space: SD, parent: "Weekly Metrics").

Disabled by default — flip cfr.publish_to_confluence: true in jira_epic_config.yaml
when signal quality has been validated.
"""

import logging
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from requests.auth import HTTPBasicAuth

try:
    from ..logic.cfr_rollup import CFRResult
    from ..output.summary_reporter import format_summary
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from lucille.cfr.logic.cfr_rollup import CFRResult
    from lucille.cfr.output.summary_reporter import format_summary

logger = logging.getLogger(__name__)


class ConfluencePublisher:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.confluence_cfg = config.get("confluence", {})
        # Jira API token doubles as Confluence token for Atlassian Cloud
        jira = config["jira"]
        self.auth = HTTPBasicAuth(jira["username"], jira["api_token"])
        # Derive Confluence base URL from Jira base URL
        jira_base = jira["base_url"].rstrip("/")
        self.base_url = jira_base  # same domain for Atlassian Cloud

    def publish(self, result: CFRResult, title: Optional[str] = None) -> Optional[str]:
        """
        Create or update a Confluence page with the CFR summary.
        Returns the page URL on success, None on failure.
        """
        space_key = self.confluence_cfg.get("space_key", "SD")
        parent_title = self.confluence_cfg.get("parent_page_title", "Weekly Metrics")
        period = result.period_start.strftime("%B %Y")
        page_title = title or f"CFR Report — {period}"
        body_text = format_summary(result, page_title)

        # Wrap in Confluence storage format
        storage_body = f"<pre>{body_text}</pre>"

        parent_id = self._get_page_id(space_key, parent_title)
        if parent_id is None:
            logger.error(f"Could not find parent page '{parent_title}' in space '{space_key}'")
            return None

        existing_id = self._get_page_id(space_key, page_title)
        if existing_id:
            return self._update_page(existing_id, page_title, storage_body, space_key)
        else:
            return self._create_page(space_key, parent_id, page_title, storage_body)

    def _get_page_id(self, space_key: str, title: str) -> Optional[str]:
        url = f"{self.base_url}/wiki/rest/api/content"
        params = {"spaceKey": space_key, "title": title, "expand": "version"}
        try:
            resp = requests.get(url, auth=self.auth, params=params, timeout=30)
            resp.raise_for_status()
            results = resp.json().get("results", [])
            return results[0]["id"] if results else None
        except Exception as e:
            logger.warning(f"Failed to look up page '{title}': {e}")
            return None

    def _create_page(
        self, space_key: str, parent_id: str, title: str, body: str
    ) -> Optional[str]:
        url = f"{self.base_url}/wiki/rest/api/content"
        payload = {
            "type": "page",
            "title": title,
            "space": {"key": space_key},
            "ancestors": [{"id": parent_id}],
            "body": {"storage": {"value": body, "representation": "storage"}},
        }
        try:
            resp = requests.post(url, auth=self.auth, json=payload, timeout=30)
            resp.raise_for_status()
            page_url = resp.json()["_links"]["webui"]
            logger.info(f"Confluence page created: {page_url}")
            return page_url
        except Exception as e:
            logger.error(f"Failed to create Confluence page '{title}': {e}")
            return None

    def _update_page(
        self, page_id: str, title: str, body: str, space_key: str
    ) -> Optional[str]:
        # Get current version
        url = f"{self.base_url}/wiki/rest/api/content/{page_id}"
        try:
            resp = requests.get(url, auth=self.auth, params={"expand": "version"}, timeout=30)
            resp.raise_for_status()
            current_version = resp.json()["version"]["number"]
        except Exception as e:
            logger.error(f"Failed to get page version for '{title}': {e}")
            return None

        payload = {
            "type": "page",
            "title": title,
            "version": {"number": current_version + 1},
            "body": {"storage": {"value": body, "representation": "storage"}},
        }
        try:
            resp = requests.put(url, auth=self.auth, json=payload, timeout=30)
            resp.raise_for_status()
            page_url = resp.json()["_links"]["webui"]
            logger.info(f"Confluence page updated: {page_url}")
            return page_url
        except Exception as e:
            logger.error(f"Failed to update Confluence page '{title}': {e}")
            return None


def publish_to_confluence(result: CFRResult, config: Dict[str, Any]) -> None:
    """Top-level helper called from cfr.py when publish_to_confluence is enabled."""
    if not config["cfr"].get("publish_to_confluence", False):
        logger.info("Confluence publishing disabled (publish_to_confluence: false)")
        return
    publisher = ConfluencePublisher(config)
    url = publisher.publish(result)
    if url:
        print(f"Published to Confluence: {url}")
