#!/usr/bin/env python3
"""
Jira API client for the CFR tool.

Reuses lucille.jira.utils for session management and pagination —
credentials come from ~/bin/lead_time_config.yaml via config_loader.
"""

import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Handle both direct script execution and module import
try:
    from lucille.jira.utils import create_jira_session, fetch_all_issues, make_jira_request
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
    from lucille.jira.utils import create_jira_session, fetch_all_issues, make_jira_request

logger = logging.getLogger(__name__)


@dataclass
class JiraIssue:
    key: str
    issue_type: str
    labels: List[str]
    created: datetime
    status: str
    summary: str
    project_key: str


class JiraClient:
    def __init__(self, base_url: str, username: str, api_token: str):
        self.base_url = base_url.rstrip("/")
        self.session = create_jira_session(base_url, username, api_token)

    def get_issue(self, key: str) -> Optional[JiraIssue]:
        """Fetch a single Jira issue by key. Returns None on failure."""
        try:
            data = make_jira_request(
                self.session,
                self.base_url,
                f"issue/{key}",
                params={"fields": "issuetype,labels,created,status,summary,project"},
            )
            return self._parse_issue(data)
        except Exception as e:
            logger.warning(f"Failed to fetch Jira issue {key}: {e}")
            return None

    def get_issues_created_after(
        self,
        project_keys: List[str],
        after: datetime,
        issue_types: Optional[List[str]] = None,
    ) -> List[JiraIssue]:
        """
        Fetch Jira issues in the given projects created after `after`.
        Optionally filter to specific issue types (e.g. Bug, Incident).
        """
        projects_jql = ", ".join(f'"{p}"' for p in project_keys)
        date_str = after.strftime("%Y-%m-%d")
        type_clause = ""
        if issue_types:
            types_jql = ", ".join(f'"{t}"' for t in issue_types)
            type_clause = f" AND issuetype IN ({types_jql})"
        jql = (
            f'project IN ({projects_jql}) AND created >= "{date_str}"'
            f"{type_clause} ORDER BY created ASC"
        )
        issues_raw = fetch_all_issues(
            session=self.session,
            base_url=self.base_url,
            jql=jql,
            fields=["issuetype", "labels", "created", "status", "summary", "project"],
        )
        return [self._parse_issue(i) for i in issues_raw]

    def _parse_issue(self, raw: Dict[str, Any]) -> JiraIssue:
        fields = raw.get("fields", {})
        created_str = fields.get("created", "")
        try:
            created = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            created = datetime.now()
        return JiraIssue(
            key=raw["key"],
            issue_type=fields.get("issuetype", {}).get("name", ""),
            labels=list(fields.get("labels") or []),
            created=created,
            status=fields.get("status", {}).get("name", ""),
            summary=fields.get("summary", ""),
            project_key=fields.get("project", {}).get("key", ""),
        )

    def is_failure_issue(self, issue: JiraIssue, config: Dict[str, Any]) -> bool:
        """Return True if this issue signals a deployment failure per cfr config."""
        cfr = config["cfr"]
        if issue.issue_type in cfr.get("failure_issue_types", []):
            return True
        failure_labels = set(cfr.get("failure_labels", []))
        if failure_labels & set(issue.labels):
            return True
        return False
