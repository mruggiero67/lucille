#!/usr/bin/env python3
"""
Detects post-deployment interventions within the configured window.

Failure signals checked in order:
  1. Hotfix / rollback PR merged in the window (high confidence)
  2. Jira Bug / Incident linked to deployment created post-deploy (high confidence)
  3. Patch version bump release published in the window (low confidence)
"""

import logging
import re
import sys
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from ..sources.github_client import GitHubClient, GitHubPR
    from ..sources.jira_client import JiraClient, JiraIssue
    from .deployment_detector import DeploymentEvent
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from lucille.cfr.sources.github_client import GitHubClient, GitHubPR
    from lucille.cfr.sources.jira_client import JiraClient, JiraIssue
    from lucille.cfr.logic.deployment_detector import DeploymentEvent

logger = logging.getLogger(__name__)


@dataclass
class InterventionResult:
    detected: bool
    reason: Optional[str] = None
    evidence_prs: List[str] = field(default_factory=list)
    evidence_jira: List[str] = field(default_factory=list)
    confidence: str = "high"  # "high" | "medium" | "low"


class InterventionDetector:
    def __init__(
        self,
        github_client: GitHubClient,
        jira_client: JiraClient,
        config: Dict[str, Any],
    ):
        self.gh = github_client
        self.jira = jira_client
        self.config = config
        self.cfr = config["cfr"]

    def check(self, event: DeploymentEvent) -> InterventionResult:
        window_hours = self.cfr.get("intervention_window_hours", 72)
        window_end = event.timestamp + timedelta(hours=window_hours)

        result = self._check_hotfix_prs(event, window_end)
        if result.detected:
            return result

        result = self._check_jira_signals(event, window_end)
        if result.detected:
            return result

        result = self._check_patch_bump(event, window_end)
        if result.detected:
            return result

        return InterventionResult(detected=False)

    def _check_hotfix_prs(
        self, event: DeploymentEvent, window_end
    ) -> InterventionResult:
        failure_title_patterns = [
            re.compile(p, re.IGNORECASE)
            for p in self.cfr.get("failure_title_patterns", ["hotfix", "rollback", "revert"])
        ]
        failure_labels = set(self.cfr.get("failure_labels", ["hotfix", "rollback"]))

        try:
            post_prs = self.gh.get_prs_merged_after(event.repo, event.timestamp)
        except Exception as e:
            logger.warning(f"Could not fetch post-deploy PRs for {event.repo}: {e}")
            return InterventionResult(detected=False)

        flagged = []
        for pr in post_prs:
            if pr.merged_at and pr.merged_at > window_end:
                continue
            title_hit = any(p.search(pr.title) for p in failure_title_patterns)
            label_hit = bool(failure_labels & set(pr.labels))
            if title_hit or label_hit:
                reason = "hotfix PR title" if title_hit else "hotfix/rollback label"
                flagged.append((pr, reason))

        if flagged:
            first_pr, reason = flagged[0]
            return InterventionResult(
                detected=True,
                reason=f"{reason}: {first_pr.title} (PR #{first_pr.number})",
                evidence_prs=[pr.html_url for pr, _ in flagged],
                confidence="high",
            )
        return InterventionResult(detected=False)

    def _check_jira_signals(
        self, event: DeploymentEvent, window_end
    ) -> InterventionResult:
        # Collect Jira keys referenced in the deployment's PRs
        all_keys: List[str] = []
        for pr in event.prs:
            all_keys.extend(self.gh.extract_jira_keys(pr))

        if not all_keys:
            return InterventionResult(detected=False)

        failure_issues = []
        for key in dict.fromkeys(all_keys):  # deduplicate, preserve order
            issue = self.jira.get_issue(key)
            if issue is None:
                continue
            if issue.created > event.timestamp and self.jira.is_failure_issue(
                issue, self.config
            ):
                failure_issues.append(issue)

        if failure_issues:
            first = failure_issues[0]
            return InterventionResult(
                detected=True,
                reason=(
                    f"{first.issue_type} {first.key} created "
                    f"{first.created.date()} post-deploy"
                ),
                evidence_jira=[i.key for i in failure_issues],
                confidence="high",
            )
        return InterventionResult(detected=False)

    def _check_patch_bump(
        self, event: DeploymentEvent, window_end
    ) -> InterventionResult:
        """Low-confidence: detects a patch version bump release within the window."""
        semver_re = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)$")
        m = semver_re.match(event.deployment_id)
        if not m:
            return InterventionResult(detected=False)

        major, minor, patch = int(m.group(1)), int(m.group(2)), int(m.group(3))
        expected_hotfix_tag = f"v{major}.{minor}.{patch + 1}"

        try:
            releases = self.gh.get_releases(event.repo, since=event.timestamp)
        except Exception as e:
            logger.warning(f"Could not fetch releases for patch bump check: {e}")
            return InterventionResult(detected=False)

        for r in releases:
            if r.published_at > window_end:
                break
            if r.tag_name in (expected_hotfix_tag, expected_hotfix_tag.lstrip("v")):
                return InterventionResult(
                    detected=True,
                    reason=f"Patch bump {event.deployment_id} → {r.tag_name} within window",
                    evidence_prs=[r.html_url],
                    confidence="low",
                )
        return InterventionResult(detected=False)
