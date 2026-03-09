"""
Tests for InterventionDetector.

Uses mocked GitHub/Jira clients — no live API calls.
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from lucille.cfr.sources.github_client import GitHubPR, GitHubRelease
from lucille.cfr.sources.jira_client import JiraIssue
from lucille.cfr.logic.deployment_detector import DeploymentEvent
from lucille.cfr.logic.intervention_detector import InterventionDetector

DEPLOY_TS = datetime(2026, 3, 1, 9, 0, 0, tzinfo=timezone.utc)

SAMPLE_CONFIG = {
    "cfr": {
        "intervention_window_hours": 72,
        "failure_title_patterns": ["hotfix", "rollback", "revert"],
        "failure_labels": ["hotfix", "rollback"],
        "failure_issue_types": ["Bug", "Incident"],
        "failure_labels": ["hotfix", "rollback"],
        "agent_jira_label": "agent-initiated",
    }
}


def _make_event(repo="analytics", tag="v1.4.2", prs=None) -> DeploymentEvent:
    return DeploymentEvent(
        deployment_id=tag,
        timestamp=DEPLOY_TS,
        repo=repo,
        prs=prs or [],
    )


def _make_pr(
    number=812,
    title="fix: normal",
    labels=None,
    merged_hours_after=24,
    body="",
) -> GitHubPR:
    return GitHubPR(
        number=number,
        title=title,
        body=body,
        author="alice",
        labels=labels or [],
        merged_at=DEPLOY_TS + timedelta(hours=merged_hours_after),
        base_branch="main",
        html_url=f"https://github.com/jarisdev/analytics/pull/{number}",
        repo="analytics",
    )


def _make_jira_issue(
    key="FED-999",
    issue_type="Bug",
    labels=None,
    created_hours_after=10,
) -> JiraIssue:
    return JiraIssue(
        key=key,
        issue_type=issue_type,
        labels=labels or [],
        created=DEPLOY_TS + timedelta(hours=created_hours_after),
        status="In Progress",
        summary="Something broke",
        project_key="FED",
    )


class TestInterventionDetector:
    def setup_method(self):
        self.gh = MagicMock()
        self.jira = MagicMock()
        self.detector = InterventionDetector(self.gh, self.jira, SAMPLE_CONFIG)

    def test_no_signals_returns_not_detected(self):
        self.gh.get_prs_merged_after.return_value = []
        self.gh.extract_jira_keys.return_value = []
        self.gh.get_releases.return_value = []

        result = self.detector.check(_make_event())
        assert result.detected is False

    def test_hotfix_pr_title_triggers_intervention(self):
        hotfix_pr = _make_pr(title="hotfix: fix null pointer", merged_hours_after=24)
        self.gh.get_prs_merged_after.return_value = [hotfix_pr]

        result = self.detector.check(_make_event())

        assert result.detected is True
        assert result.confidence == "high"
        assert "hotfix" in result.reason.lower()

    def test_rollback_label_triggers_intervention(self):
        rollback_pr = _make_pr(
            title="Revert feature X", labels=["rollback"], merged_hours_after=48
        )
        self.gh.get_prs_merged_after.return_value = [rollback_pr]

        result = self.detector.check(_make_event())

        assert result.detected is True
        assert result.confidence == "high"

    def test_hotfix_pr_outside_window_not_flagged(self):
        late_pr = _make_pr(title="hotfix: slow fix", merged_hours_after=100)
        self.gh.get_prs_merged_after.return_value = [late_pr]

        result = self.detector.check(_make_event())
        assert result.detected is False

    def test_jira_bug_created_post_deploy_triggers_intervention(self):
        pr_with_key = _make_pr(body="Fixes FED-999", title="FED-999 add feature")
        event = _make_event(prs=[pr_with_key])

        self.gh.get_prs_merged_after.return_value = []
        self.gh.extract_jira_keys.return_value = ["FED-999"]

        bug = _make_jira_issue(key="FED-999", issue_type="Bug", created_hours_after=10)
        self.jira.get_issue.return_value = bug
        self.jira.is_failure_issue.return_value = True

        result = self.detector.check(event)

        assert result.detected is True
        assert "FED-999" in result.evidence_jira
        assert result.confidence == "high"

    def test_patch_bump_low_confidence_detection(self):
        self.gh.get_prs_merged_after.return_value = []
        self.gh.extract_jira_keys.return_value = []

        hotfix_release = GitHubRelease(
            tag_name="v1.4.3",
            published_at=DEPLOY_TS + timedelta(hours=12),
            repo="analytics",
            html_url="https://github.com/jarisdev/analytics/releases/tag/v1.4.3",
        )
        self.gh.get_releases.return_value = [hotfix_release]

        event = _make_event(tag="v1.4.2")
        result = self.detector.check(event)

        assert result.detected is True
        assert result.confidence == "low"
        assert "v1.4.3" in result.reason
